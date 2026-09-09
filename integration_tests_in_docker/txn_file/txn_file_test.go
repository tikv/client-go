// Copyright 2026 TiKV Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package txn_file_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/oracle"
)

const (
	txnChunkMaxSize = 256
	txnChunkCRCSize = 4
)

func TestTxnFileCommitAcrossChunksAndRegions(t *testing.T) {
	client, _ := newTestClient(t)
	prefix := testPrefix(t)
	values := orderedKeyValues{
		{key: encodeKey(prefix, "00000010"), value: bytes.Repeat([]byte("a"), 128)},
		{key: encodeKey(prefix, "00000030"), value: bytes.Repeat([]byte("b"), 128)},
		{key: encodeKey(prefix, "00000050"), value: bytes.Repeat([]byte("c"), 128)},
	}
	splitAndVerifyRegionGroups(t, client.KVStore,
		[][]byte{
			encodeKey(prefix, "00000020"),
			encodeKey(prefix, "00000040"),
		},
		[][][]byte{
			{values[0].key},
			{values[1].key},
			{values[2].key},
		},
	)

	for _, pair := range values {
		require.LessOrEqual(t, txnChunkEntrySize(pair)+txnChunkCRCSize, txnChunkMaxSize)
	}
	require.Greater(t, txnFileChunkCount(values, txnChunkMaxSize), 1)

	txn, err := client.Begin()
	require.NoError(t, err)
	for _, pair := range values {
		require.NoError(t, txn.Set(pair.key, pair.value))
	}
	okBefore := txnFileRequestsOK()
	require.NoError(t, txn.Commit(context.Background()))
	require.Equal(t, okBefore+1, txnFileRequestsOK())

	readTxn, err := client.Begin()
	require.NoError(t, err)
	for _, pair := range values {
		entry, err := readTxn.Get(context.Background(), pair.key)
		require.NoError(t, err)
		require.Equal(t, pair.value, entry.Value)
	}
	require.NoError(t, readTxn.Rollback())

	deleteKeys(t, client, values)
}

func TestTxnFileWriteConflictRollsBack(t *testing.T) {
	client, _ := newTestClient(t)
	prefix := testPrefix(t)
	baseline := orderedKeyValues{
		{key: encodeKey(prefix, "00000010_a"), value: []byte("baseline-a")},
		{key: encodeKey(prefix, "00000020_b"), value: []byte("baseline-b")},
		{key: encodeKey(prefix, "00000030_c"), value: []byte("baseline-c")},
	}
	seed(t, client, baseline)
	splitAndVerifyRegionGroups(t, client.KVStore,
		[][]byte{baseline[2].key},
		[][][]byte{
			{baseline[0].key, baseline[1].key},
			{baseline[2].key},
		},
	)

	t1, err := client.Begin()
	require.NoError(t, err)
	for _, pair := range baseline {
		require.NoError(t, t1.Set(pair.key, []byte("t1-"+string(pair.value))))
	}

	t2, err := client.Begin()
	require.NoError(t, err)
	t2.DisableTxnFile()
	t2Value := []byte("t2-c")
	require.NoError(t, t2.Set(baseline[2].key, t2Value))
	require.NoError(t, t2.Commit(context.Background()))

	errorBefore := txnFileRequestsError()
	err = t1.Commit(context.Background())
	require.True(t, tikverr.IsErrWriteConflict(err), "expected write conflict, got %v", err)
	require.Equal(t, errorBefore+1, txnFileRequestsError())

	readTS, err := client.CurrentTimestamp(oracle.GlobalTxnScope)
	require.NoError(t, err)
	expected := orderedKeyValues{
		baseline[0],
		baseline[1],
		{key: baseline[2].key, value: t2Value},
	}
	var lastProbe strings.Builder
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		lastProbe.Reset()
		for _, pair := range expected {
			result, err := directGet(ctx, client.KVStore, pair.key, readTS)
			if err != nil {
				fmt.Fprintf(&lastProbe, "key %q: %v", pair.key, err)
				return false
			}
			if result.locked != nil || result.notFound || !bytes.Equal(result.value, pair.value) {
				fmt.Fprintf(&lastProbe, "key %q: value=%q locked=%v notFound=%t", pair.key, result.value, result.locked, result.notFound)
				return false
			}
		}
		return true
	}, regionRetryTimeout, regionRetryInterval, "last direct get probe: %s", &lastProbe)

	deleteKeys(t, client, baseline)
}

func txnChunkEntrySize(pair keyValue) int {
	return 2 + len(pair.key) + 1 + 4 + len(pair.value)
}

func txnFileChunkCount(values orderedKeyValues, maxSize int) int {
	payloadSize := 0
	chunkCount := 0
	for _, pair := range values {
		entrySize := txnChunkEntrySize(pair)
		if payloadSize != 0 && payloadSize+entrySize+txnChunkCRCSize > maxSize {
			chunkCount++
			payloadSize = 0
		}
		payloadSize += entrySize
	}
	if payloadSize != 0 {
		chunkCount++
	}
	return chunkCount
}
