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

package txnlock

import (
	"testing"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/require"
)

func TestExtractLocksFromKeyErrExpandsSharedLockHolders(t *testing.T) {
	keyErr := &kvrpcpb.KeyError{
		Locked: &kvrpcpb.LockInfo{
			Key:         []byte("shared-key"),
			LockType:    kvrpcpb.Op_SharedLock,
			LockVersion: 100,
			SharedLockInfos: []*kvrpcpb.LockInfo{
				{Key: []byte("shared-key"), LockVersion: 101, LockType: kvrpcpb.Op_PessimisticLock},
				{Key: []byte("shared-key"), LockVersion: 102, LockType: kvrpcpb.Op_Lock},
			},
		},
	}

	locks, err := ExtractLocksFromKeyErr(keyErr)

	require.NoError(t, err)
	require.Len(t, locks, 2)
	require.Equal(t, uint64(101), locks[0].TxnID)
	require.Equal(t, kvrpcpb.Op_PessimisticLock, locks[0].LockType)
	require.Equal(t, uint64(102), locks[1].TxnID)
	require.Equal(t, kvrpcpb.Op_Lock, locks[1].LockType)
}

func TestExtractLocksFromKeyErrPreservesExclusiveLock(t *testing.T) {
	locks, err := ExtractLocksFromKeyErr(&kvrpcpb.KeyError{
		Locked: &kvrpcpb.LockInfo{Key: []byte("key"), LockVersion: 7, LockType: kvrpcpb.Op_Lock},
	})

	require.NoError(t, err)
	require.Len(t, locks, 1)
	require.Equal(t, uint64(7), locks[0].TxnID)
}

func TestExtractLocksFromKeyErrReturnsKeyError(t *testing.T) {
	_, err := ExtractLocksFromKeyErr(&kvrpcpb.KeyError{
		AlreadyExist: &kvrpcpb.AlreadyExist{Key: []byte("key")},
	})

	require.Error(t, err)
}
