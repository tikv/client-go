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

package tikv

import (
	"bytes"
	"context"
	"math"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/internal/mockstore/mocktikv"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikvrpc"
)

// scanLocksProbeClient models ScanLock's holder-based limit, which can truncate
// a shared-lock wrapper. The ordinary mock store does not support shared locks.
type scanLocksProbeClient struct {
	Client
	holders int
}

func (c *scanLocksProbeClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	if req.Type != tikvrpc.CmdScanLock {
		return c.Client.SendRequest(ctx, addr, req, timeout)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r := req.ScanLock()
	var result []*kvrpcpb.LockInfo
	count := uint32(0)
	matches := func(key string, ts uint64) bool {
		return bytes.Compare([]byte(key), r.StartKey) >= 0 &&
			(len(r.EndKey) == 0 || bytes.Compare([]byte(key), r.EndKey) < 0) &&
			ts <= r.MaxVersion && (r.Limit == 0 || count < r.Limit)
	}
	var shared []*kvrpcpb.LockInfo
	for ts := uint64(1); ts <= uint64(c.holders); ts++ {
		if matches("hot", ts) {
			shared = append(shared, &kvrpcpb.LockInfo{
				Key: []byte("hot"), PrimaryLock: []byte("primary"),
				LockVersion: ts, LockTtl: 60000, LockType: kvrpcpb.Op_PessimisticLock,
			})
			count++
		}
	}
	if len(shared) > 0 {
		result = append(result, &kvrpcpb.LockInfo{
			Key: []byte("hot"), LockType: kvrpcpb.Op_SharedLock, SharedLockInfos: shared,
		})
	}
	if matches("tail", 1000000) {
		result = append(result, &kvrpcpb.LockInfo{
			Key: []byte("tail"), PrimaryLock: []byte("tail-primary"),
			LockVersion: 1000000, LockTtl: 60000, LockType: kvrpcpb.Op_PessimisticLock,
		})
	}
	return &tikvrpc.Response{Resp: &kvrpcpb.ScanLockResponse{Locks: result}}, nil
}

func TestStoreProbeScanLocksReturnsAllSharedHolders(t *testing.T) {
	tests := []struct {
		name       string
		holders    int
		split      bool
		start, end string
		maxVersion uint64
		wantShared int
		wantTail   bool
	}{
		{"below-limit", 1023, false, "hot", "z", math.MaxUint64, 1023, true},
		{"at-limit", 1024, false, "hot", "z", math.MaxUint64, 1024, true},
		{"over-limit", 1025, false, "hot", "z", math.MaxUint64, 1025, true},
		{"multiple-pages", 2049, false, "hot", "z", math.MaxUint64, 2049, true},
		{"across-regions", 1025, true, "hot", "z", math.MaxUint64, 1025, true},
		{"end-within-region", 1025, false, "hot", "tail", math.MaxUint64, 1025, false},
		{"end-at-region-boundary", 1025, true, "hot", "m", math.MaxUint64, 1025, false},
		{"start-after-shared-key", 1025, true, "m", "z", math.MaxUint64, 0, true},
		{"max-version", 1025, false, "hot", "z", 1024, 1024, false},
		{"empty-range", 1025, false, "hot", "hot", math.MaxUint64, 0, false},
		{"unbounded-end", 1025, true, "hot", "", math.MaxUint64, 1025, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rpc, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
			require.NoError(t, err)
			_, _, regionID := mocktikv.BootstrapWithSingleStore(cluster)
			if tt.split {
				region, peer := cluster.AllocID(), cluster.AllocID()
				cluster.Split(regionID, region, []byte("m"), []uint64{peer}, peer)
			}
			store, err := NewTestTiKVStore(&scanLocksProbeClient{Client: rpc, holders: tt.holders}, pdClient, nil, nil, 0)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			locks, err := (StoreProbe{KVStore: store}).ScanLocks(ctx, []byte(tt.start), []byte(tt.end), tt.maxVersion)
			require.NoError(t, err)
			wantCount := tt.wantShared
			if tt.wantTail {
				wantCount++
			}
			require.Equal(t, wantCount, len(locks), "number of returned holders and ordinary locks")
			seen := make(map[uint64]bool)
			for _, lock := range locks {
				require.False(t, seen[lock.TxnID], "duplicate holder %d", lock.TxnID)
				seen[lock.TxnID] = true
			}
			for ts := uint64(1); ts <= uint64(tt.wantShared); ts++ {
				require.True(t, seen[ts], "missing shared holder %d", ts)
			}
			require.Equal(t, tt.wantTail, seen[1000000])
		})
	}
}

func TestStoreProbeScanLocksHonorsEndKeyWithMockTiKV(t *testing.T) {
	rpc, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	mocktikv.BootstrapWithSingleStore(cluster)
	store, err := NewTestTiKVStore(rpc, pdClient, nil, nil, 0)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	for _, err := range rpc.MvccStore.Prewrite(&kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Op_Put, Key: []byte("a"), Value: []byte("value")},
			{Op: kvrpcpb.Op_Put, Key: []byte("z"), Value: []byte("value")},
		},
		PrimaryLock: []byte("a"), StartVersion: 1, LockTtl: 60000,
	}) {
		require.NoError(t, err)
	}
	// MockTiKV scans the entire Region, ignoring the requested end key.
	locks, err := (StoreProbe{KVStore: store}).ScanLocks(context.Background(), []byte("a"), []byte("m"), 2)
	require.NoError(t, err)
	require.Len(t, locks, 1)
	require.Equal(t, []byte("a"), locks[0].Key)
}
