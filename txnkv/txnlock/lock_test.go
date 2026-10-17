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
	// The wrapper itself and its placeholder transaction id must never reach the
	// resolve path: ResolveLocks and BatchResolveLocks reject a shared wrapper, and
	// a zero transaction id would turn into an illegal transaction request.
	for _, l := range locks {
		require.False(t, l.IsShared())
		require.NotZero(t, l.TxnID)
		require.Equal(t, []byte("shared-key"), l.Key)
	}
}

func TestResolveLocksRejectsSharedLockWrapper(t *testing.T) {
	f := newCheckSecondariesFixture(t)
	wrapper := &Lock{
		Key:      []byte("a"),
		TxnID:    0,
		LockType: kvrpcpb.Op_SharedLock,
	}

	_, err := f.resolver.ResolveLocks(checkSecondariesBo(), 1, []*Lock{wrapper})
	require.Error(t, err)
	require.Contains(t, err.Error(), "misuse of resolveLocks")

	_, err = f.resolver.BatchResolveLocks(checkSecondariesBo(), []*Lock{wrapper}, f.regions[0])
	require.Error(t, err)
	require.Contains(t, err.Error(), "misuse of BatchResolveLocks")

	// Neither attempt may reach TiKV.
	require.Empty(t, f.store.sent)
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
