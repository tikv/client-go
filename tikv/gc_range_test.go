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
	"context"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
	"testing"
)

type pagedLockResolver struct {
	RegionLockResolver
	pages    [][]*txnlock.Lock
	scanned  [][]byte
	resolved []*txnlock.Lock
}

func (*pagedLockResolver) Identifier() string { return "paged-test" }

func (r *pagedLockResolver) ScanLocksInOneRegion(_ *Backoffer, start, _ []byte, _ uint64, _ uint32) ([]*txnlock.Lock, *KeyLocation, error) {
	r.scanned = append(r.scanned, start)
	page := r.pages[0]
	r.pages = r.pages[1:]
	return page, &KeyLocation{}, nil
}

func (r *pagedLockResolver) ResolveLocksInOneRegion(_ *Backoffer, locks []*txnlock.Lock, loc *KeyLocation) (*KeyLocation, error) {
	r.resolved = append(r.resolved, locks...)
	return loc, nil
}

func TestResolveDataLocksForRangePassesFullMetadataPage(t *testing.T) {
	holder := &txnlock.Lock{Key: []byte("a"), LockType: kvrpcpb.Op_SharedPessimisticLock}
	prewritten := &txnlock.Lock{Key: []byte("a"), LockType: kvrpcpb.Op_Lock}
	// The next legal physical key shares the scanned key's entire prefix.
	data := &txnlock.Lock{Key: []byte{'a', 0}, LockType: kvrpcpb.Op_Put}
	r := &pagedLockResolver{pages: [][]*txnlock.Lock{{holder, prewritten}, {data}}}
	_, err := ResolveDataLocksForRange(context.Background(), r, 10, []byte("a"), nil, NewGcResolveLockMaxBackoffer, 2)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("a"), {'a', 0}}, r.scanned)
	require.Equal(t, []*txnlock.Lock{data}, r.resolved)
}

func TestGCRescansLastSharedKey(t *testing.T) {
	first := &txnlock.Lock{Key: []byte("a"), TxnID: 1, LockType: kvrpcpb.Op_SharedPessimisticLock}
	second := &txnlock.Lock{Key: []byte("a"), TxnID: 2, LockType: kvrpcpb.Op_Lock}
	third := &txnlock.Lock{Key: []byte("a"), TxnID: 3, LockType: kvrpcpb.Op_Lock}
	r := &pagedLockResolver{pages: [][]*txnlock.Lock{{first, second}, {third}}}
	_, err := ResolveLocksForRange(context.Background(), r, 10, []byte("a"), nil, NewGcResolveLockMaxBackoffer, 2)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("a"), []byte("a")}, r.scanned)
	require.Equal(t, []*txnlock.Lock{first, second, third}, r.resolved)
}
