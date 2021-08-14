// Copyright 2021 TiKV Authors
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

package txnkv

import (
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
)

// Lock represents a lock from tikv server.
type Lock = txnlock.Lock

// LockResolver resolves locks and also caches resolved txn status.
type LockResolver = txnlock.LockResolver

// TxnStatus represents a txn's final status. It should be Lock or Commit or Rollback.
type TxnStatus = txnlock.TxnStatus

// NewLock creates a new *Lock.
func NewLock(l *kvrpcpb.LockInfo) *Lock {
	return txnlock.NewLock(l)
}
