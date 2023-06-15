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

// NOTE: The code in this file is based on code from the
// TiDB project, licensed under the Apache License v 2.0
//
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/client_helper.go
//

// Copyright 2021 PingCAP, Inc.
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

package txnsnapshot

import (
	"time"

	"github.com/tikv/client-go/v2/internal/client"
	"github.com/tikv/client-go/v2/internal/locate"
	"github.com/tikv/client-go/v2/internal/retry"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
	"github.com/tikv/client-go/v2/util"
)

// ClientHelper wraps LockResolver and RegionRequestSender.
// It's introduced to support the new lock resolving pattern in the large transaction.
// In the large transaction protocol, sending requests and resolving locks are
// context-dependent. For example, when a send request meets a secondary lock, we'll
// call ResolveLock, and if the lock belongs to a large transaction, we may retry
// the request. If there is no context information about the resolved locks, we'll
// meet the secondary lock again and run into a deadloop.
//
// ClientHelper is only used for read operations because these optimizations don't apply
// to write operations.
type ClientHelper struct {
	lockResolver   *txnlock.LockResolver
	regionCache    *locate.RegionCache
	resolvedLocks  *util.TSSet
	committedLocks *util.TSSet
	client         client.Client
	resolveLite    bool
	locate.RegionRequestRuntimeStats
}

// NewClientHelper creates a helper instance.
func NewClientHelper(store kvstore, resolvedLocks *util.TSSet, committedLocks *util.TSSet, resolveLite bool) *ClientHelper {
	return &ClientHelper{
		lockResolver:   store.GetLockResolver(),
		regionCache:    store.GetRegionCache(),
		resolvedLocks:  resolvedLocks,
		committedLocks: committedLocks,
		client:         store.GetTiKVClient(),
		resolveLite:    resolveLite,
	}
}

// ResolveLocksWithOpts wraps the ResolveLocksWithOpts function and store the resolved result.
func (ch *ClientHelper) ResolveLocksWithOpts(bo *retry.Backoffer, opts txnlock.ResolveLocksOptions) (txnlock.ResolveLockResult, error) {
	if ch.Stats != nil {
		defer func(start time.Time) {
			locate.RecordRegionRequestRuntimeStats(ch.Stats, tikvrpc.CmdResolveLock, time.Since(start))
		}(time.Now())
	}
	opts.ForRead = true
	opts.Lite = ch.resolveLite
	res, err := ch.lockResolver.ResolveLocksWithOpts(bo, opts)
	if err != nil {
		return res, err
	}
	if len(res.IgnoreLocks) > 0 {
		ch.resolvedLocks.Put(res.IgnoreLocks...)
	}
	if len(res.AccessLocks) > 0 {
		ch.committedLocks.Put(res.AccessLocks...)
	}
	return res, nil
}

// ResolveLocks wraps the ResolveLocks function and store the resolved result.
func (ch *ClientHelper) ResolveLocks(bo *retry.Backoffer, callerStartTS uint64, locks []*txnlock.Lock) (int64, error) {
	if ch.Stats != nil {
		defer func(start time.Time) {
			locate.RecordRegionRequestRuntimeStats(ch.Stats, tikvrpc.CmdResolveLock, time.Since(start))
		}(time.Now())
	}
	msBeforeTxnExpired, resolvedLocks, committedLocks, err := ch.lockResolver.ResolveLocksForRead(bo, callerStartTS, locks, ch.resolveLite)
	if err != nil {
		return msBeforeTxnExpired, err
	}
	if len(resolvedLocks) > 0 {
		ch.resolvedLocks.Put(resolvedLocks...)
	}
	if len(committedLocks) > 0 {
		ch.committedLocks.Put(committedLocks...)
	}
	return msBeforeTxnExpired, nil
}

// UpdateResolvingLocks wraps the UpdateResolvingLocks function
func (ch *ClientHelper) UpdateResolvingLocks(locks []*txnlock.Lock, callerStartTS uint64, token int) {
	ch.lockResolver.UpdateResolvingLocks(locks, callerStartTS, token)
}

// RecordResolvingLocks wraps the RecordResolvingLocks function
func (ch *ClientHelper) RecordResolvingLocks(locks []*txnlock.Lock, callerStartTS uint64) int {
	return ch.lockResolver.RecordResolvingLocks(locks, callerStartTS)
}

// ResolveLocksDone wraps the ResolveLocksDone function
func (ch *ClientHelper) ResolveLocksDone(callerStartTS uint64, token int) {
	ch.lockResolver.ResolveLocksDone(callerStartTS, token)
}

// SendReqCtx wraps the SendReqCtx function and use the resolved lock result in the kvrpcpb.Context.
func (ch *ClientHelper) SendReqCtx(bo *retry.Backoffer, req *tikvrpc.Request, regionID locate.RegionVerID, timeout time.Duration, et tikvrpc.EndpointType, directStoreAddr string, opts ...locate.StoreSelectorOption) (*tikvrpc.Response, *locate.RPCContext, string, error) {
	sender := locate.NewRegionRequestSender(ch.regionCache, ch.client)
	if len(directStoreAddr) > 0 {
		sender.SetStoreAddr(directStoreAddr)
	}
	sender.Stats = ch.Stats
	req.Context.ResolvedLocks = ch.resolvedLocks.GetAll()
	req.Context.CommittedLocks = ch.committedLocks.GetAll()
	resp, ctx, _, err := sender.SendReqCtx(bo, req, regionID, timeout, et, opts...)
	return resp, ctx, sender.GetStoreAddr(), err
}
