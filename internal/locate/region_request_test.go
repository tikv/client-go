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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/locate/region_request_test.go
//

// Copyright 2017 PingCAP, Inc.
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

package locate

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
	"unsafe"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/disaggregated"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/mpp"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/config/retry"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/apicodec"
	"github.com/tikv/client-go/v2/internal/client"
	"github.com/tikv/client-go/v2/internal/client/mockserver"
	"github.com/tikv/client-go/v2/internal/mockstore/mocktikv"
	"github.com/tikv/client-go/v2/internal/txnprotocol"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/oracle/oracles"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util/async"
	pd "github.com/tikv/pd/client"
	pderr "github.com/tikv/pd/client/errs"
	"google.golang.org/grpc"
)

func (s *testRegionRequestToSingleStoreSuite) TestCollapsedResolvePreservesTxnProtocolVersion() {
	useTxnProtocolVersionForLocate(s.T(), kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_SHARED_LOCK)
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	s.Require().NoError(err)
	store := s.resolveStoreForTxnVersionTests()
	s.publishStoreTxnProtocolVersionRange(store, 0, 2, true)
	for _, useAsync := range []bool{false, true} {
		s.Run(fmt.Sprintf("async=%t", useAsync), func() {
			var wire atomic.Uint32
			transport := &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
				version := req.ResolveLock().GetContext().GetTxnProtocolVersion()
				wire.Store(version)
				if version != 2 {
					return &tikvrpc.Response{Resp: &kvrpcpb.ResolveLockResponse{RegionError: txnVersionTestStrictRejection(version, 2, 2)}}, nil
				}
				return &tikvrpc.Response{Resp: &kvrpcpb.ResolveLockResponse{}}, nil
			}}
			sender := NewRegionRequestSender(s.cache, client.NewReqCollapse(transport), oracle.NoopReadTSValidator{})
			req := tikvrpc.NewRequest(tikvrpc.CmdResolveLock, &kvrpcpb.ResolveLockRequest{StartVersion: 1234})
			if !useAsync {
				_, _, err := sender.SendReq(s.bo, req, region.Region, time.Second)
				s.Require().NoError(err)
			} else {
				loop := async.NewRunLoop()
				done := false
				sender.SendReqAsync(s.bo, req, region.Region, time.Second, async.NewCallback(loop, func(_ *tikvrpc.ResponseExt, err error) {
					s.NoError(err)
					done = true
				}))
				for !done {
					_, err := loop.Exec(context.Background())
					s.Require().NoError(err)
				}
			}
			s.Equal(uint32(2), wire.Load())
		})
	}
}

func (s *testRegionRequestToSingleStoreSuite) TestTransportIncompatibilityIsTerminal() {
	useTxnProtocolVersionForLocate(s.T(), kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_SHARED_LOCK)
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	s.Require().NoError(err)
	control := wrapStoreCacheForTxnProtocolVersionRange(s.cache, 0, 1)
	store := s.resolveStoreForTxnVersionTests()
	s.publishStoreTxnProtocolVersionRange(store, 0, 1, true)
	for _, useAsync := range []bool{false, true} {
		for _, priorFailure := range []bool{false, true} {
			s.Run(fmt.Sprintf("async=%t/prior_rpc_failure=%t", useAsync, priorFailure), func() {
				var wireCalls, releases atomic.Int32
				transport := &fnClient{fn: func(context.Context, string, *tikvrpc.Request, time.Duration) (*tikvrpc.Response, error) {
					wireCalls.Add(1)
					return &tikvrpc.Response{Resp: &kvrpcpb.PrewriteResponse{}}, nil
				}}
				sender := NewRegionRequestSender(s.cache, transport, oracle.NoopReadTSValidator{})
				if priorFailure {
					sender.rpcError = errors.New("earlier attempt lost its response")
				}
				priorErr := sender.GetRPCError()
				req := txnVersionTestWritePrewriteRequest()
				req.RequestAttemptLimiter = func(context.Context, uint64) (func(), error) {
					req.Prewrite().Mutations[0].Op = kvrpcpb.Op_SharedLock
					return func() { releases.Add(1) }, nil
				}
				bo := retry.NewBackoffer(context.Background(), 1000)
				epoch, fetches := atomic.LoadUint32(&store.epoch), control.fetchCalls()
				var sendErr error
				if !useAsync {
					_, _, sendErr = sender.SendReq(bo, req, region.Region, time.Second)
				} else {
					loop := async.NewRunLoop()
					done := false
					sender.SendReqAsync(bo, req, region.Region, time.Second, async.NewCallback(loop, func(_ *tikvrpc.ResponseExt, err error) {
						sendErr, done = err, true
					}))
					for !done {
						_, err := loop.Exec(context.Background())
						s.Require().NoError(err)
					}
				}
				var incompatible *tikverr.ErrIncompatibleRequest
				s.Require().ErrorAs(sendErr, &incompatible)
				s.Equal(uint32(1), incompatible.GetProvidedTxnProtocolVersion())
				s.Equal(priorErr, sender.GetRPCError(), "a local rejection must neither introduce nor erase RPC uncertainty")
				s.Zero(wireCalls.Load())
				s.Equal(int32(1), releases.Load())
				s.Zero(bo.GetTotalSleep())
				s.Equal(fetches, control.fetchCalls())
				s.Equal(epoch, atomic.LoadUint32(&store.epoch))
				s.Equal(resolved, store.getResolveState())
				s.False(store.healthStatus.IsSlow())
			})
		}
	}
}

func (s *testRegionRequestToSingleStoreSuite) TestTxnProtocolResponseRangeAuthorizesResendWithoutReload() {
	useTxnProtocolVersionForLocate(s.T(), kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_SHARED_LOCK)
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	s.Require().NoError(err)
	control := wrapStoreCacheForTxnProtocolVersionRange(s.cache, 0, 1)
	store := s.resolveStoreForTxnVersionTests()
	s.publishStoreTxnProtocolVersionRange(store, 0, 2, true)
	rpcCtx, err := s.cache.GetTiKVRPCContext(s.bo, region.Region, kv.ReplicaReadLeader, 0)
	s.Require().NoError(err)
	// A PD failure must not affect recovery from the Store's explicit response.
	control.fail.Store(true)
	state := &sendReqState{RegionRequestSender: s.regionRequestSender}
	state.vars.txnVersion, state.vars.txnVersionSelected = 2, true
	rejection := txnVersionTestStrictRejection(2, 0, 1)
	fetches := control.fetchCalls()
	resend, err := state.onIncompatibleRequest(s.bo, rpcCtx, txnVersionTestGetRequest(), rejection.IncompatibleRequest)
	s.Require().NoError(err)
	s.Require().NotNil(resend)
	s.Same(rpcCtx, resend.rpcCtx)
	s.Equal(uint32(1), resend.version)
	s.Equal(fetches, control.fetchCalls())
	s.Equal(txnprotocol.StoreRange{Present: true, Min: 0, Max: 2}, store.getTxnProtocolVersionRange())
}

func (s *testRegionRequestToSingleStoreSuite) TestTxnProtocolResendIgnoresChangedCache() {
	useTxnProtocolVersionForLocate(s.T(), kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_SHARED_LOCK)
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	s.Require().NoError(err)
	store := s.resolveStoreForTxnVersionTests()
	s.publishStoreTxnProtocolVersionRange(store, 0, 2, true)
	rpcCtx, err := s.cache.GetTiKVRPCContext(s.bo, region.Region, kv.ReplicaReadLeader, 0)
	s.Require().NoError(err)
	var sentVersion uint32
	sender := NewRegionRequestSender(s.cache, &fnClient{fn: func(_ context.Context, _ string, req *tikvrpc.Request, _ time.Duration) (*tikvrpc.Response, error) {
		sentVersion = req.GetTxnProtocolVersion()
		return txnVersionTestSuccessResponse(req), nil
	}}, oracle.NoopReadTSValidator{})
	req := txnVersionTestGetRequest()
	state := &sendReqState{
		RegionRequestSender: sender,
		args:                sendReqArgs{bo: s.bo, req: req, regionID: region.Region, timeout: time.Second},
	}
	state.vars.txnVersion, state.vars.txnVersionSelected = 2, true
	rejection := txnVersionTestStrictRejection(2, 0, 1)
	// A concurrent publication must neither override the response-authorized
	// resend nor be overwritten by it.
	s.publishStoreTxnProtocolVersionRange(store, 0, 0, true)
	state.vars.rpcCtx = rpcCtx
	state.vars.regionErr = rejection
	s.True(state.next())
	s.NoError(state.vars.err)
	s.Equal(uint32(1), sentVersion)
	s.Equal(1, state.vars.sendTimes)
	s.Equal(txnprotocol.StoreRange{Present: true, Min: 0, Max: 0}, store.getTxnProtocolVersionRange())
}

func (s *testRegionRequestToSingleStoreSuite) TestTxnProtocolResponseRangeMustSatisfyPayload() {
	useTxnProtocolVersionForLocate(s.T(), kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_SHARED_LOCK)
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	s.Require().NoError(err)
	control := wrapStoreCacheForTxnProtocolVersionRange(s.cache, 0, 0)
	store := s.resolveStoreForTxnVersionTests()
	rpcCtx, err := s.cache.GetTiKVRPCContext(s.bo, region.Region, kv.ReplicaReadLeader, 0)
	s.Require().NoError(err)
	s.publishStoreTxnProtocolVersionRange(store, 0, 2, true)
	fetchesBefore := control.fetchCalls()
	state := &sendReqState{RegionRequestSender: s.regionRequestSender}
	state.vars.txnVersion, state.vars.txnVersionSelected = 3, true
	rejection := txnVersionTestStrictRejection(3, 0, 1)
	resend, err := state.onIncompatibleRequest(s.bo, rpcCtx, txnVersionTestSharedPrewriteRequest(), rejection.IncompatibleRequest)
	s.Nil(resend)
	var incompatible *tikverr.ErrIncompatibleRequest
	s.Require().ErrorAs(err, &incompatible)
	s.Same(rejection.IncompatibleRequest, incompatible.IncompatibleRequest)
	s.False(state.vars.txnVersionResendUsed)
	s.Equal(fetchesBefore, control.fetchCalls())
	s.Equal(txnprotocol.StoreRange{Present: true, Min: 0, Max: 2}, store.getTxnProtocolVersionRange())
}

func (s *testRegionRequestToSingleStoreSuite) TestAsyncTxnProtocolReloadDoesNotBlockCaller() {
	useTxnProtocolVersionForLocate(s.T(), kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_SHARED_LOCK)
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	s.Require().NoError(err)
	control := wrapStoreCacheForTxnProtocolVersionRange(s.cache, 0, 2)
	store := s.resolveStoreForTxnVersionTests()
	s.publishStoreTxnProtocolVersionRange(store, 0, 1, true)
	started, release := make(chan struct{}), make(chan struct{})
	var startedOnce, releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	control.onFetch = func() { startedOnce.Do(func() { close(started) }) }
	control.releaseFetch = release
	transport := &fnClient{fn: func(context.Context, string, *tikvrpc.Request, time.Duration) (*tikvrpc.Response, error) {
		return &tikvrpc.Response{Resp: &kvrpcpb.PrewriteResponse{}}, nil
	}}
	sender := NewRegionRequestSender(s.cache, transport, oracle.NoopReadTSValidator{})
	loop := async.NewRunLoop()
	done := false
	returned := make(chan struct{})
	go func() {
		sender.SendReqAsync(s.bo, txnVersionTestSharedPrewriteRequest(), region.Region, time.Second,
			async.NewCallback(loop, func(_ *tikvrpc.ResponseExt, err error) { s.NoError(err); done = true }))
		close(returned)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		s.T().Fatal("conditional reload did not start")
	}
	select {
	case <-returned:
	case <-time.After(time.Second):
		s.T().Fatal("async sender blocked on PD reload")
	}
	releaseOnce.Do(func() { close(release) })
	for !done {
		_, err := loop.Exec(context.Background())
		s.Require().NoError(err)
	}
}

func TestRegionRequestToSingleStore(t *testing.T) {
	suite.Run(t, new(testRegionRequestToSingleStoreSuite))
}

type testRegionRequestToSingleStoreSuite struct {
	suite.Suite
	cluster             *mocktikv.Cluster
	store               uint64
	peer                uint64
	region              uint64
	pdCli               pd.Client
	cache               *RegionCache
	bo                  *retry.Backoffer
	regionRequestSender *RegionRequestSender
	mvccStore           mocktikv.MVCCStore
}

func (s *testRegionRequestToSingleStoreSuite) SetupTest() {
	s.mvccStore = mocktikv.MustNewMVCCStore()
	s.cluster = mocktikv.NewCluster(s.mvccStore)
	s.store, s.peer, s.region = mocktikv.BootstrapWithSingleStore(s.cluster)
	s.pdCli = &CodecPDClient{mocktikv.NewPDClient(s.cluster), apicodec.NewCodecV1(apicodec.ModeTxn)}
	s.cache = NewRegionCache(s.pdCli)
	s.bo = retry.NewNoopBackoff(context.Background())
	client := mocktikv.NewRPCClient(s.cluster, s.mvccStore, nil)
	s.regionRequestSender = NewRegionRequestSender(s.cache, client, oracle.NoopReadTSValidator{})

	s.NoError(failpoint.Enable("tikvclient/doNotRecoverStoreHealthCheckPanic", "return"))
}

func (s *testRegionRequestToSingleStoreSuite) TearDownTest() {
	s.cache.Close()
	s.mvccStore.Close()

	s.NoError(failpoint.Disable("tikvclient/doNotRecoverStoreHealthCheckPanic"))
}

type fnClient struct {
	fn         func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error)
	closedAddr string
	closedVer  uint64
}

func (f *fnClient) Close() error {
	return nil
}

func (f *fnClient) CloseAddr(addr string) error {
	return f.CloseAddrVer(addr, math.MaxUint64)
}

func (f *fnClient) CloseAddrVer(addr string, ver uint64) error {
	f.closedAddr = addr
	f.closedVer = ver
	return nil
}

func (f *fnClient) SetEventListener(listener client.ClientEventListener) {}

func (f *fnClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	rpcCtx, err := client.PrepareContextForTransport(ctx, req)
	if err != nil {
		return nil, err
	}
	tikvrpc.AttachContext(req, rpcCtx)
	return f.fn(ctx, addr, req, timeout)
}

func (f *fnClient) SendRequestAsync(ctx context.Context, addr string, req *tikvrpc.Request, cb async.Callback[*tikvrpc.Response]) {
	go func() {
		rpcCtx, err := client.PrepareContextForTransport(ctx, req)
		if err != nil {
			cb.Invoke(nil, err)
			return
		}
		tikvrpc.AttachContext(req, rpcCtx)
		cb.Schedule(f.fn(ctx, addr, req, 0))
	}()
}

// immediateAsyncClient invokes the callback in SendRequestAsync's caller
// goroutine, which some client implementations do for synchronous failures.
type immediateAsyncClient struct {
	*fnClient
	finished chan<- struct{}
}

func (c *immediateAsyncClient) SendRequestAsync(ctx context.Context, addr string, req *tikvrpc.Request, cb async.Callback[*tikvrpc.Response]) {
	rpcCtx, err := client.PrepareContextForTransport(ctx, req)
	if err != nil {
		cb.Invoke(nil, err)
		close(c.finished)
		return
	}
	tikvrpc.AttachContext(req, rpcCtx)
	cb.Invoke(c.fn(ctx, addr, req, 0))
	close(c.finished)
}

func (s *testRegionRequestToSingleStoreSuite) TestOnRegionError() {
	req := tikvrpc.NewRequest(tikvrpc.CmdRawPut, &kvrpcpb.RawPutRequest{
		Key:   []byte("key"),
		Value: []byte("value"),
	})
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	s.Nil(err)
	s.NotNil(region)

	// test stale command retry.
	test := func() {
		oc := s.regionRequestSender.client
		defer func() {
			s.regionRequestSender.client = oc
		}()
		s.regionRequestSender.client = &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (response *tikvrpc.Response, err error) {
			staleResp := &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{
				RegionError: &errorpb.Error{StaleCommand: &errorpb.StaleCommand{}},
			}}
			return staleResp, nil
		}}
		bo := retry.NewBackofferWithVars(context.Background(), 5, nil)
		resp, _, err := s.regionRequestSender.SendReq(bo, req, region.Region, time.Second)
		s.Nil(err)
		s.NotNil(resp)
		regionErr, _ := resp.GetRegionError()
		s.NotNil(regionErr)
	}

	s.Run("Default", test)

	failpoint.Enable("tikvclient/useSendReqAsync", `return(true)`)
	defer failpoint.Disable("tikvclient/useSendReqAsync")
	s.Run("AsyncAPI", test)
}

// TestSendReqTerminatesOnIncompatibleRequest verifies through the send state
// machine that a structured compatibility rejection reaches the caller after a
// single attempt, without retrying, backing off or switching the replica.
//
// Each case answers with the response envelope its command really returns,
// because Response.GetRegionError dispatches on the concrete response type: a
// Cop request answered with a GetResponse would take a different code path and
// would prove nothing about the Coprocessor envelopes.
func (s *testRegionRequestToSingleStoreSuite) TestSendReqTerminatesOnIncompatibleRequest() {
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	s.Require().NoError(err)
	s.Require().NotNil(region)

	// The response carries the structured error together with the legacy
	// ServerIsBusy fallback. A retry-capable backoffer is used on purpose: if the
	// fallback were consulted, the request would be retried more than once.
	incompatibleErr := incompatibleRequestError(errorpb.IncompatibleRequestReason_IncompatibleRequestReasonTxnProtocolVersionOutOfRange)
	incompatibleErr.ServerIsBusy = &errorpb.ServerIsBusy{Reason: "txn_protocol_incompatible"}

	for _, tc := range []struct {
		name string
		req  *tikvrpc.Request
		// resp is the envelope a real store would return for req.
		resp func() *tikvrpc.Response
	}{
		{
			name: "Get",
			req:  tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("key")}),
			resp: func() *tikvrpc.Response {
				return &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{RegionError: incompatibleErr}}
			},
		},
		{
			name: "Cop",
			req:  tikvrpc.NewRequest(tikvrpc.CmdCop, &coprocessor.Request{}),
			resp: func() *tikvrpc.Response {
				return &tikvrpc.Response{Resp: &coprocessor.Response{RegionError: incompatibleErr}}
			},
		},
		{
			// Declaring CmdCopStream reuses the Cop request shape, but the region
			// error arrives in the first message, which the client stores inside
			// the stream response. SendReq must still terminate on it before the
			// caller starts iterating the stream.
			name: "CopStream",
			req:  tikvrpc.NewRequest(tikvrpc.CmdCopStream, &coprocessor.Request{}),
			resp: func() *tikvrpc.Response {
				return &tikvrpc.Response{Resp: &tikvrpc.CopStreamResponse{
					Response: &coprocessor.Response{RegionError: incompatibleErr},
				}}
			},
		},
	} {
		s.Run(tc.name, func() {
			attempts := 0
			originalClient := s.regionRequestSender.client
			defer func() {
				s.regionRequestSender.client = originalClient
			}()
			s.regionRequestSender.client = &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
				attempts++
				// Response.GetRegionError dispatches on the concrete response type,
				// so a mismatched envelope would take a different path and prove
				// nothing about the envelope this case claims to cover.
				s.Equal(tc.req.Type, req.Type)
				resp := tc.resp()
				s.assertRegionErrorEnvelope(req.Type, resp)
				return resp, nil
			}}

			bo := retry.NewBackofferWithVars(context.Background(), 100, nil)
			resp, _, err := s.regionRequestSender.SendReq(bo, tc.req, region.Region, time.Second)

			// A non-nil, typed, terminal error after exactly one attempt.
			s.Require().Error(err)
			var incompatible *tikverr.ErrIncompatibleRequest
			s.Require().ErrorAs(err, &incompatible)
			s.Equal(incompatibleErr.GetIncompatibleRequest(), incompatible.IncompatibleRequest)
			s.Nil(resp)
			s.Equal(1, attempts)
			s.Equal(0, bo.GetTotalBackoffTimes())
		})
	}
}

// assertRegionErrorEnvelope checks that a canned response carries the envelope
// shape its command really produces, so the table above stays honest.
func (s *testRegionRequestToSingleStoreSuite) assertRegionErrorEnvelope(cmd tikvrpc.CmdType, resp *tikvrpc.Response) {
	s.T().Helper()
	switch cmd {
	case tikvrpc.CmdGet:
		_, ok := resp.Resp.(*kvrpcpb.GetResponse)
		s.True(ok, "CmdGet must be answered with a GetResponse")
	case tikvrpc.CmdCop:
		_, ok := resp.Resp.(*coprocessor.Response)
		s.True(ok, "CmdCop must be answered with a Coprocessor response")
	case tikvrpc.CmdCopStream:
		_, ok := resp.Resp.(*tikvrpc.CopStreamResponse)
		s.True(ok, "CmdCopStream must be answered with a CopStreamResponse")
	}
}

// resolveStoreForTxnVersionTests forces the store of the test region to be
// resolved so that a range published afterwards is not overwritten by the
// initial metadata load.
func (s *testRegionRequestToSingleStoreSuite) resolveStoreForTxnVersionTests() *Store {
	s.T().Helper()
	store := s.cache.stores.getOrInsertDefault(s.store)
	_, err := store.initResolve(s.bo, s.cache.stores)
	s.Require().NoError(err)
	return store
}

func (s *testRegionRequestToSingleStoreSuite) publishStoreTxnProtocolVersionRange(store *Store, min, max uint32, present bool) {
	s.T().Helper()
	if !present {
		store.publishTxnProtocolVersionRange(&metapb.Store{Id: s.store}, time.Now())
		return
	}
	store.publishTxnProtocolVersionRange(storeMetaWithRange(s.store, min, max), time.Now())
}

func txnVersionTestGetRequest() *tikvrpc.Request {
	return tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("key")})
}

func txnVersionTestSharedPrewriteRequest() *tikvrpc.Request {
	return tikvrpc.NewRequest(tikvrpc.CmdPrewrite, &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{{Op: kvrpcpb.Op_SharedLock, Key: []byte("key")}},
	})
}

func txnVersionTestWritePrewriteRequest() *tikvrpc.Request {
	return tikvrpc.NewRequest(tikvrpc.CmdPrewrite, &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{{Op: kvrpcpb.Op_Put, Key: []byte("key")}},
	})
}

func txnVersionTestSuccessResponse(req *tikvrpc.Request) *tikvrpc.Response {
	switch req.Type {
	case tikvrpc.CmdPrewrite:
		return &tikvrpc.Response{Resp: &kvrpcpb.PrewriteResponse{}}
	case tikvrpc.CmdPessimisticLock:
		return &tikvrpc.Response{Resp: &kvrpcpb.PessimisticLockResponse{}}
	default:
		return &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{}}
	}
}

func txnVersionTestStrictRejection(provided, min, max uint32) *errorpb.Error {
	return &errorpb.Error{
		IncompatibleRequest: &errorpb.IncompatibleRequest{
			Reason:                          errorpb.IncompatibleRequestReason_IncompatibleRequestReasonTxnProtocolVersionOutOfRange,
			Message:                         "declared transaction protocol version is not compatible",
			ProvidedTxnProtocolVersion:      provided,
			MinCompatibleTxnProtocolVersion: min,
			MaxCompatibleTxnProtocolVersion: max,
		},
	}
}

// TestSendReqSelectsTxnProtocolVersionFromExecutionStore is the shared selection
// matrix at the sender boundary: selected = min(process ceiling, execution Store
// max), rejected when it falls below the Store min or the payload requirement.
func (s *testRegionRequestToSingleStoreSuite) TestSendReqSelectsTxnProtocolVersionFromExecutionStore() {
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	s.Require().NoError(err)
	s.Require().NotNil(region)
	store := s.resolveStoreForTxnVersionTests()

	for _, tc := range []struct {
		name       string
		protocol   kvrpcpb.TxnProtocolVersion
		storeRange *[2]uint32
		req        func() *tikvrpc.Request
		version    uint32
		sent       bool
	}{
		{
			name:     "unknown_range_legacy_payload",
			protocol: kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_SHARED_LOCK,
			req:      txnVersionTestGetRequest,
			version:  0,
			sent:     true,
		},
		{
			name:       "store_caps_below_process_ceiling",
			protocol:   kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_SHARED_LOCK,
			storeRange: &[2]uint32{0, 1},
			req:        txnVersionTestGetRequest,
			version:    1,
			sent:       true,
		},
		{
			name:       "store_allows_process_ceiling",
			protocol:   kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_SHARED_LOCK,
			storeRange: &[2]uint32{0, 2},
			req:        txnVersionTestGetRequest,
			version:    2,
			sent:       true,
		},
		{
			name:       "legacy_process",
			protocol:   kvrpcpb.TxnProtocolVersion_TXN_VER_LEGACY,
			storeRange: &[2]uint32{0, 2},
			req:        txnVersionTestGetRequest,
			version:    0,
			sent:       true,
		},
		{
			name:       "shared_payload_within_range",
			protocol:   kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_SHARED_LOCK,
			storeRange: &[2]uint32{0, 2},
			req:        txnVersionTestSharedPrewriteRequest,
			version:    2,
			sent:       true,
		},
		{
			name:       "shared_payload_above_process_ceiling",
			protocol:   kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_INCOMPATIBLE_ERROR_HANDLING,
			storeRange: &[2]uint32{0, 2},
			req:        txnVersionTestSharedPrewriteRequest,
			sent:       false,
		},
		{
			name:       "store_min_above_process_ceiling",
			protocol:   kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_INCOMPATIBLE_ERROR_HANDLING,
			storeRange: &[2]uint32{2, 2},
			req:        txnVersionTestGetRequest,
			sent:       false,
		},
	} {
		s.Run(tc.name, func() {
			useTxnProtocolVersionForLocate(s.T(), tc.protocol)
			if tc.storeRange == nil {
				s.publishStoreTxnProtocolVersionRange(store, 0, 0, false)
			} else {
				s.publishStoreTxnProtocolVersionRange(store, tc.storeRange[0], tc.storeRange[1], true)
			}

			var versions []uint32
			originalClient := s.regionRequestSender.client
			s.T().Cleanup(func() { s.regionRequestSender.client = originalClient })
			s.regionRequestSender.client = &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
				versions = append(versions, req.GetTxnProtocolVersion())
				return txnVersionTestSuccessResponse(req), nil
			}}

			bo := retry.NewNoopBackoff(context.Background())
			resp, _, err := s.regionRequestSender.SendReq(bo, tc.req(), region.Region, time.Second)
			if tc.sent {
				s.Require().NoError(err)
				s.Require().NotNil(resp)
				s.Equal([]uint32{tc.version}, versions)
			} else {
				s.Require().Error(err)
				s.Empty(versions)
				var incompatible *tikverr.ErrIncompatibleRequest
				s.Require().ErrorAs(err, &incompatible)
				// A local failure must not masquerade as a server rejection.
				s.Equal(errorpb.IncompatibleRequestReason_IncompatibleRequestReasonUnknown, incompatible.GetReason())
			}
		})
	}
}

// TestSendReqConditionalReloadBeforeSend covers the two conditions that may
// trigger a pre-send PD reload and the cases that must not.
func (s *testRegionRequestToSingleStoreSuite) TestSendReqConditionalReloadBeforeSend() {
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	s.Require().NoError(err)
	s.Require().NotNil(region)

	s.Run("payload_requires_more_reloads", func() {
		t := s.T()
		control := wrapStoreCacheForTxnProtocolVersionRange(s.cache, 0, 1)
		store := s.resolveStoreForTxnVersionTests()
		s.publishStoreTxnProtocolVersionRange(store, 0, 1, true)
		useTxnProtocolVersionForLocate(t, kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_SHARED_LOCK)
		// PD reports a wider range on reload.
		control.setRange(0, 2)

		var versions []uint32
		originalClient := s.regionRequestSender.client
		t.Cleanup(func() { s.regionRequestSender.client = originalClient })
		s.regionRequestSender.client = &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
			versions = append(versions, req.GetTxnProtocolVersion())
			return txnVersionTestSuccessResponse(req), nil
		}}

		before := control.fetchCalls()
		bo := retry.NewNoopBackoff(context.Background())
		_, _, err := s.regionRequestSender.SendReq(bo, txnVersionTestSharedPrewriteRequest(), region.Region, time.Second)
		require.NoError(t, err)
		require.Equal(t, []uint32{2}, versions)
		require.Equal(t, before+1, control.fetchCalls(), "exactly one conditional reload")
		require.Equal(t, txnprotocol.StoreRange{Present: true, Min: 0, Max: 2}, store.getTxnProtocolVersionRange())
	})

	s.Run("ordinary_downgrade_does_not_reload", func() {
		t := s.T()
		control := wrapStoreCacheForTxnProtocolVersionRange(s.cache, 0, 1)
		store := s.resolveStoreForTxnVersionTests()
		s.publishStoreTxnProtocolVersionRange(store, 0, 1, true)
		useTxnProtocolVersionForLocate(t, kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_SHARED_LOCK)

		var versions []uint32
		originalClient := s.regionRequestSender.client
		t.Cleanup(func() { s.regionRequestSender.client = originalClient })
		s.regionRequestSender.client = &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
			versions = append(versions, req.GetTxnProtocolVersion())
			return txnVersionTestSuccessResponse(req), nil
		}}

		before := control.fetchCalls()
		bo := retry.NewNoopBackoff(context.Background())
		_, _, err := s.regionRequestSender.SendReq(bo, txnVersionTestGetRequest(), region.Region, time.Second)
		require.NoError(t, err)
		require.Equal(t, []uint32{1}, versions)
		require.Equal(t, before, control.fetchCalls())
	})

	s.Run("process_ceiling_below_required_does_not_reload", func() {
		t := s.T()
		control := wrapStoreCacheForTxnProtocolVersionRange(s.cache, 0, 2)
		store := s.resolveStoreForTxnVersionTests()
		s.publishStoreTxnProtocolVersionRange(store, 0, 2, true)
		useTxnProtocolVersionForLocate(t, kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_INCOMPATIBLE_ERROR_HANDLING)

		attempts := 0
		originalClient := s.regionRequestSender.client
		t.Cleanup(func() { s.regionRequestSender.client = originalClient })
		s.regionRequestSender.client = &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
			attempts++
			return txnVersionTestSuccessResponse(req), nil
		}}

		before := control.fetchCalls()
		bo := retry.NewNoopBackoff(context.Background())
		_, _, err := s.regionRequestSender.SendReq(bo, txnVersionTestSharedPrewriteRequest(), region.Region, time.Second)
		require.Error(t, err)
		var incompatible *tikverr.ErrIncompatibleRequest
		require.ErrorAs(t, err, &incompatible)
		require.Equal(t, errorpb.IncompatibleRequestReason_IncompatibleRequestReasonUnknown, incompatible.GetReason())
		require.Zero(t, attempts)
		require.Equal(t, before, control.fetchCalls())
	})
}

// TestSendReqWithoutTrustedStoreFallsBackToUnknownRange pins the no-store
// fallback: protected commands declare unknown [0, 0], which lets a legacy
// payload through and fails closed for anything newer, while unprotected
// commands are not affected.
func (s *testRegionRequestToSingleStoreSuite) TestSendReqWithoutTrustedStoreFallsBackToUnknownRange() {
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	s.Require().NoError(err)
	s.Require().NotNil(region)

	useTxnProtocolVersionForLocate(s.T(), kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_SHARED_LOCK)

	var versions []uint32
	originalClient := s.regionRequestSender.client
	s.T().Cleanup(func() { s.regionRequestSender.client = originalClient })
	s.regionRequestSender.client = &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
		versions = append(versions, req.GetTxnProtocolVersion())
		return txnVersionTestSuccessResponse(req), nil
	}}

	bo := retry.NewNoopBackoff(context.Background())
	// A TiDB endpoint RPC context has no execution Store.
	_, _, _, err = s.regionRequestSender.SendReqCtx(bo, txnVersionTestGetRequest(), region.Region, time.Second, tikvrpc.TiDB)
	s.Require().NoError(err)
	s.Equal([]uint32{0}, versions)

	// A payload that requires a newer version fails closed locally.
	req := txnVersionTestSharedPrewriteRequest()
	_, _, _, err = s.regionRequestSender.SendReqCtx(bo, req, region.Region, time.Second, tikvrpc.TiDB)
	s.Require().Error(err)
	var incompatible *tikverr.ErrIncompatibleRequest
	s.Require().ErrorAs(err, &incompatible)
	s.Equal(errorpb.IncompatibleRequestReason_IncompatibleRequestReasonUnknown, incompatible.GetReason())
	s.Equal([]uint32{0}, versions)

	// An unprotected command keeps its context and is not gated.
	unprotected := tikvrpc.NewRequest(tikvrpc.CmdUnsafeDestroyRange, &kvrpcpb.UnsafeDestroyRangeRequest{})
	unprotected.TxnProtocolVersion = uint32(kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_INCOMPATIBLE_ERROR_HANDLING)
	_, _, _, err = s.regionRequestSender.SendReqCtx(bo, unprotected, region.Region, time.Second, tikvrpc.TiDB)
	s.Require().NoError(err)
	// Unprotected commands never carry a declaration: a caller-supplied value is
	// cleared instead of reaching the wire.
	s.Equal([]uint32{0, 0}, versions)
}

// TestSendReqRecoversFromStrictUpperBoundRejection proves the controlled resend:
// a strict upper-bound admission rejection selects from the returned range and
// resends the same shard without re-executing the business action twice.
// noAttachClient models a send implementation that returns before attaching a
// wire context, for example due to an encode failure.
type noAttachClient struct {
	client.Client
}

func (c *noAttachClient) SendRequest(_ context.Context, _ string, req *tikvrpc.Request, _ time.Duration) (*tikvrpc.Response, error) {
	return txnVersionTestSuccessResponse(req), nil
}

// assertNoTrustedTxnProtocolVersion checks that a direct/no-Store transport has
// no sender hint: an ordinary payload falls back to legacy 0, while a payload
// that needs a newer version fails closed locally with reason Unknown.
func (s *testRegionRequestToSingleStoreSuite) assertNoTrustedTxnProtocolVersion(req *tikvrpc.Request, required uint32) {
	s.T().Helper()
	rpcCtx, err := client.PrepareContextForTransport(context.Background(), req)
	if required > 0 {
		s.Require().Error(err)
		var incompatible *tikverr.ErrIncompatibleRequest
		s.Require().ErrorAs(err, &incompatible)
		s.Equal(errorpb.IncompatibleRequestReason_IncompatibleRequestReasonUnknown, incompatible.GetReason())
		return
	}
	s.Require().NoError(err)
	s.True(tikvrpc.AttachContext(req, rpcCtx))
	s.Zero(req.GetTxnProtocolVersion())
	s.Zero(req.Get().GetContext().GetTxnProtocolVersion())
}

// TestSendReqPreSendRejectionDoesNotLeakSenderHint covers attempts rejected by
// the request limiter or store token before transport. Their sender hint is
// scoped to the abandoned context and cannot affect a later direct send.
func (s *testRegionRequestToSingleStoreSuite) TestSendReqPreSendRejectionDoesNotLeakSenderHint() {
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	s.Require().NoError(err)
	s.Require().NotNil(region)
	store := s.resolveStoreForTxnVersionTests()
	s.publishStoreTxnProtocolVersionRange(store, 0, 2, true)
	useTxnProtocolVersionForLocate(s.T(), kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_SHARED_LOCK)

	reject := func(t *testing.T, req *tikvrpc.Request) {
		t.Helper()
		var sent atomic.Int32
		originalClient := s.regionRequestSender.client
		t.Cleanup(func() { s.regionRequestSender.client = originalClient })
		s.regionRequestSender.client = &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
			sent.Add(1)
			return txnVersionTestSuccessResponse(req), nil
		}}
		bo := retry.NewNoopBackoff(context.Background())
		_, _, err := s.regionRequestSender.SendReq(bo, req, region.Region, time.Second)
		require.Error(t, err)
		require.Zero(t, sent.Load(), "the attempt must be rejected before the RPC client")
	}

	s.Run("attempt limiter", func() {
		t := s.T()
		limiterErr := errors.New("attempt limiter rejected the request")

		ordinary := txnVersionTestGetRequest()
		ordinary.RequestAttemptLimiter = func(context.Context, uint64) (func(), error) {
			return nil, limiterErr
		}
		reject(t, ordinary)
		s.assertNoTrustedTxnProtocolVersion(ordinary, 0)

		shared := txnVersionTestSharedPrewriteRequest()
		shared.RequestAttemptLimiter = func(context.Context, uint64) (func(), error) {
			return nil, limiterErr
		}
		reject(t, shared)
		s.assertNoTrustedTxnProtocolVersion(shared, 2)
	})

	s.Run("store token", func() {
		t := s.T()
		previousLimit := kv.StoreLimit.Load()
		kv.StoreLimit.Store(1)
		store.tokenCount.Store(1)
		t.Cleanup(func() {
			kv.StoreLimit.Store(previousLimit)
			store.tokenCount.Store(0)
		})

		ordinary := txnVersionTestGetRequest()
		reject(t, ordinary)
		s.assertNoTrustedTxnProtocolVersion(ordinary, 0)
	})
}

// TestSendReqTransportWithoutAttachDoesNotLeakSenderHint covers a transport
// that answers without attaching a context. The sender hint remains scoped to
// that call and cannot affect later direct sends.
func (s *testRegionRequestToSingleStoreSuite) TestSendReqTransportWithoutAttachDoesNotLeakSenderHint() {
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	s.Require().NoError(err)
	s.Require().NotNil(region)
	store := s.resolveStoreForTxnVersionTests()
	s.publishStoreTxnProtocolVersionRange(store, 0, 2, true)
	useTxnProtocolVersionForLocate(s.T(), kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_SHARED_LOCK)

	for _, tc := range []struct {
		name     string
		req      func() *tikvrpc.Request
		required uint32
	}{
		{name: "ordinary", req: txnVersionTestGetRequest, required: 0},
		{name: "shared", req: txnVersionTestSharedPrewriteRequest, required: 2},
	} {
		s.Run(tc.name, func() {
			sender := NewRegionRequestSender(s.cache, &noAttachClient{}, oracle.NoopReadTSValidator{})
			loc, err := s.cache.LocateRegionByID(s.bo, s.region)
			s.Require().NoError(err)
			s.Require().NotNil(loc)

			req := tc.req()
			bo := retry.NewNoopBackoff(context.Background())
			resp, _, err := sender.SendReq(bo, req, loc.Region, time.Second)
			s.Require().NoError(err)
			s.Require().NotNil(resp)
			s.assertNoTrustedTxnProtocolVersion(req, tc.required)
		})
	}
}

// TestSendReqAsyncPreSendRejectionDoesNotLeakSenderHint covers the same
// context-scoping contract for the async first attempt.
func (s *testRegionRequestToSingleStoreSuite) TestSendReqAsyncPreSendRejectionDoesNotLeakSenderHint() {
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	s.Require().NoError(err)
	s.Require().NotNil(region)
	store := s.resolveStoreForTxnVersionTests()
	s.publishStoreTxnProtocolVersionRange(store, 0, 2, true)
	useTxnProtocolVersionForLocate(s.T(), kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_SHARED_LOCK)

	limiterErr := errors.New("attempt limiter rejected the async request")
	sendAsync := func(t *testing.T, req *tikvrpc.Request) {
		t.Helper()
		var sent atomic.Int32
		originalClient := s.regionRequestSender.client
		t.Cleanup(func() { s.regionRequestSender.client = originalClient })
		s.regionRequestSender.client = &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
			sent.Add(1)
			return txnVersionTestSuccessResponse(req), nil
		}}

		runLoop := async.NewRunLoop()
		var asyncErr error
		completed := false
		s.regionRequestSender.SendReqAsync(s.bo, req, region.Region, time.Second, async.NewCallback(runLoop, func(resp *tikvrpc.ResponseExt, err error) {
			asyncErr = err
			completed = true
		}))
		for !completed {
			_, err := runLoop.Exec(context.Background())
			require.NoError(t, err)
		}
		require.Error(t, asyncErr)
		require.Zero(t, sent.Load(), "the async attempt must be rejected before the RPC client")
	}

	ordinary := txnVersionTestGetRequest()
	ordinary.RequestAttemptLimiter = func(context.Context, uint64) (func(), error) {
		return nil, limiterErr
	}
	sendAsync(s.T(), ordinary)
	s.assertNoTrustedTxnProtocolVersion(ordinary, 0)

	shared := txnVersionTestSharedPrewriteRequest()
	shared.RequestAttemptLimiter = func(context.Context, uint64) (func(), error) {
		return nil, limiterErr
	}
	sendAsync(s.T(), shared)
	s.assertNoTrustedTxnProtocolVersion(shared, 2)
}

func (s *testRegionRequestToSingleStoreSuite) TestSendReqRecoversFromStrictUpperBoundRejection() {
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	s.Require().NoError(err)
	s.Require().NotNil(region)

	control := wrapStoreCacheForTxnProtocolVersionRange(s.cache, 0, 2)
	store := s.resolveStoreForTxnVersionTests()
	s.publishStoreTxnProtocolVersionRange(store, 0, 2, true)
	useTxnProtocolVersionForLocate(s.T(), kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_SHARED_LOCK)
	// A Store rollback lowers its admission ceiling. Its response authorizes one
	// downgrade without waiting for PD metadata to catch up.
	control.setRange(0, 1)
	fetches := control.fetchCalls()

	var (
		attempts []uint32
		applied  int
	)
	originalClient := s.regionRequestSender.client
	s.T().Cleanup(func() { s.regionRequestSender.client = originalClient })
	s.regionRequestSender.client = &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
		attempts = append(attempts, req.GetTxnProtocolVersion())
		if len(attempts) == 1 {
			return &tikvrpc.Response{Resp: &kvrpcpb.PrewriteResponse{
				RegionError: txnVersionTestStrictRejection(req.GetTxnProtocolVersion(), 0, 1),
			}}, nil
		}
		applied++
		return txnVersionTestSuccessResponse(req), nil
	}}

	bo := retry.NewNoopBackoff(context.Background())
	resp, retryTimes, err := s.regionRequestSender.SendReq(bo, txnVersionTestWritePrewriteRequest(), region.Region, time.Second)
	s.Require().NoError(err)
	s.Require().NotNil(resp)
	s.Equal([]uint32{2, 1}, attempts)
	s.Equal(1, retryTimes)
	// The rejected attempt had no side effect; the business action ran once.
	s.Equal(1, applied)
	s.Equal(fetches, control.fetchCalls())
	s.Equal(txnprotocol.StoreRange{Present: true, Min: 0, Max: 2}, store.getTxnProtocolVersionRange())
}

// TestOnIncompatibleRequestStrictRecoveryBranches covers every branch that must
// stay terminal, plus the undetermined priority.
func (s *testRegionRequestToSingleStoreSuite) TestOnIncompatibleRequestStrictRecoveryBranches() {
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	s.Require().NoError(err)
	s.Require().NotNil(region)

	rpcCtx, err := s.cache.GetTiKVRPCContext(retry.NewNoopBackoff(context.Background()), region.Region, kv.ReplicaReadLeader, 0)
	s.Require().NoError(err)
	s.Require().NotNil(rpcCtx)

	type outcome struct {
		retry   bool
		version uint32
		// terminal reports that a typed incompatible error must be returned.
		terminal bool
	}

	for _, tc := range []struct {
		name            string
		attemptSelected uint32
		resendUsed      bool
		prepare         func(t *testing.T, control *txnVersionPDControl, req *tikvrpc.Request) *errorpb.Error
		want            outcome
	}{
		{
			name:            "valid_upper_bound_resends",
			attemptSelected: 1,
			prepare: func(t *testing.T, control *txnVersionPDControl, req *tikvrpc.Request) *errorpb.Error {
				return txnVersionTestStrictRejection(1, 0, 0)
			},
			want: outcome{retry: true, version: 0},
		},
		{
			name:            "unknown_reason_is_terminal",
			attemptSelected: 1,
			prepare: func(t *testing.T, control *txnVersionPDControl, req *tikvrpc.Request) *errorpb.Error {
				err := txnVersionTestStrictRejection(1, 0, 0)
				err.IncompatibleRequest.Reason = errorpb.IncompatibleRequestReason_IncompatibleRequestReasonUnknown
				return err
			},
			want: outcome{terminal: true},
		},
		{
			name:            "invalid_returned_range_is_terminal",
			attemptSelected: 3,
			prepare: func(t *testing.T, control *txnVersionPDControl, req *tikvrpc.Request) *errorpb.Error {
				return txnVersionTestStrictRejection(3, 5, 2)
			},
			want: outcome{terminal: true},
		},
		{
			name:            "echoed_provided_mismatch_is_terminal",
			attemptSelected: 1,
			prepare: func(t *testing.T, control *txnVersionPDControl, req *tikvrpc.Request) *errorpb.Error {
				return txnVersionTestStrictRejection(3, 0, 0)
			},
			want: outcome{terminal: true},
		},
		{
			name:            "provided_within_range_is_terminal",
			attemptSelected: 1,
			prepare: func(t *testing.T, control *txnVersionPDControl, req *tikvrpc.Request) *errorpb.Error {
				return txnVersionTestStrictRejection(1, 0, 1)
			},
			want: outcome{terminal: true},
		},
		{
			name:            "response_range_cannot_satisfy_payload_is_terminal",
			attemptSelected: 3,
			prepare: func(t *testing.T, control *txnVersionPDControl, req *tikvrpc.Request) *errorpb.Error {
				*req = *txnVersionTestSharedPrewriteRequest()
				return txnVersionTestStrictRejection(3, 0, 1)
			},
			want: outcome{terminal: true},
		},
		{
			name:            "unprotected_current_payload_is_terminal",
			attemptSelected: 1,
			prepare: func(t *testing.T, control *txnVersionPDControl, req *tikvrpc.Request) *errorpb.Error {
				*req = *tikvrpc.NewRequest(tikvrpc.CmdRawGet, &kvrpcpb.RawGetRequest{})
				return txnVersionTestStrictRejection(1, 0, 0)
			},
			want: outcome{terminal: true},
		},
		{
			name:            "second_resend_is_terminal",
			attemptSelected: 1,
			resendUsed:      true,
			prepare: func(t *testing.T, control *txnVersionPDControl, req *tikvrpc.Request) *errorpb.Error {
				return txnVersionTestStrictRejection(1, 0, 0)
			},
			want: outcome{terminal: true},
		},
	} {
		s.Run(tc.name, func() {
			control := wrapStoreCacheForTxnProtocolVersionRange(s.cache, 0, 1)
			store := s.resolveStoreForTxnVersionTests()
			s.publishStoreTxnProtocolVersionRange(store, 0, 2, true)
			useTxnProtocolVersionForLocate(s.T(), kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_SHARED_LOCK)

			reqSend := &sendReqState{RegionRequestSender: NewRegionRequestSender(s.cache, nil, oracle.NoopReadTSValidator{})}
			reqSend.vars.txnVersion = tc.attemptSelected
			reqSend.vars.txnVersionSelected = true
			reqSend.vars.txnVersionResendUsed = tc.resendUsed
			req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{})
			regionErr := tc.prepare(s.T(), control, req)
			before := control.fetchCalls()

			bo := retry.NewNoopBackoff(context.Background())
			resend, err := reqSend.onIncompatibleRequest(bo, rpcCtx, req, regionErr.IncompatibleRequest)

			s.Equal(tc.want.retry, resend != nil)
			if tc.want.terminal {
				s.Require().Error(err)
				var incompatible *tikverr.ErrIncompatibleRequest
				s.Require().ErrorAs(err, &incompatible)
				s.Equal(regionErr.GetIncompatibleRequest(), incompatible.IncompatibleRequest)
			} else {
				s.NoError(err)
			}
			s.Equal(before, control.fetchCalls())
			s.Equal(txnprotocol.StoreRange{Present: true, Min: 0, Max: 2}, store.getTxnProtocolVersionRange())
			if tc.want.retry {
				s.Same(rpcCtx, resend.rpcCtx)
				s.Equal(tc.want.version, resend.version)
				s.True(reqSend.vars.txnVersionResendUsed)
			} else {
				s.Nil(resend)
			}
		})
	}
}

// TestOnRegionErrorUndeterminedBeatsIncompatible pins the priority inside one
// error envelope, including the suppression of any reload.
func (s *testRegionRequestToSingleStoreSuite) TestOnRegionErrorUndeterminedBeatsIncompatible() {
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	s.Require().NoError(err)
	s.Require().NotNil(region)

	control := wrapStoreCacheForTxnProtocolVersionRange(s.cache, 0, 2)
	store := s.resolveStoreForTxnVersionTests()
	s.publishStoreTxnProtocolVersionRange(store, 0, 2, true)
	useTxnProtocolVersionForLocate(s.T(), kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_SHARED_LOCK)

	rpcCtx, err := s.cache.GetTiKVRPCContext(retry.NewNoopBackoff(context.Background()), region.Region, kv.ReplicaReadLeader, 0)
	s.Require().NoError(err)
	s.Require().NotNil(rpcCtx)

	reqSend := NewRegionRequestSender(s.cache, nil, oracle.NoopReadTSValidator{})
	state := &sendReqState{RegionRequestSender: reqSend}
	state.vars.txnVersion = 1
	state.vars.txnVersionSelected = true
	req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{})

	regionErr := txnVersionTestStrictRejection(1, 0, 0)
	regionErr.UndeterminedResult = &errorpb.UndeterminedResult{}
	before := control.fetchCalls()

	bo := retry.NewNoopBackoff(context.Background())
	shouldRetry, err := state.onRegionError(bo, rpcCtx, req, regionErr)

	s.False(shouldRetry)
	s.NoError(err)
	s.False(state.vars.txnVersionResendUsed)
	s.Equal(before, control.fetchCalls())

	// Exercise the dispatch in next as well: the combined envelope must never
	// reach compatibility recovery.
	state.args = sendReqArgs{bo: bo, req: req}
	state.vars.rpcCtx = rpcCtx
	state.vars.regionErr = regionErr
	s.True(state.next())
	s.NoError(state.vars.err)
	s.False(state.vars.txnVersionResendUsed)
	s.Equal(before, control.fetchCalls())
}

// TestPrepareTxnProtocolVersionUsesExecutionStoreRange pins that forwarding uses
// the execution Store's range: the proxy only transports the request and must not
// decide the declaration.
func (s *testRegionRequestToSingleStoreSuite) TestPrepareTxnProtocolVersionUsesExecutionStoreRange() {
	useTxnProtocolVersionForLocate(s.T(), kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_SHARED_LOCK)

	executionStore := newUninitializedStore(1)
	executionStore.publishTxnProtocolVersionRange(storeMetaWithRange(1, 0, 2), time.Now())
	proxyStore := newUninitializedStore(2)
	proxyStore.publishTxnProtocolVersionRange(storeMetaWithRange(2, 0, 0), time.Now())

	req := txnVersionTestGetRequest()
	state := &sendReqState{
		RegionRequestSender: s.regionRequestSender,
		args:                sendReqArgs{bo: s.bo, req: req},
	}
	state.vars.rpcCtx = &RPCContext{
		Store:      executionStore,
		ProxyStore: proxyStore,
		Addr:       "execution-store",
		ProxyAddr:  "proxy-store",
	}

	selection, err := state.prepareTxnProtocolVersion()
	s.Require().NoError(err)
	s.True(selection.Protected)
	s.Equal(uint32(2), selection.Selected)
	s.Equal(uint32(2), state.vars.txnVersion)
	s.True(state.vars.txnVersionSelected)
}

// TestSendReqAsyncSelectsTxnProtocolVersion covers the async first attempt: it
// must select its declaration exactly like the sync path, and a local selection
// failure must reach the callback without sending anything.
func (s *testRegionRequestToSingleStoreSuite) TestSendReqAsyncSelectsTxnProtocolVersion() {
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	s.Require().NoError(err)
	s.Require().NotNil(region)
	store := s.resolveStoreForTxnVersionTests()
	s.publishStoreTxnProtocolVersionRange(store, 0, 1, true)
	useTxnProtocolVersionForLocate(s.T(), kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_SHARED_LOCK)

	var versions []uint32
	originalClient := s.regionRequestSender.client
	s.T().Cleanup(func() { s.regionRequestSender.client = originalClient })
	s.regionRequestSender.client = &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
		versions = append(versions, req.GetTxnProtocolVersion())
		return txnVersionTestSuccessResponse(req), nil
	}}

	runLoop := async.NewRunLoop()
	completed := false
	s.regionRequestSender.SendReqAsync(s.bo, txnVersionTestGetRequest(), region.Region, time.Second, async.NewCallback(runLoop, func(resp *tikvrpc.ResponseExt, err error) {
		s.NoError(err)
		s.NotNil(resp)
		completed = true
	}))
	for !completed {
		_, err := runLoop.Exec(context.Background())
		s.Require().NoError(err)
	}
	s.Equal([]uint32{1}, versions)

	// A payload the process ceiling cannot express must fail locally on the async
	// first attempt instead of being sent.
	useTxnProtocolVersionForLocate(s.T(), kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_INCOMPATIBLE_ERROR_HANDLING)
	attempts := 0
	s.regionRequestSender.client = &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
		attempts++
		return txnVersionTestSuccessResponse(req), nil
	}}
	runLoop = async.NewRunLoop()
	var asyncErr error
	completed = false
	s.regionRequestSender.SendReqAsync(s.bo, txnVersionTestSharedPrewriteRequest(), region.Region, time.Second, async.NewCallback(runLoop, func(resp *tikvrpc.ResponseExt, err error) {
		asyncErr = err
		completed = true
	}))
	for !completed {
		_, err := runLoop.Exec(context.Background())
		s.Require().NoError(err)
	}
	s.Require().Error(asyncErr)
	var incompatible *tikverr.ErrIncompatibleRequest
	s.Require().ErrorAs(asyncErr, &incompatible)
	s.Zero(attempts)
}

// TestSendReqCanceledReloadWaiterDoesNotSend covers waiter cancellation during a
// conditional reload: a caller that gives up must stop the attempt without
// reaching the RPC client, while the shared reload keeps running for others.
func (s *testRegionRequestToSingleStoreSuite) TestSendReqCanceledReloadWaiterDoesNotSend() {
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	s.Require().NoError(err)
	s.Require().NotNil(region)

	control := wrapStoreCacheForTxnProtocolVersionRange(s.cache, 0, 1)
	store := s.resolveStoreForTxnVersionTests()
	s.publishStoreTxnProtocolVersionRange(store, 0, 1, true)
	useTxnProtocolVersionForLocate(s.T(), kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_SHARED_LOCK)

	started := make(chan struct{})
	var startedOnce sync.Once
	control.onFetch = func() { startedOnce.Do(func() { close(started) }) }
	control.releaseFetch = make(chan struct{})
	// The reload the shared task performs learns a wider range.
	control.setRange(0, 2)

	// Hold the Store's single-flight open with a background reload.
	ownerDone := make(chan struct{})
	go func() {
		defer close(ownerDone)
		store.reloadTxnProtocolVersionRange(context.Background(), control.cache, context.Background())
	}()
	<-started

	var rpcCalls atomic.Int32
	client := &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
		rpcCalls.Add(1)
		return txnVersionTestSuccessResponse(req), nil
	}}
	waiterSender := NewRegionRequestSender(s.cache, client, oracle.NoopReadTSValidator{})

	waiterCtx, cancelWaiter := context.WithCancel(context.Background())
	waiterErr := make(chan error, 1)
	go func() {
		bo := retry.NewBackofferWithVars(waiterCtx, 100, nil)
		_, _, err := waiterSender.SendReq(bo, txnVersionTestSharedPrewriteRequest(), region.Region, time.Second)
		waiterErr <- err
	}()

	// Give the waiter time to join the in-flight reload, then cancel it. The
	// shared reload completes afterwards.
	time.Sleep(20 * time.Millisecond)
	cancelWaiter()
	select {
	case err := <-waiterErr:
		s.Require().Error(err)
	case <-time.After(2 * time.Second):
		s.T().Fatal("a canceled reload waiter must stop waiting")
	}

	close(control.releaseFetch)
	<-ownerDone

	s.Zero(rpcCalls.Load(), "a canceled waiter must not reach the RPC client")
	s.Equal(txnprotocol.StoreRange{Present: true, Min: 0, Max: 2}, store.getTxnProtocolVersionRange())

	// The published range is usable by a later, uncanceled attempt.
	uncanceledSender := NewRegionRequestSender(s.cache, client, oracle.NoopReadTSValidator{})
	bo := retry.NewNoopBackoff(context.Background())
	_, _, err = uncanceledSender.SendReq(bo, txnVersionTestSharedPrewriteRequest(), region.Region, time.Second)
	s.Require().NoError(err)
	s.Equal(int32(1), rpcCalls.Load())
}

// TestSendReqOwnerCanceledDuringReloadDoesNotSend covers the caller that starts
// the shared reload: canceling its own request context must return immediately
// and stop the attempt, while the shared PD task keeps running and publishes its
// result for later requests.
func (s *testRegionRequestToSingleStoreSuite) TestSendReqOwnerCanceledDuringReloadDoesNotSend() {
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	s.Require().NoError(err)
	s.Require().NotNil(region)

	control := wrapStoreCacheForTxnProtocolVersionRange(s.cache, 0, 1)
	store := s.resolveStoreForTxnVersionTests()
	s.publishStoreTxnProtocolVersionRange(store, 0, 1, true)
	useTxnProtocolVersionForLocate(s.T(), kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_SHARED_LOCK)

	started := make(chan struct{})
	var startedOnce sync.Once
	control.onFetch = func() { startedOnce.Do(func() { close(started) }) }
	control.releaseFetch = make(chan struct{})
	control.setRange(0, 2)

	var rpcCalls atomic.Int32
	client := &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
		rpcCalls.Add(1)
		return txnVersionTestSuccessResponse(req), nil
	}}
	sender := NewRegionRequestSender(s.cache, client, oracle.NoopReadTSValidator{})

	reqCtx, cancelReq := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		bo := retry.NewBackofferWithVars(reqCtx, 100, nil)
		_, _, err := sender.SendReq(bo, txnVersionTestSharedPrewriteRequest(), region.Region, time.Second)
		errCh <- err
	}()
	<-started
	cancelReq()
	select {
	case err := <-errCh:
		s.Require().Error(err)
	case <-time.After(2 * time.Second):
		s.T().Fatal("the reload owner must stop waiting as soon as its context is canceled")
	}
	s.Zero(rpcCalls.Load(), "a canceled owner must not reach the RPC client")

	// The shared task is bound to the client lifecycle, not to the canceled
	// request, so it still completes and publishes the refreshed range.
	close(control.releaseFetch)
	s.Require().Eventually(func() bool {
		return store.getTxnProtocolVersionRange() == txnprotocol.StoreRange{Present: true, Min: 0, Max: 2}
	}, 2*time.Second, 5*time.Millisecond)
}

func (s *testRegionRequestToSingleStoreSuite) TestOnIncompatibleRequestCanceledContextDoesNotResend() {
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	s.Require().NoError(err)
	s.Require().NotNil(region)

	control := wrapStoreCacheForTxnProtocolVersionRange(s.cache, 0, 1)
	store := s.resolveStoreForTxnVersionTests()
	s.publishStoreTxnProtocolVersionRange(store, 0, 2, true)
	useTxnProtocolVersionForLocate(s.T(), kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_SHARED_LOCK)

	rpcCtx, err := s.cache.GetTiKVRPCContext(retry.NewNoopBackoff(context.Background()), region.Region, kv.ReplicaReadLeader, 0)
	s.Require().NoError(err)
	s.Require().NotNil(rpcCtx)

	reqSend := NewRegionRequestSender(s.cache, nil, oracle.NoopReadTSValidator{})
	state := &sendReqState{RegionRequestSender: reqSend}
	state.vars.txnVersion = 2
	state.vars.txnVersionSelected = true
	req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{})
	regionErr := txnVersionTestStrictRejection(2, 0, 1)
	requestCtx, cancel := context.WithCancel(context.Background())
	cancel()
	bo := retry.NewBackofferWithVars(requestCtx, 100, nil)
	fetches := control.fetchCalls()
	resend, err := state.onIncompatibleRequest(bo, rpcCtx, req, regionErr.IncompatibleRequest)

	s.Nil(resend)
	var incompatible *tikverr.ErrIncompatibleRequest
	s.Require().ErrorAs(err, &incompatible)
	s.Equal(regionErr.GetIncompatibleRequest(), incompatible.IncompatibleRequest)
	s.False(state.vars.txnVersionResendUsed)
	s.Equal(fetches, control.fetchCalls())
	s.Equal(txnprotocol.StoreRange{Present: true, Min: 0, Max: 2}, store.getTxnProtocolVersionRange())
}

func (s *testRegionRequestToSingleStoreSuite) TestOnSendFailByResourceGroupThrottled() {
	req := tikvrpc.NewRequest(tikvrpc.CmdRawPut, &kvrpcpb.RawPutRequest{
		Key:   []byte("key"),
		Value: []byte("value"),
	})
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	s.Nil(err)
	s.NotNil(region)

	// test ErrClientResourceGroupThrottled handled by regionRequestSender
	test := func() {
		oc := s.regionRequestSender.client
		defer func() {
			s.regionRequestSender.client = oc
		}()
		storeOld, _ := s.regionRequestSender.regionCache.stores.get(1)
		epoch := storeOld.epoch
		s.regionRequestSender.client = &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (response *tikvrpc.Response, err error) {
			return nil, pderr.ErrClientResourceGroupThrottled
		}}
		bo := retry.NewBackofferWithVars(context.Background(), 5, nil)
		_, _, err := s.regionRequestSender.SendReq(bo, req, region.Region, time.Second)
		s.NotNil(err)
		storeNew, _ := s.regionRequestSender.regionCache.stores.get(1)
		//  not mark the store need be refill, then the epoch should not be changed.
		s.Equal(epoch, storeNew.epoch)
		// no rpc error if the error is ErrClientResourceGroupThrottled
		s.Nil(s.regionRequestSender.rpcError)
	}

	s.Run("Default", test)

	failpoint.Enable("tikvclient/useSendReqAsync", `return(true)`)
	defer failpoint.Disable("tikvclient/useSendReqAsync")
	s.Run("AsyncAPI", test)
}

func (s *testRegionRequestToSingleStoreSuite) TestOnSendFailedWithStoreRestart() {
	s.testOnSendFailedWithStoreRestart()
}

func (s *testRegionRequestToSingleStoreSuite) TestOnSendFailedWithStoreRestartUsingAsyncAPI() {
	failpoint.Enable("tikvclient/useSendReqAsync", `return(true)`)
	defer failpoint.Disable("tikvclient/useSendReqAsync")
	s.testOnSendFailedWithStoreRestart()
}

func (s *testRegionRequestToSingleStoreSuite) testOnSendFailedWithStoreRestart() {
	req := tikvrpc.NewRequest(tikvrpc.CmdRawPut, &kvrpcpb.RawPutRequest{
		Key:   []byte("key"),
		Value: []byte("value"),
	})
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	s.Nil(err)
	s.NotNil(region)
	resp, _, err := s.regionRequestSender.SendReq(s.bo, req, region.Region, time.Second)
	s.Nil(err)
	s.NotNil(resp.Resp)
	s.Nil(s.regionRequestSender.rpcError)

	// stop store.
	s.cluster.StopStore(s.store)
	_, _, err = s.regionRequestSender.SendReq(s.bo, req, region.Region, time.Second)
	s.NotNil(err)
	// The RPC error shouldn't be nil since it failed to sent the request.
	s.NotNil(s.regionRequestSender.rpcError)

	// start store.
	s.cluster.StartStore(s.store)

	// locate region again is needed
	// since last request on the region failed and region's info had been cleared.
	region, err = s.cache.LocateRegionByID(s.bo, s.region)
	s.Nil(err)
	s.NotNil(region)
	s.NotNil(s.regionRequestSender.rpcError)
	resp, _, err = s.regionRequestSender.SendReq(s.bo, req, region.Region, time.Second)
	s.Nil(err)
	s.NotNil(resp.Resp)
}

func (s *testRegionRequestToSingleStoreSuite) TestOnSendFailedWithCloseKnownStoreThenUseNewOne() {
	s.testOnSendFailedWithCloseKnownStoreThenUseNewOne()
}

func (s *testRegionRequestToSingleStoreSuite) TestOnSendFailedWithCloseKnownStoreThenUseNewOneUsingAsyncAPI() {
	failpoint.Enable("tikvclient/useSendReqAsync", `return(true)`)
	defer failpoint.Disable("tikvclient/useSendReqAsync")
	s.testOnSendFailedWithCloseKnownStoreThenUseNewOne()
}

func (s *testRegionRequestToSingleStoreSuite) testOnSendFailedWithCloseKnownStoreThenUseNewOne() {
	req := tikvrpc.NewRequest(tikvrpc.CmdRawPut, &kvrpcpb.RawPutRequest{
		Key:   []byte("key"),
		Value: []byte("value"),
	})

	// add new store2 and make store2 as leader.
	store2 := s.cluster.AllocID()
	peer2 := s.cluster.AllocID()
	s.cluster.AddStore(store2, fmt.Sprintf("store%d", store2))
	s.cluster.AddPeer(s.region, store2, peer2)
	s.cluster.ChangeLeader(s.region, peer2)

	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	s.Nil(err)
	s.NotNil(region)
	resp, _, err := s.regionRequestSender.SendReq(s.bo, req, region.Region, time.Second)
	s.Nil(err)
	s.NotNil(resp.Resp)

	// stop store2 and make store1 as new leader.
	s.cluster.StopStore(store2)
	s.cluster.ChangeLeader(s.region, s.peer)

	// send to store2 fail and send to new leader store1.
	bo2 := retry.NewBackofferWithVars(context.Background(), 100, nil)
	resp, _, err = s.regionRequestSender.SendReq(bo2, req, region.Region, time.Second)
	s.Nil(err)
	regionErr, err := resp.GetRegionError()
	s.Nil(err)
	s.Nil(regionErr)
	s.NotNil(resp.Resp)
}

func (s *testRegionRequestToSingleStoreSuite) TestOnSendFailedWithCancelled() {
	s.testOnSendFailedWithCancelled()
}

func (s *testRegionRequestToSingleStoreSuite) TestOnSendFailedWithCancelledUsingAsyncAPI() {
	failpoint.Enable("tikvclient/useSendReqAsync", `return(true)`)
	defer failpoint.Disable("tikvclient/useSendReqAsync")
	s.testOnSendFailedWithCancelled()
}

func (s *testRegionRequestToSingleStoreSuite) testOnSendFailedWithCancelled() {
	req := tikvrpc.NewRequest(tikvrpc.CmdRawPut, &kvrpcpb.RawPutRequest{
		Key:   []byte("key"),
		Value: []byte("value"),
	})
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	s.Nil(err)
	s.NotNil(region)
	resp, _, err := s.regionRequestSender.SendReq(s.bo, req, region.Region, time.Second)
	s.Nil(err)
	s.NotNil(resp.Resp)

	// set store to cancel state.
	s.cluster.CancelStore(s.store)
	// locate region again is needed
	// since last request on the region failed and region's info had been cleared.
	_, _, err = s.regionRequestSender.SendReq(s.bo, req, region.Region, time.Second)
	s.NotNil(err)
	s.Equal(errors.Cause(err), context.Canceled)

	// set store to normal state.
	s.cluster.UnCancelStore(s.store)
	region, err = s.cache.LocateRegionByID(s.bo, s.region)
	s.Nil(err)
	s.NotNil(region)
	resp, _, err = s.regionRequestSender.SendReq(s.bo, req, region.Region, time.Second)
	s.Nil(err)
	s.NotNil(resp.Resp)
}

func (s *testRegionRequestToSingleStoreSuite) TestNoReloadRegionWhenCtxCanceled() {
	s.testNoReloadRegionWhenCtxCanceled()
}

func (s *testRegionRequestToSingleStoreSuite) TestNoReloadRegionWhenCtxCanceledUsingAsyncAPI() {
	failpoint.Enable("tikvclient/useSendReqAsync", `return(true)`)
	defer failpoint.Disable("tikvclient/useSendReqAsync")
	s.testNoReloadRegionWhenCtxCanceled()
}

func (s *testRegionRequestToSingleStoreSuite) testNoReloadRegionWhenCtxCanceled() {
	req := tikvrpc.NewRequest(tikvrpc.CmdRawPut, &kvrpcpb.RawPutRequest{
		Key:   []byte("key"),
		Value: []byte("value"),
	})
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	s.Nil(err)
	s.NotNil(region)

	sender := s.regionRequestSender
	bo, cancel := s.bo.Fork()
	cancel()
	// Call SendKVReq with a canceled context.
	_, _, err = sender.SendReq(bo, req, region.Region, time.Second)
	// Check this kind of error won't cause region cache drop.
	s.Equal(errors.Cause(err), context.Canceled)
	r, expired := sender.regionCache.searchCachedRegionByID(s.region)
	s.False(expired)
	s.NotNil(r)
}

func (s *testRegionRequestToSingleStoreSuite) TestSendReqCtx() {
	req := tikvrpc.NewRequest(tikvrpc.CmdRawPut, &kvrpcpb.RawPutRequest{
		Key:   []byte("key"),
		Value: []byte("value"),
	})
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	s.Nil(err)
	s.NotNil(region)
	resp, ctx, _, err := s.regionRequestSender.SendReqCtx(s.bo, req, region.Region, time.Second, tikvrpc.TiKV)
	s.Nil(err)
	s.NotNil(resp.Resp)
	s.NotNil(ctx)
	req.ReplicaRead = true
	resp, ctx, _, err = s.regionRequestSender.SendReqCtx(s.bo, req, region.Region, time.Second, tikvrpc.TiKV)
	s.Nil(err)
	s.NotNil(resp.Resp)
	s.NotNil(ctx)
}

func (s *testRegionRequestToSingleStoreSuite) TestRequestAttemptLimiter() {
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	s.Require().NoError(err)
	s.Require().NotNil(region)

	s.Run("Sync", func() {
		req := tikvrpc.NewRequest(tikvrpc.CmdRawPut, &kvrpcpb.RawPutRequest{
			Key:   []byte("key"),
			Value: []byte("value"),
		})
		var acquiredStoreID uint64
		var releaseCount atomic.Int32
		req.RequestAttemptLimiter = func(ctx context.Context, storeID uint64) (func(), error) {
			acquiredStoreID = storeID
			return func() { releaseCount.Add(1) }, nil
		}

		resp, _, _, err := s.regionRequestSender.SendReqCtx(s.bo, req, region.Region, time.Second, tikvrpc.TiKV)
		s.Require().NoError(err)
		s.Require().NotNil(resp)
		s.Equal(s.store, acquiredStoreID)
		s.Equal(int32(1), releaseCount.Load())
	})

	s.Run("Async", func() {
		req := tikvrpc.NewRequest(tikvrpc.CmdRawPut, &kvrpcpb.RawPutRequest{
			Key:   []byte("key"),
			Value: []byte("value"),
		})
		var acquiredStoreID uint64
		var releaseCount atomic.Int32
		req.RequestAttemptLimiter = func(ctx context.Context, storeID uint64) (func(), error) {
			acquiredStoreID = storeID
			return func() { releaseCount.Add(1) }, nil
		}

		complete := false
		rl := async.NewRunLoop()
		s.regionRequestSender.SendReqAsync(s.bo, req, region.Region, time.Second, async.NewCallback(rl, func(resp *tikvrpc.ResponseExt, err error) {
			s.Require().NoError(err)
			s.Require().NotNil(resp)
			complete = true
		}))
		for !complete {
			_, err := rl.Exec(context.Background())
			s.Require().NoError(err)
		}
		s.Equal(s.store, acquiredStoreID)
		s.Equal(int32(1), releaseCount.Load())
	})

	s.Run("AsyncLimiterSchedulesSynchronousClientCallback", func() {
		originalClient := s.regionRequestSender.client
		clientFinished := make(chan struct{})
		s.regionRequestSender.client = &immediateAsyncClient{
			fnClient: &fnClient{fn: func(context.Context, string, *tikvrpc.Request, time.Duration) (*tikvrpc.Response, error) {
				return &tikvrpc.Response{Resp: &kvrpcpb.RawPutResponse{}}, nil
			}},
			finished: clientFinished,
		}
		defer func() { s.regionRequestSender.client = originalClient }()

		req := tikvrpc.NewRequest(tikvrpc.CmdRawPut, &kvrpcpb.RawPutRequest{
			Key:   []byte("key"),
			Value: []byte("value"),
		})
		req.RequestAttemptLimiter = func(context.Context, uint64) (func(), error) {
			return func() {}, nil
		}

		complete := false
		rl := async.NewRunLoop()
		s.regionRequestSender.SendReqAsync(s.bo, req, region.Region, time.Second, async.NewCallback(rl, func(resp *tikvrpc.ResponseExt, err error) {
			s.Require().NoError(err)
			s.Require().NotNil(resp)
			complete = true
		}))

		select {
		case <-clientFinished:
		case <-time.After(time.Second):
			s.FailNow("synchronous async callback was not invoked")
		}
		s.False(complete, "callback must be scheduled on the run loop")

		for !complete {
			_, err := rl.Exec(context.Background())
			s.Require().NoError(err)
		}
	})

	s.Run("AsyncLimiterWaitExcludedFromRPCStats", func() {
		synctest.Test(s.T(), func(t *testing.T) {
			req := tikvrpc.NewRequest(tikvrpc.CmdRawPut, &kvrpcpb.RawPutRequest{
				Key:   []byte("key"),
				Value: []byte("value"),
			})
			limiterStarted := make(chan struct{})
			admit := make(chan struct{})
			req.RequestAttemptLimiter = func(context.Context, uint64) (func(), error) {
				close(limiterStarted)
				<-admit
				return func() {}, nil
			}

			stats := NewRegionRequestRuntimeStats()
			s.regionRequestSender.Stats = stats
			defer func() { s.regionRequestSender.Stats = nil }()

			complete := false
			rl := async.NewRunLoop()
			s.regionRequestSender.SendReqAsync(s.bo, req, region.Region, time.Second, async.NewCallback(rl, func(resp *tikvrpc.ResponseExt, err error) {
				require.NoError(t, err)
				require.NotNil(t, resp)
				complete = true
			}))

			<-limiterStarted
			const limiterWait = time.Hour
			time.Sleep(limiterWait)
			close(admit)

			runCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			for !complete {
				_, err := rl.Exec(runCtx)
				require.NoError(t, err)
			}

			require.Len(t, stats.RPCStatsList, 1)
			require.Equal(t, tikvrpc.CmdRawPut, stats.RPCStatsList[0].Cmd)
			require.Equal(t, uint32(1), stats.RPCStatsList[0].Count)
			require.Less(t, stats.RPCStatsList[0].Consume, limiterWait,
				"RPC runtime must not include request-attempt limiter wait")
		})
	})

	s.Run("AsyncLimiterCanceled", func() {
		req := tikvrpc.NewRequest(tikvrpc.CmdRawPut, &kvrpcpb.RawPutRequest{
			Key:   []byte("key"),
			Value: []byte("value"),
		})
		limiterStarted := make(chan struct{})
		req.RequestAttemptLimiter = func(ctx context.Context, _ uint64) (func(), error) {
			close(limiterStarted)
			<-ctx.Done()
			return nil, ctx.Err()
		}

		originalClient := s.regionRequestSender.client
		var sendCount atomic.Int32
		s.regionRequestSender.client = &fnClient{fn: func(context.Context, string, *tikvrpc.Request, time.Duration) (*tikvrpc.Response, error) {
			sendCount.Add(1)
			return &tikvrpc.Response{Resp: &kvrpcpb.RawPutResponse{}}, nil
		}}
		defer func() { s.regionRequestSender.client = originalClient }()

		bo, cancelRequest := s.bo.Fork()
		defer cancelRequest()

		complete := false
		var sendErr error
		rl := async.NewRunLoop()
		s.regionRequestSender.SendReqAsync(bo, req, region.Region, time.Second, async.NewCallback(rl, func(resp *tikvrpc.ResponseExt, err error) {
			s.Nil(resp)
			sendErr = err
			complete = true
		}))

		<-limiterStarted
		cancelRequest()

		runCtx, cancelRun := context.WithTimeout(context.Background(), time.Second)
		defer cancelRun()
		for !complete {
			_, err := rl.Exec(runCtx)
			s.Require().NoError(err)
		}
		s.ErrorIs(sendErr, context.Canceled)
		s.Zero(sendCount.Load())
	})

	s.Run("AsyncLimiterError", func() {
		req := tikvrpc.NewRequest(tikvrpc.CmdRawPut, &kvrpcpb.RawPutRequest{
			Key:   []byte("key"),
			Value: []byte("value"),
		})
		req.RequestAttemptLimiter = func(context.Context, uint64) (func(), error) {
			return nil, errors.New("async limiter rejected")
		}

		complete := false
		rl := async.NewRunLoop()
		s.regionRequestSender.SendReqAsync(s.bo, req, region.Region, time.Second, async.NewCallback(rl, func(resp *tikvrpc.ResponseExt, err error) {
			s.Nil(resp)
			s.EqualError(err, "async limiter rejected")
			complete = true
		}))
		for !complete {
			_, err := rl.Exec(context.Background())
			s.Require().NoError(err)
		}
	})

	s.Run("LimiterError", func() {
		req := tikvrpc.NewRequest(tikvrpc.CmdRawPut, &kvrpcpb.RawPutRequest{
			Key:   []byte("key"),
			Value: []byte("value"),
		})
		var releaseCount atomic.Int32
		req.RequestAttemptLimiter = func(context.Context, uint64) (func(), error) {
			return func() { releaseCount.Add(1) }, errors.New("limiter rejected")
		}

		resp, _, _, err := s.regionRequestSender.SendReqCtx(s.bo, req, region.Region, time.Second, tikvrpc.TiKV)
		s.Nil(resp)
		s.EqualError(err, "limiter rejected")
		s.Equal(int32(1), releaseCount.Load())
	})

	s.Run("Canceled", func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		bo := retry.NewNoopBackoff(ctx)
		req := tikvrpc.NewRequest(tikvrpc.CmdRawPut, &kvrpcpb.RawPutRequest{
			Key:   []byte("key"),
			Value: []byte("value"),
		})
		req.RequestAttemptLimiter = func(ctx context.Context, _ uint64) (func(), error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}

		resp, _, _, err := s.regionRequestSender.SendReqCtx(bo, req, region.Region, time.Second, tikvrpc.TiKV)
		s.Nil(resp)
		s.ErrorIs(err, context.Canceled)
	})

	s.Run("StoreLimitErrorReleasesAttempt", func() {
		req := tikvrpc.NewRequest(tikvrpc.CmdRawPut, &kvrpcpb.RawPutRequest{
			Key:   []byte("key"),
			Value: []byte("value"),
		})
		var releaseCount atomic.Int32
		req.RequestAttemptLimiter = func(context.Context, uint64) (func(), error) {
			return func() { releaseCount.Add(1) }, nil
		}

		store := s.cache.stores.getOrInsertDefault(s.store)
		defer func(storeLimit int64, tokenCount int64) {
			kv.StoreLimit.Store(storeLimit)
			store.tokenCount.Store(tokenCount)
		}(kv.StoreLimit.Load(), store.tokenCount.Load())
		kv.StoreLimit.Store(1)
		store.tokenCount.Store(1)

		resp, _, _, err := s.regionRequestSender.SendReqCtx(s.bo, req, region.Region, time.Second, tikvrpc.TiKV)
		s.Nil(resp)
		s.Error(err)
		s.Equal(int32(1), releaseCount.Load())
	})
}

func (s *testRegionRequestToSingleStoreSuite) TestSendReqAsync() {
	reachable.injectConstantLiveness(s.regionRequestSender.regionCache.stores)

	ctx := context.Background()
	rl := async.NewRunLoop()

	s.Run("Basic", func() {
		req := tikvrpc.NewRequest(tikvrpc.CmdRawPut, &kvrpcpb.RawPutRequest{
			Key:   []byte("key"),
			Value: []byte("value"),
		})
		region, err := s.cache.LocateRegionByID(s.bo, s.region)
		s.Nil(err)
		s.NotNil(region)

		complete := false
		s.regionRequestSender.SendReqAsync(s.bo, req, region.Region, time.Second, async.NewCallback(rl, func(resp *tikvrpc.ResponseExt, err error) {
			s.Nil(err)
			s.NotNil(resp.Resp)
			s.NotEmpty(resp.Addr)
			complete = true
		}))
		for !complete {
			_, err := rl.Exec(ctx)
			s.Require().NoError(err)
		}
	})

	s.Run("StoreLimit", func() {
		req := tikvrpc.NewRequest(tikvrpc.CmdRawPut, &kvrpcpb.RawPutRequest{
			Key:   []byte("key"),
			Value: []byte("value"),
		})
		var releaseCount atomic.Int32
		req.RequestAttemptLimiter = func(context.Context, uint64) (func(), error) {
			return func() { releaseCount.Add(1) }, nil
		}
		region, err := s.cache.LocateRegionByID(s.bo, s.region)
		s.Nil(err)
		s.NotNil(region)

		store := s.cache.stores.getOrInsertDefault(s.store)

		defer func(storeLimit int64, tokenCount int64) {
			kv.StoreLimit.Store(storeLimit)
			store.tokenCount.Store(tokenCount)
		}(kv.StoreLimit.Load(), store.tokenCount.Load())
		kv.StoreLimit.Store(100)
		store.tokenCount.Store(100)

		complete := false
		s.regionRequestSender.SendReqAsync(s.bo, req, region.Region, time.Second, async.NewCallback(rl, func(resp *tikvrpc.ResponseExt, err error) {
			s.Nil(resp)
			s.NotNil(err)
			e, ok := errors.Cause(err).(*tikverr.ErrTokenLimit)
			s.True(ok)
			s.Equal(s.store, e.StoreID)
			complete = true
		}))
		for !complete {
			_, err := rl.Exec(ctx)
			s.Require().NoError(err)
		}
		s.Equal(int32(1), releaseCount.Load())
	})

	s.Run("RPCCancel", func() {
		req := tikvrpc.NewRequest(tikvrpc.CmdRawPut, &kvrpcpb.RawPutRequest{
			Key:   []byte("key"),
			Value: []byte("value"),
		})
		var releaseCount atomic.Int32
		req.RequestAttemptLimiter = func(context.Context, uint64) (func(), error) {
			return func() { releaseCount.Add(1) }, nil
		}
		region, err := s.cache.LocateRegionByID(s.bo, s.region)
		s.Nil(err)
		s.NotNil(region)

		defer func(ctx context.Context, cli client.Client) {
			s.bo.SetCtx(ctx)
			s.regionRequestSender.client = cli
		}(s.bo.GetCtx(), s.regionRequestSender.client)

		var once sync.Once
		rpcCanceller := NewRPCanceller()
		s.bo.SetCtx(context.WithValue(s.bo.GetCtx(), RPCCancellerCtxKey{}, rpcCanceller))
		s.regionRequestSender.client = &fnClient{
			fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
				once.Do(func() { rpcCanceller.CancelAll() })
				return nil, context.Canceled
			},
		}

		complete := false
		s.regionRequestSender.SendReqAsync(s.bo, req, region.Region, time.Second, async.NewCallback(rl, func(resp *tikvrpc.ResponseExt, err error) {
			s.Nil(resp)
			s.ErrorIs(err, context.Canceled)
			complete = true
		}))
		for !complete {
			_, err := rl.Exec(ctx)
			s.Require().NoError(err)
		}
		s.Equal(int32(1), releaseCount.Load())
	})

	s.Run("Timeout", func() {
		req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{
			Key:     []byte("key"),
			Version: math.MaxUint64,
		})
		region, err := s.cache.LocateRegionByID(s.bo, s.region)
		s.Nil(err)
		s.NotNil(region)

		defer func(cli client.Client) {
			s.regionRequestSender.client = cli
		}(s.regionRequestSender.client)

		s.regionRequestSender.client = &fnClient{
			fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
				<-ctx.Done()
				return nil, ctx.Err()
			},
		}

		complete := false
		s.regionRequestSender.SendReqAsync(s.bo, req, region.Region, 100*time.Millisecond, async.NewCallback(rl, func(resp *tikvrpc.ResponseExt, err error) {
			s.Nil(err)
			s.NotNil(resp)
			regionErr, err := resp.GetRegionError()
			s.Nil(err)
			s.True(retry.IsFakeRegionError(regionErr))
			complete = true
		}))
		for !complete {
			_, err := rl.Exec(ctx)
			s.Require().NoError(err)
		}
	})
}

// cancelContextClient wraps rpcClient and always cancels context before sending requests.
type cancelContextClient struct {
	client.Client
	redirectAddr string
}

func (c *cancelContextClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	childCtx, cancel := context.WithCancel(ctx)
	cancel()
	return c.Client.SendRequest(childCtx, c.redirectAddr, req, timeout)
}

// mockTikvGrpcServer mock a tikv gprc server for testing.
type mockTikvGrpcServer struct{}

var _ tikvpb.TikvServer = &mockTikvGrpcServer{}

// KvGet commands with mvcc/txn supported.
func (s *mockTikvGrpcServer) KvGet(context.Context, *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) KvScan(context.Context, *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) KvPrewrite(context.Context, *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) KvCommit(context.Context, *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) KvImport(context.Context, *kvrpcpb.ImportRequest) (*kvrpcpb.ImportResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) KvCleanup(context.Context, *kvrpcpb.CleanupRequest) (*kvrpcpb.CleanupResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) KvBatchGet(context.Context, *kvrpcpb.BatchGetRequest) (*kvrpcpb.BatchGetResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) KvBatchRollback(context.Context, *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) KvScanLock(context.Context, *kvrpcpb.ScanLockRequest) (*kvrpcpb.ScanLockResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) KvResolveLock(context.Context, *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) KvPessimisticLock(context.Context, *kvrpcpb.PessimisticLockRequest) (*kvrpcpb.PessimisticLockResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) KVPessimisticRollback(context.Context, *kvrpcpb.PessimisticRollbackRequest) (*kvrpcpb.PessimisticRollbackResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) KvCheckTxnStatus(ctx context.Context, in *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) KvCheckSecondaryLocks(ctx context.Context, in *kvrpcpb.CheckSecondaryLocksRequest) (*kvrpcpb.CheckSecondaryLocksResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) KvTxnHeartBeat(ctx context.Context, in *kvrpcpb.TxnHeartBeatRequest) (*kvrpcpb.TxnHeartBeatResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) KvGC(context.Context, *kvrpcpb.GCRequest) (*kvrpcpb.GCResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) KvDeleteRange(context.Context, *kvrpcpb.DeleteRangeRequest) (*kvrpcpb.DeleteRangeResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) RawGet(context.Context, *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) RawBatchGet(context.Context, *kvrpcpb.RawBatchGetRequest) (*kvrpcpb.RawBatchGetResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) RawPut(context.Context, *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) RawBatchPut(context.Context, *kvrpcpb.RawBatchPutRequest) (*kvrpcpb.RawBatchPutResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) RawDelete(context.Context, *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) RawBatchDelete(context.Context, *kvrpcpb.RawBatchDeleteRequest) (*kvrpcpb.RawBatchDeleteResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) RawScan(context.Context, *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) RawDeleteRange(context.Context, *kvrpcpb.RawDeleteRangeRequest) (*kvrpcpb.RawDeleteRangeResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) RawBatchScan(context.Context, *kvrpcpb.RawBatchScanRequest) (*kvrpcpb.RawBatchScanResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) RawGetKeyTTL(context.Context, *kvrpcpb.RawGetKeyTTLRequest) (*kvrpcpb.RawGetKeyTTLResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) UnsafeDestroyRange(context.Context, *kvrpcpb.UnsafeDestroyRangeRequest) (*kvrpcpb.UnsafeDestroyRangeResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) RegisterLockObserver(context.Context, *kvrpcpb.RegisterLockObserverRequest) (*kvrpcpb.RegisterLockObserverResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) CheckLockObserver(context.Context, *kvrpcpb.CheckLockObserverRequest) (*kvrpcpb.CheckLockObserverResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) RemoveLockObserver(context.Context, *kvrpcpb.RemoveLockObserverRequest) (*kvrpcpb.RemoveLockObserverResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) PhysicalScanLock(context.Context, *kvrpcpb.PhysicalScanLockRequest) (*kvrpcpb.PhysicalScanLockResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) Coprocessor(context.Context, *coprocessor.Request) (*coprocessor.Response, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) BatchCoprocessor(*coprocessor.BatchRequest, tikvpb.Tikv_BatchCoprocessorServer) error {
	return errors.New("unreachable")
}

func (s *mockTikvGrpcServer) DelegateCoprocessor(context.Context, *coprocessor.DelegateRequest) (*coprocessor.DelegateResponse, error) {
	return nil, errors.New("unreachable")
}

func (s *mockTikvGrpcServer) RawCoprocessor(context.Context, *kvrpcpb.RawCoprocessorRequest) (*kvrpcpb.RawCoprocessorResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) DispatchMPPTask(context.Context, *mpp.DispatchTaskRequest) (*mpp.DispatchTaskResponse, error) {
	return nil, errors.New("unreachable")
}

func (s *mockTikvGrpcServer) IsAlive(context.Context, *mpp.IsAliveRequest) (*mpp.IsAliveResponse, error) {
	return nil, errors.New("unreachable")
}

func (s *mockTikvGrpcServer) ReportMPPTaskStatus(context.Context, *mpp.ReportTaskStatusRequest) (*mpp.ReportTaskStatusResponse, error) {
	return nil, errors.New("unreachable")
}

func (s *mockTikvGrpcServer) EstablishMPPConnection(*mpp.EstablishMPPConnectionRequest, tikvpb.Tikv_EstablishMPPConnectionServer) error {
	return errors.New("unreachable")
}
func (s *mockTikvGrpcServer) CancelMPPTask(context.Context, *mpp.CancelTaskRequest) (*mpp.CancelTaskResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) Raft(tikvpb.Tikv_RaftServer) error {
	return errors.New("unreachable")
}
func (s *mockTikvGrpcServer) BatchRaft(tikvpb.Tikv_BatchRaftServer) error {
	return errors.New("unreachable")
}
func (s *mockTikvGrpcServer) Snapshot(tikvpb.Tikv_SnapshotServer) error {
	return errors.New("unreachable")
}
func (s *mockTikvGrpcServer) MvccGetByKey(context.Context, *kvrpcpb.MvccGetByKeyRequest) (*kvrpcpb.MvccGetByKeyResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) MvccGetByStartTs(context.Context, *kvrpcpb.MvccGetByStartTsRequest) (*kvrpcpb.MvccGetByStartTsResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockTikvGrpcServer) SplitRegion(context.Context, *kvrpcpb.SplitRegionRequest) (*kvrpcpb.SplitRegionResponse, error) {
	return nil, errors.New("unreachable")
}

func (s *mockTikvGrpcServer) CoprocessorStream(*coprocessor.Request, tikvpb.Tikv_CoprocessorStreamServer) error {
	return errors.New("unreachable")
}

func (s *mockTikvGrpcServer) BatchCommands(tikvpb.Tikv_BatchCommandsServer) error {
	return errors.New("unreachable")
}

func (s *mockTikvGrpcServer) ReadIndex(context.Context, *kvrpcpb.ReadIndexRequest) (*kvrpcpb.ReadIndexResponse, error) {
	return nil, errors.New("unreachable")
}

func (s *mockTikvGrpcServer) CheckLeader(context.Context, *kvrpcpb.CheckLeaderRequest) (*kvrpcpb.CheckLeaderResponse, error) {
	return nil, errors.New("unreachable")
}

func (s *mockTikvGrpcServer) GetStoreSafeTS(context.Context, *kvrpcpb.StoreSafeTSRequest) (*kvrpcpb.StoreSafeTSResponse, error) {
	return nil, errors.New("unreachable")
}

func (s *mockTikvGrpcServer) RawCompareAndSwap(context.Context, *kvrpcpb.RawCASRequest) (*kvrpcpb.RawCASResponse, error) {
	return nil, errors.New("unreachable")
}

func (s *mockTikvGrpcServer) GetLockWaitInfo(context.Context, *kvrpcpb.GetLockWaitInfoRequest) (*kvrpcpb.GetLockWaitInfoResponse, error) {
	return nil, errors.New("unreachable")
}

func (s *mockTikvGrpcServer) RawChecksum(context.Context, *kvrpcpb.RawChecksumRequest) (*kvrpcpb.RawChecksumResponse, error) {
	return nil, errors.New("unreachable")
}

func (s *mockTikvGrpcServer) Compact(ctx context.Context, request *kvrpcpb.CompactRequest) (*kvrpcpb.CompactResponse, error) {
	return nil, errors.New("unreachable")
}

func (s *mockTikvGrpcServer) GetLockWaitHistory(ctx context.Context, request *kvrpcpb.GetLockWaitHistoryRequest) (*kvrpcpb.GetLockWaitHistoryResponse, error) {
	return nil, errors.New("unreachable")
}

func (s *mockTikvGrpcServer) TryAddLock(context.Context, *disaggregated.TryAddLockRequest) (*disaggregated.TryAddLockResponse, error) {
	return nil, errors.New("unreachable")
}

func (s *mockTikvGrpcServer) TryMarkDelete(context.Context, *disaggregated.TryMarkDeleteRequest) (*disaggregated.TryMarkDeleteResponse, error) {
	return nil, errors.New("unreachable")
}

func (s *mockTikvGrpcServer) KvFlashbackToVersion(context.Context, *kvrpcpb.FlashbackToVersionRequest) (*kvrpcpb.FlashbackToVersionResponse, error) {
	return nil, errors.New("unreachable")
}

func (s *mockTikvGrpcServer) KvPrepareFlashbackToVersion(context.Context, *kvrpcpb.PrepareFlashbackToVersionRequest) (*kvrpcpb.PrepareFlashbackToVersionResponse, error) {
	return nil, errors.New("unreachable")
}

func (s *mockTikvGrpcServer) EstablishDisaggTask(context.Context, *disaggregated.EstablishDisaggTaskRequest) (*disaggregated.EstablishDisaggTaskResponse, error) {
	return nil, errors.New("unreachable")
}

func (s *mockTikvGrpcServer) FetchDisaggPages(*disaggregated.FetchDisaggPagesRequest, tikvpb.Tikv_FetchDisaggPagesServer) error {
	return errors.New("unreachable")
}

func (s *mockTikvGrpcServer) TabletSnapshot(_ tikvpb.Tikv_TabletSnapshotServer) error {
	return errors.New("unreachable")
}

func (s *mockTikvGrpcServer) GetTiFlashSystemTable(context.Context, *kvrpcpb.TiFlashSystemTableRequest) (*kvrpcpb.TiFlashSystemTableResponse, error) {
	return nil, errors.New("unreachable")
}

func (s *mockTikvGrpcServer) GetEstimateTiCICount(context.Context, *coprocessor.TiCIEstimateCountRequest) (*coprocessor.TiCIEstimateCountResponse, error) {
	return nil, errors.New("unreachable")
}

func (s *mockTikvGrpcServer) GetDisaggConfig(context.Context, *disaggregated.GetDisaggConfigRequest) (*disaggregated.GetDisaggConfigResponse, error) {
	return nil, errors.New("unreachable")
}

func (s *mockTikvGrpcServer) CancelDisaggTask(context.Context, *disaggregated.CancelDisaggTaskRequest) (*disaggregated.CancelDisaggTaskResponse, error) {
	return nil, errors.New("unreachable")
}

func (s *mockTikvGrpcServer) KvFlush(context.Context, *kvrpcpb.FlushRequest) (*kvrpcpb.FlushResponse, error) {
	return nil, errors.New("unreachable")
}

func (s *mockTikvGrpcServer) KvBufferBatchGet(context.Context, *kvrpcpb.BufferBatchGetRequest) (*kvrpcpb.BufferBatchGetResponse, error) {
	return nil, errors.New("unreachable")
}

func (s *mockTikvGrpcServer) GetHealthFeedback(ctx context.Context, request *kvrpcpb.GetHealthFeedbackRequest) (*kvrpcpb.GetHealthFeedbackResponse, error) {
	return nil, errors.New("unreachable")
}

func (s *mockTikvGrpcServer) BroadcastTxnStatus(ctx context.Context, request *kvrpcpb.BroadcastTxnStatusRequest) (*kvrpcpb.BroadcastTxnStatusResponse, error) {
	return nil, errors.New("unreachable")
}

func (s *testRegionRequestToSingleStoreSuite) TestNoReloadRegionForGrpcWhenCtxCanceled() {
	// prepare a mock tikv grpc server
	addr := "localhost:56341"
	lc := net.ListenConfig{}
	lis, err := lc.Listen(context.Background(), "tcp", addr)
	s.Nil(err)
	server := grpc.NewServer()
	tikvpb.RegisterTikvServer(server, &mockTikvGrpcServer{})
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		server.Serve(lis)
		wg.Done()
	}()

	cli := client.NewRPCClient()
	sender := NewRegionRequestSender(s.cache, cli, oracle.NoopReadTSValidator{})
	req := tikvrpc.NewRequest(tikvrpc.CmdRawPut, &kvrpcpb.RawPutRequest{
		Key:   []byte("key"),
		Value: []byte("value"),
	})
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	s.Nil(err)

	bo, cancel := s.bo.Fork()
	cancel()
	_, _, err = sender.SendReq(bo, req, region.Region, 3*time.Second)
	s.Equal(errors.Cause(err), context.Canceled)
	r, expired := sender.regionCache.searchCachedRegionByID(s.region)
	s.False(expired)
	s.NotNil(r)

	// Just for covering error code = codes.Canceled.
	client1 := &cancelContextClient{
		Client:       client.NewRPCClient(),
		redirectAddr: addr,
	}
	sender = NewRegionRequestSender(s.cache, client1, oracle.NoopReadTSValidator{})
	sender.SendReq(s.bo, req, region.Region, 3*time.Second)

	// cleanup
	server.Stop()
	wg.Wait()
	cli.Close()
	client1.Close()
}

func (s *testRegionRequestToSingleStoreSuite) TestOnMaxTimestampNotSyncedError() {
	req := tikvrpc.NewRequest(tikvrpc.CmdPrewrite, &kvrpcpb.PrewriteRequest{})
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	s.Nil(err)
	s.NotNil(region)

	// test retry for max timestamp not synced
	func() {
		oc := s.regionRequestSender.client
		defer func() {
			s.regionRequestSender.client = oc
		}()
		count := 0
		s.regionRequestSender.client = &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (response *tikvrpc.Response, err error) {
			count++
			var resp *tikvrpc.Response
			if count < 3 {
				resp = &tikvrpc.Response{Resp: &kvrpcpb.PrewriteResponse{
					RegionError: &errorpb.Error{MaxTimestampNotSynced: &errorpb.MaxTimestampNotSynced{}},
				}}
			} else {
				resp = &tikvrpc.Response{Resp: &kvrpcpb.PrewriteResponse{}}
			}
			return resp, nil
		}}
		bo := retry.NewBackofferWithVars(context.Background(), 5, nil)
		resp, _, err := s.regionRequestSender.SendReq(bo, req, region.Region, time.Second)
		s.Nil(err)
		s.NotNil(resp)
	}()
}

func (s *testRegionRequestToSingleStoreSuite) TestGetRegionByIDFromCache() {
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	s.Nil(err)
	s.NotNil(region)

	// test kv epochNotMatch return empty regions
	s.cache.OnRegionEpochNotMatch(s.bo, &RPCContext{Region: region.Region, Store: &Store{storeID: s.store}}, []*metapb.Region{})
	s.Nil(err)
	r, expired := s.cache.searchCachedRegionByID(s.region)
	s.True(expired)
	s.NotNil(r)

	// refill cache
	region, err = s.cache.LocateRegionByID(s.bo, s.region)
	s.Nil(err)
	s.NotNil(region)

	// test kv load new region with new start-key and new epoch
	v2 := region.Region.confVer + 1
	r2 := metapb.Region{Id: region.Region.id, RegionEpoch: &metapb.RegionEpoch{Version: region.Region.ver, ConfVer: v2}, StartKey: []byte{1}}
	st := newUninitializedStore(s.store)
	s.cache.insertRegionToCache(&Region{meta: &r2, store: unsafe.Pointer(st), ttl: nextTTLWithoutJitter(time.Now().Unix())}, true, true)
	region, err = s.cache.LocateRegionByID(s.bo, s.region)
	s.Nil(err)
	s.NotNil(region)
	s.Equal(region.Region.confVer, v2)
	s.Equal(region.Region.ver, region.Region.ver)

	v3 := region.Region.confVer + 1
	r3 := metapb.Region{Id: region.Region.id, RegionEpoch: &metapb.RegionEpoch{Version: v3, ConfVer: region.Region.confVer}, StartKey: []byte{2}}
	st = newUninitializedStore(s.store)
	s.cache.insertRegionToCache(&Region{meta: &r3, store: unsafe.Pointer(st), ttl: nextTTLWithoutJitter(time.Now().Unix())}, true, true)
	region, err = s.cache.LocateRegionByID(s.bo, s.region)
	s.Nil(err)
	s.NotNil(region)
	s.Equal(region.Region.confVer, region.Region.confVer)
	s.Equal(region.Region.ver, v3)
}

func (s *testRegionRequestToSingleStoreSuite) TestCloseConnectionOnStoreNotMatch() {
	req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{
		Key: []byte("key"),
	})
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	s.Nil(err)
	s.NotNil(region)

	oc := s.regionRequestSender.client
	defer func() {
		s.regionRequestSender.client = oc
	}()

	var target string
	client := &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (response *tikvrpc.Response, err error) {
		target = addr
		resp := &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{
			RegionError: &errorpb.Error{StoreNotMatch: &errorpb.StoreNotMatch{}},
		}}
		return resp, nil
	}}

	s.regionRequestSender.client = client
	bo := retry.NewBackofferWithVars(context.Background(), 5, nil)
	resp, _, err := s.regionRequestSender.SendReq(bo, req, region.Region, time.Second)
	s.Nil(err)
	s.NotNil(resp)
	regionErr, _ := resp.GetRegionError()
	s.NotNil(regionErr)
	s.Equal(target, client.closedAddr)
	var expected uint64 = math.MaxUint64
	s.Equal(expected, client.closedVer)
}

func (s *testRegionRequestToSingleStoreSuite) TestKVReadTimeoutWithDisableBatchClient() {
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.MaxBatchSize = 0
	})()

	server, port := mockserver.StartMockTikvService()
	s.True(port > 0)
	server.SetMetaChecker(func(ctx context.Context) error {
		return context.DeadlineExceeded
	})
	rpcClient := client.NewRPCClient()
	s.regionRequestSender.client = &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (response *tikvrpc.Response, err error) {
		return rpcClient.SendRequest(ctx, server.Addr(), req, timeout)
	}}
	defer func() {
		rpcClient.Close()
		server.Stop()
	}()

	bo := retry.NewBackofferWithVars(context.Background(), 2000, nil)
	region, err := s.cache.LocateRegionByID(bo, s.region)
	s.Nil(err)
	s.NotNil(region)
	req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("a"), Version: 1})
	// send a probe request to make sure the mock server is ready.
	s.regionRequestSender.SendReq(retry.NewNoopBackoff(context.Background()), req, region.Region, time.Second)
	resp, _, err := s.regionRequestSender.SendReq(bo, req, region.Region, time.Millisecond*10)
	s.Nil(err)
	s.NotNil(resp)
	regionErr, _ := resp.GetRegionError()
	s.True(retry.IsFakeRegionError(regionErr))
	s.Equal(0, bo.GetTotalBackoffTimes()) // use kv read timeout will do fast retry, so backoff times should be 0.
}

func (s *testRegionRequestToSingleStoreSuite) TestBatchClientSendLoopPanic() {
	// This test should use `go test -race` to run.
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.MaxBatchSize = 128
	})()

	server, port := mockserver.StartMockTikvService()
	s.True(port > 0)
	rpcClient := client.NewRPCClient()
	fnClient := &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (response *tikvrpc.Response, err error) {
		return rpcClient.SendRequest(ctx, server.Addr(), req, timeout)
	}}

	defer func() {
		rpcClient.Close()
		server.Stop()
	}()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				ctx, cancel := context.WithCancel(context.Background())
				bo := retry.NewBackofferWithVars(ctx, int(client.ReadTimeoutShort.Milliseconds()), nil)
				region, err := s.cache.LocateRegionByID(bo, s.region)
				s.Nil(err)
				s.NotNil(region)
				go func() {
					// mock for kill query execution or timeout.
					time.Sleep(time.Millisecond * time.Duration(rand.Intn(5)+1))
					cancel()
				}()
				req := tikvrpc.NewRequest(tikvrpc.CmdCop, &coprocessor.Request{Data: []byte("a"), StartTs: 1})
				regionRequestSender := NewRegionRequestSender(s.cache, fnClient, oracle.NoopReadTSValidator{})
				reachable.injectConstantLiveness(regionRequestSender.regionCache.stores)
				regionRequestSender.SendReq(bo, req, region.Region, client.ReadTimeoutShort)
			}
		}()
	}
	wg.Wait()
	// batchSendLoop should not panic.
	s.Equal(atomic.LoadInt64(&client.BatchSendLoopPanicCounter), int64(0))
}

func (s *testRegionRequestToSingleStoreSuite) TestClusterIDInReq() {
	server, port := mockserver.StartMockTikvService()
	s.True(port > 0)
	rpcClient := client.NewRPCClient()
	s.regionRequestSender.client = &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (response *tikvrpc.Response, err error) {
		s.Greater(req.ClusterId, uint64(0))
		return rpcClient.SendRequest(ctx, server.Addr(), req, timeout)
	}}
	defer func() {
		rpcClient.Close()
		server.Stop()
	}()

	bo := retry.NewBackofferWithVars(context.Background(), 2000, nil)
	region, err := s.cache.LocateRegionByID(bo, s.region)
	s.Nil(err)
	s.NotNil(region)
	req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("a"), Version: 1})
	// Wait for the gRPC serve loop to start accepting connections before running assertions.
	s.Eventually(func() bool {
		_, probeErr := rpcClient.SendRequest(context.Background(), server.Addr(), req, time.Second)
		return probeErr == nil
	}, 3*time.Second, 10*time.Millisecond)
	resp, _, err := s.regionRequestSender.SendReq(bo, req, region.Region, time.Second)
	s.Nil(err)
	s.NotNil(resp)
	regionErr, _ := resp.GetRegionError()
	s.Nil(regionErr)
}

type emptyClient struct {
	client.Client
}

func (s *testRegionRequestToSingleStoreSuite) TestClientExt() {
	var cli client.Client = client.NewRPCClient()
	sender := NewRegionRequestSender(s.cache, cli, oracle.NoopReadTSValidator{})
	s.NotNil(sender.client)
	s.NotNil(sender.getClientExt())
	cli.Close()

	cli = &emptyClient{}
	sender = NewRegionRequestSender(s.cache, cli, oracle.NoopReadTSValidator{})
	s.NotNil(sender.client)
	s.Nil(sender.getClientExt())
}

func (s *testRegionRequestToSingleStoreSuite) TestRegionRequestSenderString() {
	sender := NewRegionRequestSender(s.cache, &fnClient{}, oracle.NoopReadTSValidator{})
	loc, err := s.cache.LocateRegionByID(s.bo, s.region)
	s.Nil(err)
	// invalid region cache before sending request.
	s.cache.InvalidateCachedRegion(loc.Region)
	sender.SendReqCtx(s.bo, tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{}), loc.Region, time.Second, tikvrpc.TiKV)
	s.Equal("{rpcError:<nil>, replicaSelector: <nil>}", sender.String())
}

func (s *testRegionRequestToSingleStoreSuite) TestRegionRequestStats() {
	reqStats := NewRegionRequestRuntimeStats()
	reqStats.RecordRPCRuntimeStats(tikvrpc.CmdGet, time.Second)
	reqStats.RecordRPCRuntimeStats(tikvrpc.CmdGet, time.Millisecond)
	reqStats.RecordRPCRuntimeStats(tikvrpc.CmdCop, time.Second*2)
	reqStats.RecordRPCRuntimeStats(tikvrpc.CmdCop, time.Millisecond*200)
	reqStats.RecordRPCErrorStats("context canceled")
	reqStats.RecordRPCErrorStats("context canceled")
	reqStats.RecordRPCErrorStats("region_not_found")
	reqStats.Merge(NewRegionRequestRuntimeStats())
	reqStats2 := NewRegionRequestRuntimeStats()
	reqStats2.Merge(reqStats.Clone())
	expecteds := []string{
		// Since map iteration order is random, we need to check all possible orders.
		"Get:{num_rpc:2, total_time:1s},Cop:{num_rpc:2, total_time:2.2s}, rpc_errors:{region_not_found:1, context canceled:2}",
		"Get:{num_rpc:2, total_time:1s},Cop:{num_rpc:2, total_time:2.2s}, rpc_errors:{context canceled:2, region_not_found:1}",
		"Cop:{num_rpc:2, total_time:2.2s},Get:{num_rpc:2, total_time:1s}, rpc_errors:{context canceled:2, region_not_found:1}",
		"Cop:{num_rpc:2, total_time:2.2s},Get:{num_rpc:2, total_time:1s}, rpc_errors:{region_not_found:1, context canceled:2}",
	}
	s.Contains(expecteds, reqStats.String())
	s.Contains(expecteds, reqStats2.String())
	for i := 0; i < 50; i++ {
		reqStats.RecordRPCErrorStats("err_" + strconv.Itoa(i))
	}
	s.Regexp("{.*err_.*:1.*, other_error:36}", reqStats.RequestErrorStats.String())
	s.Regexp(".*num_rpc.*total_time.*, rpc_errors:{.*err.*, other_error:36}", reqStats.String())

	access := &ReplicaAccessStats{}
	access.recordReplicaAccessInfo(true, false, 1, 2, "data_not_ready")
	access.recordReplicaAccessInfo(false, false, 3, 4, "not_leader")
	access.recordReplicaAccessInfo(false, true, 5, 6, "server_is_Busy")
	s.Equal("{stale_read, peer:1, store:2, err:data_not_ready}, {peer:3, store:4, err:not_leader}, {replica_read, peer:5, store:6, err:server_is_Busy}", access.String())
	for i := 0; i < 20; i++ {
		access.recordReplicaAccessInfo(false, false, 5+uint64(i)%2, 6, "server_is_Busy")
	}
	expecteds = []string{
		// Since map iteration order is random, we need to check all possible orders.
		"{stale_read, peer:1, store:2, err:data_not_ready}, {peer:3, store:4, err:not_leader}, {replica_read, peer:5, store:6, err:server_is_Busy}, {peer:5, store:6, err:server_is_Busy}, {peer:6, store:6, err:server_is_Busy}, overflow_count:{{peer:5, error_stats:{server_is_Busy:9}}, {peer:6, error_stats:{server_is_Busy:9}}}",
		"{stale_read, peer:1, store:2, err:data_not_ready}, {peer:3, store:4, err:not_leader}, {replica_read, peer:5, store:6, err:server_is_Busy}, {peer:5, store:6, err:server_is_Busy}, {peer:6, store:6, err:server_is_Busy}, overflow_count:{{peer:6, error_stats:{server_is_Busy:9}}, {peer:5, error_stats:{server_is_Busy:9}}}",
	}
	s.Contains(expecteds, access.String())
}

func (s *testRegionRequestToSingleStoreSuite) TestRegionRequestValidateReadTS() {
	oracles.EnableTSValidation.Store(true)
	defer oracles.EnableTSValidation.Store(false)
	o, err := oracles.NewPdOracle(s.pdCli, &oracles.PDOracleOptions{
		UpdateInterval: time.Second * 2,
	})
	s.NoError(err)
	s.regionRequestSender.readTSValidator = o
	defer o.Close()

	testImpl := func(ts func() uint64, staleRead bool, expectedErrorType error) {
		region, err := s.cache.LocateRegionByID(s.bo, s.region)
		s.Nil(err)
		s.NotNil(region)

		req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{
			Key:     []byte("k"),
			Version: ts(),
		})

		req.StaleRead = staleRead
		_, _, _, err = s.regionRequestSender.SendReqCtx(s.bo, req, region.Region, time.Second, tikvrpc.TiKV)

		if expectedErrorType == nil {
			s.NoError(err)
		} else {
			s.Error(err)
			s.IsType(err, expectedErrorType)
		}
	}

	getTS := func() uint64 {
		ts, err := o.GetTimestamp(s.bo.GetCtx(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
		s.NoError(err)
		return ts
	}

	addTS := func(ts uint64, diff time.Duration) uint64 {
		return oracle.ComposeTS(oracle.GetPhysical(oracle.GetTimeFromTS(ts).Add(diff)), oracle.ExtractLogical(ts))
	}

	testImpl(getTS, false, nil)
	testImpl(getTS, true, nil)
	testImpl(func() uint64 { return addTS(getTS(), -time.Minute) }, false, nil)
	testImpl(func() uint64 { return addTS(getTS(), -time.Minute) }, true, nil)
	testImpl(func() uint64 { return addTS(getTS(), +time.Minute) }, false, oracle.ErrFutureTSRead{})
	testImpl(func() uint64 { return addTS(getTS(), +time.Minute) }, true, oracle.ErrFutureTSRead{})
	testImpl(func() uint64 { return math.MaxUint64 }, false, nil)
	if config.NextGen {
		testImpl(func() uint64 { return math.MaxUint64 }, true, nil)
	} else {
		testImpl(func() uint64 { return math.MaxUint64 }, true, oracle.ErrLatestStaleRead{})
	}
}

type noCauseError struct {
	error
}

func (noCauseError) Cause() error {
	return nil
}

func TestGetErrMsg(t *testing.T) {
	err := noCauseError{error: errors.New("no cause err")}
	require.Equal(t, nil, errors.Cause(err))
	require.Panicsf(t, func() {
		_ = errors.Cause(err).Error()
	}, "should panic")
	require.Equal(t, "no cause err", getErrMsg(err))
}

func TestRPCContextString(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		var ctx *RPCContext
		require.Equal(t, "<nil>", ctx.ToBackoffReasonString())
		require.Equal(t, "<nil>", ctx.String())
	})

	t.Run("without proxy", func(t *testing.T) {
		region := RegionVerID{id: 100, confVer: 2, ver: 3}
		meta := &metapb.Region{
			Id:          region.id,
			RegionEpoch: &metapb.RegionEpoch{ConfVer: region.confVer, Version: region.ver},
		}
		peer := &metapb.Peer{Id: 101, StoreId: 1}
		store := newStore(1, "tikv-1", "", "", tikvrpc.TiKV, resolved, nil)
		ctx := &RPCContext{
			Region:     region,
			Meta:       meta,
			Peer:       peer,
			AccessIdx:  4,
			Store:      store,
			Addr:       "tikv-1",
			AccessMode: tiKVOnly,
		}

		require.Equal(
			t,
			fmt.Sprintf(
				"region ID: %d, meta: %s, peer: %s, addr: %s, idx: %d, reqStoreType: %s, runStoreType: %s",
				region.GetID(), meta, peer, "tikv-1", AccessIndex(4), tiKVOnly, tikvrpc.TiKV.Name(),
			),
			ctx.String(),
		)
		require.Equal(
			t,
			fmt.Sprintf(
				"region: %s, peerID: %d, storeID: %d, addr: %s, idx: %d, reqStoreType: %s, runStoreType: %s",
				region.String(), peer.Id, peer.StoreId, "tikv-1", AccessIndex(4), tiKVOnly, tikvrpc.TiKV.Name(),
			),
			ctx.ToBackoffReasonString(),
		)
	})

	t.Run("with proxy", func(t *testing.T) {
		store := newStore(1, "tikv-1", "", "", tikvrpc.TiKV, resolved, nil)
		proxyStore := newStore(2, "tikv-2", "", "", tikvrpc.TiKV, resolved, nil)
		ctx := &RPCContext{
			Region:     RegionVerID{id: 200, confVer: 5, ver: 8},
			Store:      store,
			Addr:       "tikv-1",
			AccessMode: tiKVOnly,
			ProxyStore: proxyStore,
		}

		require.Contains(t, ctx.String(), ", proxy store id: 2, proxy addr: tikv-2")
		require.Contains(t, ctx.ToBackoffReasonString(), ", proxy store id: 2, proxy addr: tikv-2")
	})
}

func TestBackoffErrWithRPCContext(t *testing.T) {
	ctx := &RPCContext{
		Region:     RegionVerID{id: 200, confVer: 5, ver: 8},
		Peer:       &metapb.Peer{Id: 101, StoreId: 1},
		Addr:       "tikv-1",
		AccessMode: tiKVOnly,
	}

	err := newBackoffErrWithRPCContext("reason1", ctx)
	require.Equal(t, "reason1, ctx: "+ctx.ToBackoffReasonString(), err.Error())

	err = newBackoffErrWithRPCContextAndAdvice("reason1", ctx, "advice1")
	require.Equal(t, "reason1, ctx: "+ctx.ToBackoffReasonString()+", advice1", err.Error())
}
