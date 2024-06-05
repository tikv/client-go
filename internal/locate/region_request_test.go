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
	"github.com/tikv/client-go/v2/internal/apicodec"
	"github.com/tikv/client-go/v2/internal/client"
	"github.com/tikv/client-go/v2/internal/client/mockserver"
	"github.com/tikv/client-go/v2/internal/mockstore/mocktikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	pderr "github.com/tikv/pd/client/errs"
	"google.golang.org/grpc"
)

func TestRegionRequestToSingleStore(t *testing.T) {
	suite.Run(t, new(testRegionRequestToSingleStoreSuite))
}

type testRegionRequestToSingleStoreSuite struct {
	suite.Suite
	cluster             *mocktikv.Cluster
	store               uint64
	peer                uint64
	region              uint64
	cache               *RegionCache
	bo                  *retry.Backoffer
	regionRequestSender *RegionRequestSender
	mvccStore           mocktikv.MVCCStore
}

func (s *testRegionRequestToSingleStoreSuite) SetupTest() {
	s.mvccStore = mocktikv.MustNewMVCCStore()
	s.cluster = mocktikv.NewCluster(s.mvccStore)
	s.store, s.peer, s.region = mocktikv.BootstrapWithSingleStore(s.cluster)
	pdCli := &CodecPDClient{mocktikv.NewPDClient(s.cluster), apicodec.NewCodecV1(apicodec.ModeTxn)}
	s.cache = NewRegionCache(pdCli)
	s.bo = retry.NewNoopBackoff(context.Background())
	client := mocktikv.NewRPCClient(s.cluster, s.mvccStore, nil)
	s.regionRequestSender = NewRegionRequestSender(s.cache, client)

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
	return f.fn(ctx, addr, req, timeout)
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
	func() {
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
	}()
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
	func() {
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
	}()
}

func (s *testRegionRequestToSingleStoreSuite) TestOnSendFailedWithStoreRestart() {
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

func (s *testRegionRequestToSingleStoreSuite) TestOnSendFailedWithCancelled() {
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

func (s *testRegionRequestToSingleStoreSuite) TestNoReloadRegionForGrpcWhenCtxCanceled() {
	// prepare a mock tikv grpc server
	addr := "localhost:56341"
	lis, err := net.Listen("tcp", addr)
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
	sender := NewRegionRequestSender(s.cache, cli)
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
	sender = NewRegionRequestSender(s.cache, client1)
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
	s.True(IsFakeRegionError(regionErr))
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
				regionRequestSender := NewRegionRequestSender(s.cache, fnClient)
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
		s.True(req.ClusterId > 0)
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
	s.Nil(regionErr)
}

type emptyClient struct {
	client.Client
}

func (s *testRegionRequestToSingleStoreSuite) TestClientExt() {
	var cli client.Client = client.NewRPCClient()
	sender := NewRegionRequestSender(s.cache, cli)
	s.NotNil(sender.client)
	s.NotNil(sender.getClientExt())
	cli.Close()

	cli = &emptyClient{}
	sender = NewRegionRequestSender(s.cache, cli)
	s.NotNil(sender.client)
	s.Nil(sender.getClientExt())
}

func (s *testRegionRequestToSingleStoreSuite) TestRegionRequestSenderString() {
	sender := NewRegionRequestSender(s.cache, &fnClient{})
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

type noCauseError struct {
	error
}

func (_ noCauseError) Cause() error {
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
