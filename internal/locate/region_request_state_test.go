// Copyright 2023 TiKV Authors
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
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/apicodec"
	"github.com/tikv/client-go/v2/internal/mockstore/mocktikv"
	"github.com/tikv/client-go/v2/internal/retry"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/tikvrpc"
)

type testRegionCacheStaleReadSuite struct {
	*require.Assertions
	cluster             *mocktikv.Cluster
	storeIDs            []uint64
	peerIDs             []uint64
	regionID            uint64
	leaderPeer          uint64
	store2zone          map[uint64]string
	cache               *RegionCache
	bo                  *retry.Backoffer
	regionRequestSender *RegionRequestSender
	mvccStore           mocktikv.MVCCStore
	injection           testRegionCacheFSMSuiteInjection
}

type testRegionCacheFSMSuiteInjection struct {
	leaderRegionError   func(*tikvrpc.Request, string) *errorpb.Error
	followerRegionError func(*tikvrpc.Request, string) *errorpb.Error
	unavailableStoreIDs map[uint64]struct{}
	timeoutStoreIDs     map[uint64]struct{}
}

type SuccessReadType int

const (
	ReadFail SuccessReadType = iota
	SuccessLeaderRead
	SuccessFollowerRead
	SuccessStaleRead
)

func (s *testRegionCacheStaleReadSuite) SetupTest() {
	s.mvccStore = mocktikv.MustNewMVCCStore()
	s.cluster = mocktikv.NewCluster(s.mvccStore)
	s.storeIDs, s.peerIDs, s.regionID, s.leaderPeer, s.store2zone = mocktikv.BootstrapWithMultiZones(s.cluster, 3, 2)
	pdCli := &CodecPDClient{mocktikv.NewPDClient(s.cluster), apicodec.NewCodecV1(apicodec.ModeTxn)}
	s.cache = NewRegionCache(pdCli)
	s.bo = retry.NewNoopBackoff(context.Background())
	client := mocktikv.NewRPCClient(s.cluster, s.mvccStore, nil)
	s.regionRequestSender = NewRegionRequestSender(s.cache, client)
	s.setClient()
	s.injection = testRegionCacheFSMSuiteInjection{
		unavailableStoreIDs: make(map[uint64]struct{}),
	}
}

func (s *testRegionCacheStaleReadSuite) TearDownTest() {
	s.cache.testingKnobs.mockRequestLiveness.Store((*livenessFunc)(nil))
	s.cache.Close()
	s.mvccStore.Close()
}

func (s *testRegionCacheStaleReadSuite) getStore(leader bool) (uint64, *metapb.Store) {
	var (
		zone    string
		peerID  uint64
		storeID uint64
	)
	if leader {
		zone = "z1"
	} else {
		zone = "z2"
	}
	region, _ := s.cluster.GetRegion(s.regionID)
FIND:
	for _, peer := range region.Peers {
		store := s.cluster.GetStore(peer.StoreId)
		for _, label := range store.Labels {
			if label.Key == "zone" && label.Value == zone {
				peerID = peer.Id
				storeID = peer.StoreId
				break FIND
			}
		}
	}
	store := s.cluster.GetStore(storeID)
	if store == nil {
		return 0, nil
	}
	return peerID, store
}

func (s *testRegionCacheStaleReadSuite) getLeader() (uint64, *metapb.Store) {
	return s.getStore(true)
}

func (s *testRegionCacheStaleReadSuite) getFollower() (uint64, *metapb.Store) {
	return s.getStore(false)
}

func (s *testRegionCacheStaleReadSuite) setClient() {
	s.regionRequestSender.client = &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (response *tikvrpc.Response, err error) {
		var store *metapb.Store
		find := false
		for _, one := range s.cluster.GetAllStores() {
			if one.Address == addr {
				store = one
				find = true
				break
			}
		}
		if !find {
			return nil, errors.New("no available connections")
		}
		if _, unavailable := s.injection.unavailableStoreIDs[store.Id]; unavailable {
			return nil, errors.New("no available connections")
		}
		if _, timeout := s.injection.timeoutStoreIDs[store.Id]; timeout {
			return nil, errors.WithMessage(context.DeadlineExceeded, "wait recvLoop")
		}

		zone := ""
		for _, label := range store.Labels {
			if label.Key == "zone" {
				zone = label.Value
				break
			}
		}
		response = &tikvrpc.Response{}
		region, _ := s.cluster.GetRegion(s.regionID)
		peerExist := false
		for _, peer := range region.Peers {
			if req.Peer.Id == peer.Id {
				if peer.StoreId != store.Id {
					response.Resp = &kvrpcpb.GetResponse{RegionError: &errorpb.Error{
						RegionNotFound: &errorpb.RegionNotFound{RegionId: s.regionID},
					}}
					return
				}
				peerExist = true
			}
		}
		if !peerExist {
			response.Resp = &kvrpcpb.GetResponse{RegionError: &errorpb.Error{
				RegionNotFound: &errorpb.RegionNotFound{RegionId: s.regionID},
			}}
			return
		}

		_, leader := s.getLeader()
		s.NotNil(leader)
		isLeader := addr == leader.Address
		if isLeader {
			// leader region error
			if s.injection.leaderRegionError != nil {
				if regionRrr := s.injection.leaderRegionError(req, zone); regionRrr != nil {
					response.Resp = &kvrpcpb.GetResponse{RegionError: regionRrr}
					return
				}
			}
		} else {
			// follower read leader
			if !req.ReplicaRead && !req.StaleRead {
				_, leaderPeer, _ := s.cluster.GetRegionByID(s.regionID)
				response.Resp = &kvrpcpb.GetResponse{RegionError: &errorpb.Error{
					NotLeader: &errorpb.NotLeader{
						RegionId: req.RegionId,
						Leader:   leaderPeer,
					},
				}}
				return
			}
			// follower region error
			if s.injection.followerRegionError != nil {
				if regionRrr := s.injection.followerRegionError(req, zone); regionRrr != nil {
					response.Resp = &kvrpcpb.GetResponse{RegionError: regionRrr}
					return
				}
			}
		}
		// no error
		var successReadType SuccessReadType
		if req.StaleRead {
			successReadType = SuccessStaleRead
		} else if isLeader {
			successReadType = SuccessLeaderRead
		} else {
			successReadType = SuccessFollowerRead
		}
		s.NotEmpty(zone)
		respStr := fmt.Sprintf("%d-%s-%d", store.Id, zone, successReadType)
		response.Resp = &kvrpcpb.GetResponse{Value: []byte(respStr)}
		return
	}}

	tf := func(store *Store, bo *retry.Backoffer) livenessState {
		_, ok := s.injection.unavailableStoreIDs[store.storeID]
		if ok {
			return unreachable
		}
		return reachable
	}
	s.cache.testingKnobs.mockRequestLiveness.Store((*livenessFunc)(&tf))
}

func (s *testRegionCacheStaleReadSuite) extractResp(resp *tikvrpc.Response) (uint64, string, SuccessReadType) {
	resps := strings.Split(string(resp.Resp.(*kvrpcpb.GetResponse).Value), "-")
	s.Len(resps, 3)
	storeID, err := strconv.Atoi(resps[0])
	s.Nil(err)
	successReadType, err := strconv.Atoi(resps[2])
	return uint64(storeID), resps[1], SuccessReadType(successReadType)
}

func (s *testRegionCacheStaleReadSuite) setUnavailableStore(id uint64) {
	s.injection.unavailableStoreIDs[id] = struct{}{}
}

func (s *testRegionCacheStaleReadSuite) setTimeout(id uint64) {
	s.injection.timeoutStoreIDs[id] = struct{}{}
}

func TestRegionCacheStaleRead(t *testing.T) {
	originReloadRegionInterval := atomic.LoadInt64(&reloadRegionInterval)
	originBoTiKVServerBusy := retry.BoTiKVServerBusy
	defer func() {
		reloadRegionInterval = originReloadRegionInterval
		retry.BoTiKVServerBusy = originBoTiKVServerBusy
	}()
	atomic.StoreInt64(&reloadRegionInterval, int64(24*time.Hour)) // disable reload region
	retry.BoTiKVServerBusy = retry.NewConfig("tikvServerBusy", &metrics.BackoffHistogramServerBusy, retry.NewBackoffFnCfg(2, 10, retry.EqualJitter), tikverr.ErrTiKVServerBusy)
	regionCacheTestCases := []RegionCacheTestCase{
		{
			do:                      followerDown,
			leaderRegionValid:       true,
			leaderAsyncReload:       Some(false),
			leaderSuccessReplica:    []string{"z1"},
			leaderSuccessReadType:   SuccessStaleRead,
			followerRegionValid:     true,
			followerAsyncReload:     Some(false),
			followerSuccessReplica:  []string{"z1"},
			followerSuccessReadType: SuccessLeaderRead,
		},
		{
			do:                     followerDownAndUp,
			leaderRegionValid:      true,
			leaderAsyncReload:      None[bool](),
			leaderSuccessReplica:   []string{"z1"},
			leaderSuccessReadType:  SuccessStaleRead,
			followerRegionValid:    true,
			followerAsyncReload:    Some(true),
			followerSuccessReplica: []string{"z1"},
			// because follower's epoch is changed, leader will be selected.
			followerSuccessReadType: SuccessStaleRead,
		},
		{
			do:                    followerMove,
			recoverable:           true,
			leaderRegionValid:     true,
			leaderAsyncReload:     Some(false),
			leaderSuccessReplica:  []string{"z1"},
			leaderSuccessReadType: SuccessStaleRead,
			followerRegionValid:   false,
			followerAsyncReload:   Some(false),
			// may async reload region and access it from leader.
			followerSuccessReplica:  []string{},
			followerSuccessReadType: ReadFail,
		},
		{
			do:                evictLeader,
			leaderRegionValid: true,
			leaderAsyncReload: Some(false),
			// leader is evicted, but can still serve as follower.
			leaderSuccessReplica:    []string{"z1"},
			leaderSuccessReadType:   SuccessStaleRead,
			followerRegionValid:     true,
			followerAsyncReload:     Some(false),
			followerSuccessReplica:  []string{"z2"},
			followerSuccessReadType: SuccessStaleRead,
		},
		{
			do:                      leaderMove,
			leaderRegionValid:       false,
			leaderAsyncReload:       Some(false),
			leaderSuccessReplica:    []string{},
			leaderSuccessReadType:   ReadFail,
			followerRegionValid:     true,
			followerAsyncReload:     Some(false),
			followerSuccessReplica:  []string{"z2"},
			followerSuccessReadType: SuccessStaleRead,
		},
		{
			do:                      leaderDown,
			leaderRegionValid:       true,
			leaderAsyncReload:       Some(true),
			leaderSuccessReplica:    []string{"z2", "z3"},
			leaderSuccessReadType:   SuccessFollowerRead,
			followerRegionValid:     true,
			followerAsyncReload:     Some(false),
			followerSuccessReplica:  []string{"z2"},
			followerSuccessReadType: SuccessStaleRead,
		},
		{
			do:                      leaderDownAndUp,
			leaderRegionValid:       true,
			leaderAsyncReload:       Some(true),
			leaderSuccessReplica:    []string{"z2", "z3"},
			leaderSuccessReadType:   SuccessFollowerRead,
			followerRegionValid:     true,
			followerAsyncReload:     None[bool](),
			followerSuccessReplica:  []string{"z2"},
			followerSuccessReadType: SuccessStaleRead,
		},
		{
			do:                      leaderDownAndElect,
			leaderRegionValid:       true,
			leaderAsyncReload:       Some(true),
			leaderSuccessReplica:    []string{"z2", "z3"},
			leaderSuccessReadType:   SuccessFollowerRead,
			followerRegionValid:     true,
			followerAsyncReload:     None[bool](),
			followerSuccessReplica:  []string{"z2"},
			followerSuccessReadType: SuccessStaleRead,
		},
		{
			do:                      leaderDataIsNotReady,
			leaderRegionValid:       true,
			leaderAsyncReload:       Some(false),
			leaderSuccessReplica:    []string{"z1"},
			leaderSuccessReadType:   SuccessLeaderRead,
			followerRegionValid:     true,
			followerAsyncReload:     Some(false),
			followerSuccessReplica:  []string{"z2"},
			followerSuccessReadType: SuccessStaleRead,
		},
		{
			do:                      followerDataIsNotReady,
			leaderRegionValid:       true,
			leaderAsyncReload:       Some(false),
			leaderSuccessReplica:    []string{"z1"},
			leaderSuccessReadType:   SuccessStaleRead,
			followerRegionValid:     true,
			followerAsyncReload:     Some(false),
			followerSuccessReplica:  []string{"z1"},
			followerSuccessReadType: SuccessLeaderRead,
		},
		{
			debug:                   true,
			do:                      leaderServerIsBusy,
			recoverable:             true,
			leaderRegionValid:       true,
			leaderAsyncReload:       Some(false),
			leaderSuccessReplica:    []string{"z2", "z3"},
			leaderSuccessReadType:   SuccessFollowerRead,
			followerRegionValid:     true,
			followerAsyncReload:     Some(false),
			followerSuccessReplica:  []string{"z2"},
			followerSuccessReadType: SuccessStaleRead,
		},
		{
			do:                      followerServerIsBusy,
			recoverable:             true,
			leaderRegionValid:       true,
			leaderAsyncReload:       Some(false),
			leaderSuccessReplica:    []string{"z1"},
			leaderSuccessReadType:   SuccessStaleRead,
			followerRegionValid:     true,
			followerAsyncReload:     Some(false),
			followerSuccessReplica:  []string{"z1"},
			followerSuccessReadType: SuccessLeaderRead,
		},
		{
			do:                      leaderDataIsNotReady,
			extra:                   []func(suite *testRegionCacheStaleReadSuite){followerServerIsBusy},
			recoverable:             true,
			leaderRegionValid:       true,
			leaderAsyncReload:       Some(false),
			leaderSuccessReplica:    []string{"z1"},
			leaderSuccessReadType:   SuccessLeaderRead,
			followerRegionValid:     true,
			followerAsyncReload:     Some(false),
			followerSuccessReplica:  []string{"z1"},
			followerSuccessReadType: SuccessLeaderRead,
		},
		{
			do:                      leaderDataIsNotReady,
			extra:                   []func(suite *testRegionCacheStaleReadSuite){followerDataIsNotReady},
			recoverable:             true,
			leaderRegionValid:       true,
			leaderAsyncReload:       Some(false),
			leaderSuccessReplica:    []string{"z1"},
			leaderSuccessReadType:   SuccessLeaderRead,
			followerRegionValid:     true,
			followerAsyncReload:     Some(false),
			followerSuccessReplica:  []string{"z1"},
			followerSuccessReadType: SuccessLeaderRead,
		},
		{
			do:                      leaderDataIsNotReady,
			extra:                   []func(suite *testRegionCacheStaleReadSuite){followerDown},
			recoverable:             true,
			leaderRegionValid:       true,
			leaderAsyncReload:       Some(false),
			leaderSuccessReplica:    []string{"z1"},
			leaderSuccessReadType:   SuccessLeaderRead,
			followerRegionValid:     true,
			followerAsyncReload:     Some(false),
			followerSuccessReplica:  []string{"z1"},
			followerSuccessReadType: SuccessLeaderRead,
		},
		{
			do:                      leaderServerIsBusy,
			extra:                   []func(suite *testRegionCacheStaleReadSuite){followerServerIsBusy},
			recoverable:             true,
			leaderRegionValid:       true,
			leaderAsyncReload:       Some(false),
			leaderSuccessReplica:    []string{"z3"},
			leaderSuccessReadType:   SuccessFollowerRead,
			followerRegionValid:     true,
			followerAsyncReload:     Some(false),
			followerSuccessReplica:  []string{"z3"},
			followerSuccessReadType: SuccessFollowerRead,
		},
		{
			do:                      leaderServerIsBusy,
			extra:                   []func(suite *testRegionCacheStaleReadSuite){followerDataIsNotReady},
			recoverable:             true,
			leaderRegionValid:       true,
			leaderAsyncReload:       Some(false),
			leaderSuccessReplica:    []string{"z2", "z3"},
			leaderSuccessReadType:   SuccessFollowerRead,
			followerRegionValid:     true,
			followerAsyncReload:     Some(false),
			followerSuccessReplica:  []string{"z2", "z3"},
			followerSuccessReadType: SuccessFollowerRead,
		},
		{
			do:                      leaderServerIsBusy,
			extra:                   []func(suite *testRegionCacheStaleReadSuite){followerDown},
			recoverable:             true,
			leaderRegionValid:       true,
			leaderAsyncReload:       Some(false),
			leaderSuccessReplica:    []string{"z3"},
			leaderSuccessReadType:   SuccessFollowerRead,
			followerRegionValid:     true,
			followerAsyncReload:     Some(false),
			followerSuccessReplica:  []string{"z3"},
			followerSuccessReadType: SuccessFollowerRead,
		},
		{
			do:                      leaderDown,
			extra:                   []func(suite *testRegionCacheStaleReadSuite){followerDataIsNotReady},
			recoverable:             true,
			leaderRegionValid:       true,
			leaderAsyncReload:       Some(true),
			leaderSuccessReplica:    []string{"z2", "z3"},
			leaderSuccessReadType:   SuccessFollowerRead,
			followerRegionValid:     true,
			followerAsyncReload:     Some(true),
			followerSuccessReplica:  []string{"z2", "z3"},
			followerSuccessReadType: SuccessFollowerRead,
		},
		{
			do:                      leaderDown,
			extra:                   []func(suite *testRegionCacheStaleReadSuite){followerServerIsBusy},
			recoverable:             true,
			leaderRegionValid:       true,
			leaderAsyncReload:       Some(true),
			leaderSuccessReplica:    []string{"z3"},
			leaderSuccessReadType:   SuccessFollowerRead,
			followerRegionValid:     true,
			followerAsyncReload:     Some(true),
			followerSuccessReplica:  []string{"z3"},
			followerSuccessReadType: SuccessFollowerRead,
		},
		{
			do:                      leaderDown,
			extra:                   []func(suite *testRegionCacheStaleReadSuite){followerDown},
			recoverable:             true,
			leaderRegionValid:       true,
			leaderAsyncReload:       Some(true),
			leaderSuccessReplica:    []string{"z3"},
			leaderSuccessReadType:   SuccessFollowerRead,
			followerRegionValid:     true,
			followerAsyncReload:     Some(true),
			followerSuccessReplica:  []string{"z3"},
			followerSuccessReadType: SuccessFollowerRead,
		},
	}
	tests := []func(*testRegionCacheStaleReadSuite, *RegionCacheTestCase){
		testStaleReadFollower, testStaleReadLeader,
	}
	for _, regionCacheTestCase := range regionCacheTestCases {
		for _, test := range tests {
			s := &testRegionCacheStaleReadSuite{
				Assertions: require.New(t),
			}
			s.SetupTest()
			_, err := s.cache.LocateRegionByID(s.bo, s.regionID)
			s.Nil(err)
			regionCacheTestCase.do(s)
			for _, extra := range regionCacheTestCase.extra {
				extra(s)
			}
			test(s, &regionCacheTestCase)
			s.TearDownTest()
		}
	}
}

func testStaleReadFollower(s *testRegionCacheStaleReadSuite, r *RegionCacheTestCase) {
	testStaleRead(s, r, "z2")
}

func testStaleReadLeader(s *testRegionCacheStaleReadSuite, r *RegionCacheTestCase) {
	testStaleRead(s, r, "z1")
}

func testStaleRead(s *testRegionCacheStaleReadSuite, r *RegionCacheTestCase, zone string) {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	leaderZone := zone == "z1"
	var available bool
	if leaderZone {
		available = len(r.leaderSuccessReplica) > 0
	} else {
		available = len(r.followerSuccessReplica) > 0
	}

	regionLoc, err := s.cache.LocateRegionByID(s.bo, s.regionID)
	s.Nil(err)
	s.NotNil(regionLoc)

	s.cache.mu.RLock()
	region := s.cache.getRegionByIDFromCache(s.regionID)
	s.cache.mu.RUnlock()
	defer func() {
		var (
			valid       bool
			asyncReload *bool
		)
		if leaderZone {
			valid = r.leaderRegionValid
			asyncReload = r.leaderAsyncReload.Inner()
		} else {
			valid = r.followerRegionValid
			asyncReload = r.followerAsyncReload.Inner()
		}
		s.Equal(valid, region.isValid())

		if asyncReload == nil {
			return
		}

		s.cache.regionsNeedReload.Lock()
		if *asyncReload {
			s.Len(s.cache.regionsNeedReload.regions, 1)
			s.Equal(s.cache.regionsNeedReload.regions[0], s.regionID)
		} else {
			s.Empty(s.cache.regionsNeedReload.regions)
		}
		s.cache.regionsNeedReload.Unlock()
	}()

	bo := retry.NewBackoffer(ctx, -1)
	req := tikvrpc.NewReplicaReadRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("key")}, kv.ReplicaReadMixed, nil)
	req.EnableStaleRead()
	ops := []StoreSelectorOption{WithMatchLabels([]*metapb.StoreLabel{{
		Key:   "zone",
		Value: zone,
	}})}

	resp, _, _, err := s.regionRequestSender.SendReqCtx(bo, req, regionLoc.Region, time.Second, tikvrpc.TiKV, ops...)
	if !available {
		if err != nil {
			return
		}
		regionErr, err := resp.GetRegionError()
		s.Nil(err)
		s.NotNil(regionErr)
		return
	}

	_, successZone, successReadType := s.extractResp(resp)
	find := false
	if leaderZone {
		s.Equal(r.leaderSuccessReadType, successReadType)
		for _, z := range r.leaderSuccessReplica {
			if z == successZone {
				find = true
				break
			}
		}
	} else {
		s.Equal(r.followerSuccessReadType, successReadType)
		for _, z := range r.followerSuccessReplica {
			if z == successZone {
				find = true
				break
			}
		}
	}
	s.True(find)
}

type Option[T interface{}] struct {
	inner *T
}

func Some[T interface{}](inner T) Option[T] {
	return Option[T]{inner: &inner}
}

func None[T interface{}]() Option[T] {
	return Option[T]{inner: nil}
}

func (o Option[T]) Inner() *T {
	return o.inner
}

type RegionCacheTestCase struct {
	debug       bool
	do          func(s *testRegionCacheStaleReadSuite)
	extra       []func(s *testRegionCacheStaleReadSuite)
	recoverable bool
	// local peer is leader
	leaderRegionValid     bool
	leaderAsyncReload     Option[bool]
	leaderSuccessReplica  []string
	leaderSuccessReadType SuccessReadType
	// local peer is follower
	followerRegionValid     bool
	followerAsyncReload     Option[bool]
	followerSuccessReplica  []string
	followerSuccessReadType SuccessReadType
}

func followerDown(s *testRegionCacheStaleReadSuite) {
	_, follower := s.getFollower()
	s.NotNil(follower)
	s.setUnavailableStore(follower.Id)
}

func followerDownAndUp(s *testRegionCacheStaleReadSuite) {
	s.cache.mu.RLock()
	cachedRegion := s.cache.getRegionByIDFromCache(s.regionID)
	s.cache.mu.RUnlock()
	_, follower := s.getFollower()
	s.NotNil(cachedRegion)
	s.NotNil(follower)
	regionStore := cachedRegion.getStore()
	for _, storeIdx := range regionStore.accessIndex[tiKVOnly] {
		if regionStore.stores[storeIdx].storeID == follower.Id {
			atomic.AddUint32(&regionStore.stores[storeIdx].epoch, 1)
		}
	}
}

func followerMove(s *testRegionCacheStaleReadSuite) {
	peerID, follower := s.getFollower()
	zone := ""
	for _, label := range follower.Labels {
		if label.Key == "zone" {
			zone = label.Value
			break
		}
	}
	s.NotEqual("", zone)
	var target *metapb.Store
FIND:
	for _, store := range s.cluster.GetAllStores() {
		if store.Id == follower.Id {
			continue
		}
		for _, label := range store.Labels {
			if label.Key == "zone" && label.Value == zone {
				target = store
				break FIND
			}
		}
	}
	s.NotNil(target)
	s.cluster.RemovePeer(s.regionID, peerID)
	s.cluster.AddPeer(s.regionID, target.Id, peerID)
}

func evictLeader(s *testRegionCacheStaleReadSuite) {
	region, leader := s.cluster.GetRegion(s.regionID)
	for _, peer := range region.Peers {
		if peer.Id != leader {
			s.cluster.ChangeLeader(s.regionID, peer.Id)
			return
		}
	}
	s.Fail("unreachable")
}

func leaderMove(s *testRegionCacheStaleReadSuite) {
	peerID, leader := s.getLeader()
	zone := ""
	for _, label := range leader.Labels {
		if label.Key == "zone" {
			zone = label.Value
			break
		}
	}
	s.NotEqual("", zone)
	var target *metapb.Store
FIND:
	for _, store := range s.cluster.GetAllStores() {
		if store.Id == leader.Id {
			continue
		}
		for _, label := range store.Labels {
			if label.Key == "zone" && label.Value == zone {
				target = store
				break FIND
			}
		}
	}
	s.NotNil(target)
	s.cluster.RemovePeer(s.regionID, peerID)
	s.cluster.AddPeer(s.regionID, target.Id, peerID)
	s.cluster.ChangeLeader(s.regionID, peerID)
}

func leaderDown(s *testRegionCacheStaleReadSuite) {
	_, leader := s.getLeader()
	s.NotNil(leader)
	s.setUnavailableStore(leader.Id)
}

func leaderDownAndUp(s *testRegionCacheStaleReadSuite) {
	s.cache.mu.RLock()
	cachedRegion := s.cache.getRegionByIDFromCache(s.regionID)
	s.cache.mu.RUnlock()
	_, leader := s.getLeader()
	s.NotNil(cachedRegion)
	s.NotNil(leader)
	regionStore := cachedRegion.getStore()
	for _, storeIdx := range regionStore.accessIndex[tiKVOnly] {
		if regionStore.stores[storeIdx].storeID == leader.Id {
			atomic.AddUint32(&regionStore.stores[storeIdx].epoch, 1)
		}
	}
}
func leaderDownAndElect(s *testRegionCacheStaleReadSuite) {
	_, leader := s.getLeader()
	s.NotNil(leader)
	leaderMove(s)
	s.setUnavailableStore(leader.Id)
}

func leaderDataIsNotReady(s *testRegionCacheStaleReadSuite) {
	peerID, _ := s.getLeader()
	s.injection.leaderRegionError = func(req *tikvrpc.Request, zone string) *errorpb.Error {
		if !req.StaleRead || zone != "z1" {
			return nil
		}
		return &errorpb.Error{
			DataIsNotReady: &errorpb.DataIsNotReady{
				RegionId: s.regionID,
				PeerId:   peerID,
				SafeTs:   0,
			},
		}
	}
}

func leaderServerIsBusy(s *testRegionCacheStaleReadSuite) {
	s.injection.leaderRegionError = func(req *tikvrpc.Request, zone string) *errorpb.Error {
		if zone != "z1" {
			return nil
		}
		return &errorpb.Error{
			ServerIsBusy: &errorpb.ServerIsBusy{
				Reason:    "test",
				BackoffMs: 1,
			},
		}
	}
}

func followerDataIsNotReady(s *testRegionCacheStaleReadSuite) {
	s.injection.followerRegionError = func(req *tikvrpc.Request, zone string) *errorpb.Error {
		if !req.StaleRead || zone != "z2" {
			return nil
		}
		return &errorpb.Error{
			DataIsNotReady: &errorpb.DataIsNotReady{
				RegionId: s.regionID,
				SafeTs:   0,
			},
		}
	}
}

func followerServerIsBusy(s *testRegionCacheStaleReadSuite) {
	s.injection.followerRegionError = func(req *tikvrpc.Request, zone string) *errorpb.Error {
		if zone != "z2" {
			return nil
		}
		return &errorpb.Error{
			ServerIsBusy: &errorpb.ServerIsBusy{
				Reason:    "test",
				BackoffMs: 1,
			},
		}
	}
}
