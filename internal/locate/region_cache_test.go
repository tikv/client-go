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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/locate/region_cache_test.go
//

// Copyright 2016 PingCAP, Inc.
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
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/client-go/v2/config/retry"
	"github.com/tikv/client-go/v2/internal/apicodec"
	"github.com/tikv/client-go/v2/internal/mockstore/mocktikv"
	"github.com/tikv/client-go/v2/kv"
	pd "github.com/tikv/pd/client"
	uatomic "go.uber.org/atomic"
)

type inspectedPDClient struct {
	pd.Client
	getRegion func(ctx context.Context, cli pd.Client, key []byte, opts ...pd.GetRegionOption) (*pd.Region, error)
}

func (c *inspectedPDClient) GetRegion(ctx context.Context, key []byte, opts ...pd.GetRegionOption) (*pd.Region, error) {
	if c.getRegion != nil {
		return c.getRegion(ctx, c.Client, key, opts...)
	}
	return c.Client.GetRegion(ctx, key, opts...)
}

func TestBackgroundRunner(t *testing.T) {
	t.Run("ShutdownWait", func(t *testing.T) {
		dur := 100 * time.Millisecond
		r := newBackgroundRunner(context.Background())
		r.run(func(ctx context.Context) {
			time.Sleep(dur)
		})
		start := time.Now()
		r.shutdown(true)
		require.True(t, time.Since(start) >= dur)
	})

	t.Run("ShutdownNoWait", func(t *testing.T) {
		dur := 100 * time.Millisecond
		done := make(chan struct{})
		r := newBackgroundRunner(context.Background())
		r.run(func(ctx context.Context) {
			select {
			case <-ctx.Done():
				close(done)
			case <-time.After(dur):
				require.Fail(t, "run should be canceled by shutdown")
			}
		})
		r.shutdown(false)
		<-done
	})

	t.Run("RunAfterShutdown", func(t *testing.T) {
		var called atomic.Bool
		r := newBackgroundRunner(context.Background())
		r.shutdown(false)
		r.run(func(ctx context.Context) {
			called.Store(true)
		})
		require.False(t, called.Load())
		r.schedule(until(func() bool {
			called.Store(true)
			return true
		}), time.Second)
		require.False(t, called.Load())
		r.scheduleWithTrigger(until(func() bool {
			called.Store(true)
			return true
		}), time.Second, make(chan struct{}))
		require.False(t, called.Load())
	})

	t.Run("Schedule", func(t *testing.T) {
		var (
			done     = make(chan struct{})
			interval = 20 * time.Millisecond
			history  = make([]int64, 0, 3)
			start    = time.Now().UnixMilli()
		)
		r := newBackgroundRunner(context.Background())
		r.schedule(func(_ context.Context, t time.Time) bool {
			history = append(history, t.UnixMilli())
			if len(history) == 3 {
				close(done)
				return true
			}
			return false
		}, interval)
		<-done
		require.Equal(t, 3, len(history))
		for i := range history {
			require.LessOrEqual(t, int64(i+1)*interval.Milliseconds(), history[i]-start)
		}

		history = history[:0]
		start = time.Now().UnixMilli()
		r.schedule(func(ctx context.Context, t time.Time) bool {
			history = append(history, t.UnixMilli())
			return false
		}, interval)
		time.Sleep(interval*3 + interval/2)
		r.shutdown(true)
		require.Equal(t, 3, len(history))
		for i := range history {
			require.LessOrEqual(t, int64(i+1)*interval.Milliseconds(), history[i]-start)
		}
	})

	t.Run("ScheduleWithTrigger", func(t *testing.T) {
		var (
			done     = make(chan struct{})
			trigger  = make(chan struct{})
			interval = 20 * time.Millisecond
			history  = make([]int64, 0, 3)
			start    = time.Now().UnixMilli()
		)
		r := newBackgroundRunner(context.Background())
		r.scheduleWithTrigger(func(ctx context.Context, t time.Time) bool {
			if t.IsZero() {
				history = append(history, -1)
			} else {
				history = append(history, t.UnixMilli())
			}
			if len(history) == 3 {
				close(done)
				return true
			}
			return false
		}, interval, trigger)
		trigger <- struct{}{}
		time.Sleep(interval + interval/2)
		trigger <- struct{}{}
		<-done
		require.Equal(t, 3, len(history))
		require.Equal(t, int64(-1), history[0])
		require.Equal(t, int64(-1), history[2])
		require.LessOrEqual(t, int64(1)*interval.Milliseconds(), history[1]-start)

		history = history[:0]
		start = time.Now().UnixMilli()
		r.scheduleWithTrigger(func(ctx context.Context, t time.Time) bool {
			if t.IsZero() {
				history = append(history, -1)
			} else {
				history = append(history, t.UnixMilli())
			}
			return false
		}, interval, trigger)
		trigger <- struct{}{}
		trigger <- struct{}{}
		close(trigger)
		time.Sleep(interval + interval/2)
		r.shutdown(true)
		require.Equal(t, 3, len(history))
		require.Equal(t, int64(-1), history[0])
		require.Equal(t, int64(-1), history[1])
		require.LessOrEqual(t, int64(1)*interval.Milliseconds(), history[2]-start)
	})
}

func TestRegionCache(t *testing.T) {
	suite.Run(t, new(testRegionCacheSuite))
}

type testRegionCacheSuite struct {
	suite.Suite
	mvccStore mocktikv.MVCCStore
	cluster   *mocktikv.Cluster
	store1    uint64 // store1 is leader
	store2    uint64 // store2 is follower
	peer1     uint64 // peer1 is leader
	peer2     uint64 // peer2 is follower
	region1   uint64
	cache     *RegionCache
	bo        *retry.Backoffer
	onClosed  func()
}

func (s *testRegionCacheSuite) SetupTest() {
	s.mvccStore = mocktikv.MustNewMVCCStore()
	s.cluster = mocktikv.NewCluster(s.mvccStore)
	storeIDs, peerIDs, regionID, _ := mocktikv.BootstrapWithMultiStores(s.cluster, 2)
	s.region1 = regionID
	s.store1 = storeIDs[0]
	s.store2 = storeIDs[1]
	s.peer1 = peerIDs[0]
	s.peer2 = peerIDs[1]
	pdCli := &CodecPDClient{mocktikv.NewPDClient(s.cluster), apicodec.NewCodecV1(apicodec.ModeTxn)}
	s.cache = NewRegionCache(pdCli)
	s.bo = retry.NewBackofferWithVars(context.Background(), 5000, nil)
}

func (s *testRegionCacheSuite) TearDownTest() {
	s.cache.Close()
	s.mvccStore.Close()
	if s.onClosed != nil {
		s.onClosed()
	}
}

func (s *testRegionCacheSuite) storeAddr(id uint64) string {
	return fmt.Sprintf("store%d", id)
}

func (s *testRegionCacheSuite) checkCache(len int) {
	ts := time.Now().Unix()
	s.Equal(validRegions(s.cache.mu.regions, ts), len)
	s.Equal(validRegionsSearchedByVersions(s.cache.mu.latestVersions, s.cache.mu.regions, ts), len)
	s.Equal(s.cache.mu.sorted.ValidRegionsInBtree(ts), len)
}

func validRegionsSearchedByVersions(
	versions map[uint64]RegionVerID,
	regions map[RegionVerID]*Region,
	ts int64,
) (count int) {
	for _, ver := range versions {
		region, ok := regions[ver]
		if !ok || !region.checkRegionCacheTTL(ts) {
			continue
		}
		count++
	}
	return
}

func validRegions(regions map[RegionVerID]*Region, ts int64) (len int) {
	for _, region := range regions {
		if !region.checkRegionCacheTTL(ts) {
			continue
		}
		len++
	}
	return
}

func (s *testRegionCacheSuite) getRegion(key []byte) *Region {
	_, err := s.cache.LocateKey(s.bo, key)
	s.Nil(err)
	r, expired := s.cache.searchCachedRegionByKey(key, false)
	s.False(expired)
	s.NotNil(r)
	return r
}

func (s *testRegionCacheSuite) getRegionWithEndKey(key []byte) *Region {
	_, err := s.cache.LocateEndKey(s.bo, key)
	s.Nil(err)
	r, expired := s.cache.searchCachedRegionByKey(key, true)
	s.False(expired)
	s.NotNil(r)
	return r
}

func (s *testRegionCacheSuite) getAddr(key []byte, replicaRead kv.ReplicaReadType, seed uint32) string {
	loc, err := s.cache.LocateKey(s.bo, key)
	s.Nil(err)
	ctx, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, replicaRead, seed)
	s.Nil(err)
	if ctx == nil {
		return ""
	}
	return ctx.Addr
}

func (s *testRegionCacheSuite) TestStoreLabels() {
	testcases := []struct {
		storeID uint64
	}{
		{
			storeID: s.store1,
		},
		{
			storeID: s.store2,
		},
	}
	for _, testcase := range testcases {
		s.T().Log(testcase.storeID)
		store := s.cache.getStoreOrInsertDefault(testcase.storeID)
		_, err := store.initResolve(s.bo, s.cache)
		s.Nil(err)
		labels := []*metapb.StoreLabel{
			{
				Key:   "id",
				Value: fmt.Sprintf("%v", testcase.storeID),
			},
		}
		stores := s.cache.filterStores(nil, func(s *Store) bool { return s.IsLabelsMatch(labels) })
		s.Equal(len(stores), 1)
		s.Equal(stores[0].labels, labels)
	}
}

func (s *testRegionCacheSuite) TestSimple() {
	seed := rand.Uint32()
	r := s.getRegion([]byte("a"))
	s.NotNil(r)
	s.Equal(r.GetID(), s.region1)
	s.Equal(s.getAddr([]byte("a"), kv.ReplicaReadLeader, 0), s.storeAddr(s.store1))
	s.Equal(s.getAddr([]byte("a"), kv.ReplicaReadFollower, seed), s.storeAddr(s.store2))
	s.checkCache(1)
	s.Equal(r.GetMeta(), r.meta)
	s.Equal(r.GetLeaderPeerID(), r.meta.Peers[r.getStore().workTiKVIdx].Id)
	s.cache.mu.regions[r.VerID()].ttl = 0
	var expired bool
	r, expired = s.cache.searchCachedRegionByKey([]byte("a"), true)
	s.True(expired)
	s.NotNil(r)
}

// TestResolveStateTransition verifies store's resolve state transition. For example,
// a newly added store is in unresolved state and will be resolved soon if it's an up store,
// or in tombstone state if it's a tombstone.
func (s *testRegionCacheSuite) TestResolveStateTransition() {
	cache := s.cache
	bo := retry.NewNoopBackoff(context.Background())

	// Check resolving normal stores. The resolve state should be resolved.
	for _, storeMeta := range s.cluster.GetAllStores() {
		store := cache.getStoreOrInsertDefault(storeMeta.GetId())
		s.Equal(store.getResolveState(), unresolved)
		addr, err := store.initResolve(bo, cache)
		s.Nil(err)
		s.Equal(addr, storeMeta.GetAddress())
		s.Equal(store.getResolveState(), resolved)
	}

	waitResolve := func(s *Store) {
		for i := 0; i < 10; i++ {
			if s.getResolveState() != needCheck {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	// Mark the store needCheck. The resolve state should be resolved soon.
	store := cache.getStoreOrInsertDefault(s.store1)
	cache.markStoreNeedCheck(store)
	waitResolve(store)
	s.Equal(store.getResolveState(), resolved)

	// Mark the store needCheck and it becomes a tombstone. The resolve state should be tombstone.
	s.cluster.MarkTombstone(s.store1)
	cache.markStoreNeedCheck(store)
	waitResolve(store)
	s.Equal(store.getResolveState(), tombstone)
	s.cluster.StartStore(s.store1)

	// Mark the store needCheck and it's deleted from PD. The resolve state should be tombstone.
	cache.clear()
	store = cache.getStoreOrInsertDefault(s.store1)
	store.initResolve(bo, cache)
	s.Equal(store.getResolveState(), resolved)
	storeMeta := s.cluster.GetStore(s.store1)
	s.cluster.RemoveStore(s.store1)
	cache.markStoreNeedCheck(store)
	waitResolve(store)
	s.Equal(store.getResolveState(), tombstone)
	s.cluster.AddStore(storeMeta.GetId(), storeMeta.GetAddress(), storeMeta.GetLabels()...)

	// Mark the store needCheck and its address and labels are changed.
	// The resolve state should be deleted and a new store is added to the cache.
	cache.clear()
	store = cache.getStoreOrInsertDefault(s.store1)
	store.initResolve(bo, cache)
	s.Equal(store.getResolveState(), resolved)
	s.cluster.UpdateStoreAddr(s.store1, store.addr+"0", &metapb.StoreLabel{Key: "k", Value: "v"})
	cache.markStoreNeedCheck(store)
	waitResolve(store)
	s.Equal(store.getResolveState(), deleted)
	newStore := cache.getStoreOrInsertDefault(s.store1)
	s.Equal(newStore.getResolveState(), resolved)
	s.Equal(newStore.addr, store.addr+"0")
	s.Equal(newStore.labels, []*metapb.StoreLabel{{Key: "k", Value: "v"}})

	// Check initResolve()ing a tombstone store. The resolve state should be tombstone.
	cache.clear()
	s.cluster.MarkTombstone(s.store1)
	store = cache.getStoreOrInsertDefault(s.store1)
	for i := 0; i < 2; i++ {
		addr, err := store.initResolve(bo, cache)
		s.Nil(err)
		s.Equal(addr, "")
		s.Equal(store.getResolveState(), tombstone)
	}
	s.cluster.StartStore(s.store1)
	cache.clear()

	// Check initResolve()ing a dropped store. The resolve state should be tombstone.
	cache.clear()
	storeMeta = s.cluster.GetStore(s.store1)
	s.cluster.RemoveStore(s.store1)
	store = cache.getStoreOrInsertDefault(s.store1)
	for i := 0; i < 2; i++ {
		addr, err := store.initResolve(bo, cache)
		s.Nil(err)
		s.Equal(addr, "")
		s.Equal(store.getResolveState(), tombstone)
	}
	s.cluster.AddStore(storeMeta.GetId(), storeMeta.GetAddress(), storeMeta.GetLabels()...)
}

func (s *testRegionCacheSuite) TestNeedExpireRegionAfterTTL() {
	s.onClosed = func() { SetRegionCacheTTLWithJitter(600, 60) }
	SetRegionCacheTTLWithJitter(2, 0)

	cntGetRegion := 0
	s.cache.pdClient = &inspectedPDClient{
		Client: s.cache.pdClient,
		getRegion: func(ctx context.Context, cli pd.Client, key []byte, opts ...pd.GetRegionOption) (*pd.Region, error) {
			cntGetRegion++
			return cli.GetRegion(ctx, key, opts...)
		},
	}

	s.Run("WithDownPeers", func() {
		cntGetRegion = 0
		s.cache.clear()
		s.cluster.MarkPeerDown(s.peer2)

		for i := 0; i < 50; i++ {
			time.Sleep(100 * time.Millisecond)
			_, err := s.cache.LocateKey(s.bo, []byte("a"))
			s.NoError(err)
		}
		s.Equal(2, cntGetRegion, "should reload region with down peers every RegionCacheTTL")
	})

	s.Run("WithStaleStores", func() {
		cntGetRegion = 0
		s.cache.clear()
		store2 := s.cache.getStoreOrInsertDefault(s.store2)

		for i := 0; i < 50; i++ {
			atomic.StoreUint32(&store2.epoch, uint32(i))
			time.Sleep(100 * time.Millisecond)
			_, err := s.cache.LocateKey(s.bo, []byte("a"))
			s.NoError(err)
		}
		s.Equal(2, cntGetRegion, "should reload region with stale stores every RegionCacheTTL")
	})

	s.Run("WithUnreachableStores", func() {
		cntGetRegion = 0
		s.cache.clear()
		store2 := s.cache.getStoreOrInsertDefault(s.store2)
		atomic.StoreUint32(&store2.livenessState, uint32(unreachable))
		defer atomic.StoreUint32(&store2.livenessState, uint32(reachable))

		for i := 0; i < 50; i++ {
			time.Sleep(100 * time.Millisecond)
			_, err := s.cache.LocateKey(s.bo, []byte("a"))
			s.NoError(err)
		}
		s.Equal(2, cntGetRegion, "should reload region with unreachable stores every RegionCacheTTL")
	})
}

func (s *testRegionCacheSuite) TestTiFlashRecoveredFromDown() {
	s.onClosed = func() { SetRegionCacheTTLWithJitter(600, 60) }
	SetRegionCacheTTLWithJitter(3, 0)

	store3 := s.cluster.AllocID()
	peer3 := s.cluster.AllocID()
	s.cluster.AddStore(store3, s.storeAddr(store3))
	s.cluster.AddPeer(s.region1, store3, peer3)
	s.cluster.UpdateStoreAddr(store3, s.storeAddr(store3), &metapb.StoreLabel{Key: "engine", Value: "tiflash"})
	store4 := s.cluster.AllocID()
	peer4 := s.cluster.AllocID()
	s.cluster.AddStore(store4, s.storeAddr(store4))
	s.cluster.AddPeer(s.region1, store4, peer4)
	s.cluster.UpdateStoreAddr(store4, s.storeAddr(store4), &metapb.StoreLabel{Key: "engine", Value: "tiflash"})

	// load region to region cache with no down tiflash peer
	loc, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	s.Equal(loc.Region.id, s.region1)
	ctx, err := s.cache.GetTiFlashRPCContext(s.bo, loc.Region, true, LabelFilterNoTiFlashWriteNode)
	s.Nil(err)
	s.NotNil(ctx)
	region := s.cache.GetCachedRegionWithRLock(loc.Region)
	s.Equal(region.checkSyncFlags(needExpireAfterTTL), false)
	s.cache.clear()

	s.cluster.MarkPeerDown(peer3)
	loc, err = s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	s.Equal(loc.Region.id, s.region1)
	region = s.cache.GetCachedRegionWithRLock(loc.Region)
	s.Equal(region.checkSyncFlags(needExpireAfterTTL), true)

	for i := 0; i <= 3; i++ {
		time.Sleep(1 * time.Second)
		loc, err = s.cache.LocateKey(s.bo, []byte("a"))
		s.Nil(err)
		rpcCtx, err := s.cache.GetTiFlashRPCContext(s.bo, loc.Region, true, LabelFilterNoTiFlashWriteNode)
		s.Nil(err)
		if rpcCtx != nil {
			s.NotEqual(s.storeAddr(store3), rpcCtx.Addr, "should not access peer3 when it is down")
		}
	}
	newRegion := s.cache.GetCachedRegionWithRLock(loc.Region)
	s.NotNil(newRegion)
	s.NotEqual(region, newRegion)

	s.cluster.RemoveDownPeer(peer3)
	for i := 0; ; i++ {
		if i > 10 {
			s.Fail("should access peer3 after it is up")
			break
		}
		loc, err = s.cache.LocateKey(s.bo, []byte("a"))
		s.Nil(err)
		rpcCtx, err := s.cache.GetTiFlashRPCContext(s.bo, loc.Region, true, LabelFilterNoTiFlashWriteNode)
		s.Nil(err)
		if rpcCtx != nil && rpcCtx.Addr == s.storeAddr(store3) {
			break
		}
		time.Sleep(1 * time.Second)
	}
}

// TestFilterDownPeersOrPeersOnTombstoneOrDroppedStore verifies the RegionCache filter
// region's down peers and peers on tombstone or dropped stores. RegionCache shouldn't
// report errors in such cases if there are available peers.
func (s *testRegionCacheSuite) TestFilterDownPeersOrPeersOnTombstoneOrDroppedStores() {
	key := []byte("a")
	bo := retry.NewBackofferWithVars(context.Background(), 100, nil)

	verifyGetRPCCtx := func(meta *metapb.Region) {
		loc, err := s.cache.LocateKey(bo, key)
		s.NotNil(loc)
		s.Nil(err)
		ctx, err := s.cache.GetTiKVRPCContext(bo, loc.Region, kv.ReplicaReadLeader, 0)
		s.Nil(err)
		s.NotNil(ctx)
		s.Equal(ctx.Meta, meta)
		ctx, err = s.cache.GetTiKVRPCContext(bo, loc.Region, kv.ReplicaReadFollower, rand.Uint32())
		s.Nil(err)
		s.NotNil(ctx)
		s.Equal(ctx.Meta, meta)
	}

	// When all peers are normal, the cached region should contain all peers.
	reg, err := s.cache.findRegionByKey(bo, key, false)
	s.NotNil(reg)
	s.Nil(err)
	regInPD, _ := s.cluster.GetRegion(reg.GetID())
	s.Equal(reg.meta, regInPD)
	s.Equal(len(reg.meta.GetPeers()), len(reg.getStore().stores))
	verifyGetRPCCtx(reg.meta)
	s.checkCache(1)
	s.cache.clear()

	// Shouldn't contain the peer on the tombstone store.
	s.cluster.MarkTombstone(s.store1)
	reg, err = s.cache.findRegionByKey(bo, key, false)
	s.NotNil(reg)
	s.Nil(err)
	s.Equal(len(reg.meta.GetPeers()), len(regInPD.GetPeers())-1)
	s.Equal(len(reg.meta.GetPeers()), len(reg.getStore().stores))
	for _, peer := range reg.meta.GetPeers() {
		s.NotEqual(peer.GetStoreId(), s.store1)
	}
	for _, store := range reg.getStore().stores {
		s.NotEqual(store.storeID, s.store1)
	}
	verifyGetRPCCtx(reg.meta)
	s.checkCache(1)
	s.cache.clear()
	s.cluster.StartStore(s.store1)

	// Shouldn't contain the peer on the dropped store.
	store := s.cluster.GetStore(s.store1)
	s.cluster.RemoveStore(s.store1)
	reg, err = s.cache.findRegionByKey(bo, key, false)
	s.NotNil(reg)
	s.Nil(err)
	s.Equal(len(reg.meta.GetPeers()), len(regInPD.GetPeers())-1)
	s.Equal(len(reg.meta.GetPeers()), len(reg.getStore().stores))
	for _, peer := range reg.meta.GetPeers() {
		s.NotEqual(peer.GetStoreId(), s.store1)
	}
	for _, store := range reg.getStore().stores {
		s.NotEqual(store.storeID, s.store1)
	}
	verifyGetRPCCtx(reg.meta)
	s.checkCache(1)
	s.cache.clear()
	s.cluster.AddStore(store.GetId(), store.GetAddress(), store.GetLabels()...)

	// Report an error when there's no available peers.
	s.cluster.MarkTombstone(s.store1)
	s.cluster.MarkTombstone(s.store2)
	_, err = s.cache.findRegionByKey(bo, key, false)
	s.NotNil(err)
	s.Regexp(".*no available peers.", err.Error())
	s.cluster.StartStore(s.store1)
	s.cluster.StartStore(s.store2)
}

func (s *testRegionCacheSuite) TestUpdateLeader() {
	seed := rand.Uint32()
	loc, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	// tikv-server reports `NotLeader`
	s.cache.UpdateLeader(loc.Region, &metapb.Peer{Id: s.peer2, StoreId: s.store2}, 0)

	r := s.getRegion([]byte("a"))
	s.NotNil(r)
	s.Equal(r.GetID(), s.region1)
	s.Equal(s.getAddr([]byte("a"), kv.ReplicaReadLeader, 0), s.storeAddr(s.store2))
	s.Equal(s.getAddr([]byte("a"), kv.ReplicaReadFollower, seed), s.storeAddr(s.store1))

	r = s.getRegionWithEndKey([]byte("z"))
	s.NotNil(r)
	s.Equal(r.GetID(), s.region1)
	s.Equal(s.getAddr([]byte("z"), kv.ReplicaReadLeader, 0), s.storeAddr(s.store2))
	s.Equal(s.getAddr([]byte("a"), kv.ReplicaReadFollower, seed), s.storeAddr(s.store1))
}

func (s *testRegionCacheSuite) TestUpdateLeader2() {
	seed := rand.Uint32()
	loc, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	// new store3 becomes leader
	store3 := s.cluster.AllocID()
	peer3 := s.cluster.AllocID()
	s.cluster.AddStore(store3, s.storeAddr(store3))
	s.cluster.AddPeer(s.region1, store3, peer3)
	// tikv-server reports `NotLeader`
	s.cache.UpdateLeader(loc.Region, &metapb.Peer{Id: peer3, StoreId: store3}, 0)

	// Store3 does not exist in cache, causes a reload from PD.
	r := s.getRegion([]byte("a"))
	s.NotNil(r)
	s.Equal(r.GetID(), s.region1)
	s.Equal(s.getAddr([]byte("a"), kv.ReplicaReadLeader, 0), s.storeAddr(s.store1))
	follower := s.getAddr([]byte("a"), kv.ReplicaReadFollower, seed)
	if seed%2 == 0 {
		s.Equal(follower, s.storeAddr(s.store2))
	} else {
		s.Equal(follower, s.storeAddr(store3))
	}
	follower2 := s.getAddr([]byte("a"), kv.ReplicaReadFollower, seed+1)
	if (seed+1)%2 == 0 {
		s.Equal(follower2, s.storeAddr(s.store2))
	} else {
		s.Equal(follower2, s.storeAddr(store3))
	}
	s.NotEqual(follower, follower2)

	// tikv-server notifies new leader to pd-server.
	s.cluster.ChangeLeader(s.region1, peer3)
	// tikv-server reports `NotLeader` again.
	s.cache.UpdateLeader(r.VerID(), &metapb.Peer{Id: peer3, StoreId: store3}, 0)
	r = s.getRegion([]byte("a"))
	s.NotNil(r)
	s.Equal(r.GetID(), s.region1)
	s.Equal(s.getAddr([]byte("a"), kv.ReplicaReadLeader, 0), s.storeAddr(store3))
	follower = s.getAddr([]byte("a"), kv.ReplicaReadFollower, seed)
	if seed%2 == 0 {
		s.Equal(follower, s.storeAddr(s.store1))
	} else {
		s.Equal(follower, s.storeAddr(s.store2))
	}
	follower2 = s.getAddr([]byte("a"), kv.ReplicaReadFollower, seed+1)
	if (seed+1)%2 == 0 {
		s.Equal(follower2, s.storeAddr(s.store1))
	} else {
		s.Equal(follower2, s.storeAddr(s.store2))
	}
	s.NotEqual(follower, follower2)
}

func (s *testRegionCacheSuite) TestUpdateLeader3() {
	seed := rand.Uint32()
	loc, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	// store2 becomes leader
	s.cluster.ChangeLeader(s.region1, s.peer2)
	// store2 gone, store3 becomes leader
	s.cluster.RemoveStore(s.store2)
	store3 := s.cluster.AllocID()
	peer3 := s.cluster.AllocID()
	s.cluster.AddStore(store3, s.storeAddr(store3))
	s.cluster.AddPeer(s.region1, store3, peer3)
	// tikv-server notifies new leader to pd-server.
	s.cluster.ChangeLeader(s.region1, peer3)
	// tikv-server reports `NotLeader`(store2 is the leader)
	s.cache.UpdateLeader(loc.Region, &metapb.Peer{Id: s.peer2, StoreId: s.store2}, 0)

	// Store2 does not exist any more, causes a reload from PD.
	r := s.getRegion([]byte("a"))
	s.Nil(err)
	s.NotNil(r)
	s.Equal(r.GetID(), s.region1)
	loc, err = s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	// return resolved store2 address and send fail
	ctx, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, seed)
	s.Nil(err)
	s.Equal(ctx.Addr, "store2")
	s.cache.OnSendFail(retry.NewNoopBackoff(context.Background()), ctx, false, errors.New("send fail"))
	s.cache.checkAndResolve(nil, func(*Store) bool { return true })
	s.cache.UpdateLeader(loc.Region, &metapb.Peer{Id: s.peer2, StoreId: s.store2}, 0)
	addr := s.getAddr([]byte("a"), kv.ReplicaReadLeader, 0)
	s.Equal(addr, "")
	addr = s.getAddr([]byte("a"), kv.ReplicaReadLeader, 0)
	s.Equal(addr, s.storeAddr(store3))

	addr = s.getAddr([]byte("a"), kv.ReplicaReadFollower, seed)
	addr2 := s.getAddr([]byte("a"), kv.ReplicaReadFollower, seed+1)
	s.NotEqual(addr, s.storeAddr(store3))
	s.NotEqual(addr2, s.storeAddr(store3))
}

func (s *testRegionCacheSuite) TestSendFailedButLeaderNotChange() {
	// 3 nodes and no.1 is leader.
	store3 := s.cluster.AllocID()
	peer3 := s.cluster.AllocID()
	s.cluster.AddStore(store3, s.storeAddr(store3))
	s.cluster.AddPeer(s.region1, store3, peer3)
	s.cluster.ChangeLeader(s.region1, s.peer1)

	loc, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	ctx, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, 0)
	s.Nil(err)
	s.Equal(ctx.Peer.Id, s.peer1)
	s.Equal(len(ctx.Meta.Peers), 3)

	// verify follower to be one of store2 and store3
	seed := rand.Uint32()
	ctxFollower1, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed)
	s.Nil(err)
	if seed%2 == 0 {
		s.Equal(ctxFollower1.Peer.Id, s.peer2)
	} else {
		s.Equal(ctxFollower1.Peer.Id, peer3)
	}
	ctxFollower2, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed)
	s.Nil(err)
	if seed%2 == 0 {
		s.Equal(ctxFollower2.Peer.Id, s.peer2)
	} else {
		s.Equal(ctxFollower2.Peer.Id, peer3)
	}
	s.Equal(ctxFollower1.Peer.Id, ctxFollower2.Peer.Id)

	// send fail leader switch to 2
	s.cache.OnSendFail(s.bo, ctx, false, nil)
	ctx, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, 0)
	s.Nil(err)
	s.Equal(ctx.Peer.Id, s.peer2)

	// verify follower to be one of store1 and store3
	ctxFollower1, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed)
	s.Nil(err)
	if seed%2 == 0 {
		s.Equal(ctxFollower1.Peer.Id, s.peer1)
	} else {
		s.Equal(ctxFollower1.Peer.Id, peer3)
	}
	ctxFollower2, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed+1)
	s.Nil(err)
	if (seed+1)%2 == 0 {
		s.Equal(ctxFollower2.Peer.Id, s.peer1)
	} else {
		s.Equal(ctxFollower2.Peer.Id, peer3)
	}
	s.NotEqual(ctxFollower1.Peer.Id, ctxFollower2.Peer.Id)

	// access 1 it will return NotLeader, leader back to 2 again
	s.cache.UpdateLeader(loc.Region, &metapb.Peer{Id: s.peer2, StoreId: s.store2}, ctx.AccessIdx)
	ctx, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, 0)
	s.Nil(err)
	s.Equal(ctx.Peer.Id, s.peer2)

	// verify follower to be one of store1 and store3
	ctxFollower1, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed)
	s.Nil(err)
	if seed%2 == 0 {
		s.Equal(ctxFollower1.Peer.Id, s.peer1)
	} else {
		s.Equal(ctxFollower1.Peer.Id, peer3)
	}
	ctxFollower2, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed+1)
	s.Nil(err)
	if (seed+1)%2 == 0 {
		s.Equal(ctxFollower2.Peer.Id, s.peer1)
	} else {
		s.Equal(ctxFollower2.Peer.Id, peer3)
	}
	s.NotEqual(ctxFollower1.Peer.Id, ctxFollower2.Peer.Id)
}

func (s *testRegionCacheSuite) TestSendFailedInHibernateRegion() {
	// 3 nodes and no.1 is leader.
	store3 := s.cluster.AllocID()
	peer3 := s.cluster.AllocID()
	s.cluster.AddStore(store3, s.storeAddr(store3))
	s.cluster.AddPeer(s.region1, store3, peer3)
	s.cluster.ChangeLeader(s.region1, s.peer1)

	loc, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	ctx, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, 0)
	s.Nil(err)
	s.Equal(ctx.Peer.Id, s.peer1)
	s.Equal(len(ctx.Meta.Peers), 3)

	// verify follower to be one of store2 and store3
	seed := rand.Uint32()
	ctxFollower1, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed)
	s.Nil(err)
	if seed%2 == 0 {
		s.Equal(ctxFollower1.Peer.Id, s.peer2)
	} else {
		s.Equal(ctxFollower1.Peer.Id, peer3)
	}
	ctxFollower2, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed)
	s.Nil(err)
	if seed%2 == 0 {
		s.Equal(ctxFollower2.Peer.Id, s.peer2)
	} else {
		s.Equal(ctxFollower2.Peer.Id, peer3)
	}
	s.Equal(ctxFollower1.Peer.Id, ctxFollower2.Peer.Id)

	// send fail leader switch to 2
	s.cache.OnSendFail(s.bo, ctx, false, nil)
	ctx, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, 0)
	s.Nil(err)
	s.Equal(ctx.Peer.Id, s.peer2)

	// verify follower to be one of store1 and store3
	ctxFollower1, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed)
	s.Nil(err)
	if seed%2 == 0 {
		s.Equal(ctxFollower1.Peer.Id, s.peer1)
	} else {
		s.Equal(ctxFollower1.Peer.Id, peer3)
	}
	s.True(ctxFollower1.Peer.Id == s.peer1 || ctxFollower1.Peer.Id == peer3)
	ctxFollower2, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed+1)
	s.Nil(err)
	if (seed+1)%2 == 0 {
		s.Equal(ctxFollower2.Peer.Id, s.peer1)
	} else {
		s.Equal(ctxFollower2.Peer.Id, peer3)
	}
	s.NotEqual(ctxFollower1.Peer.Id, ctxFollower2.Peer.Id)

	// access 2, it's in hibernate and return 0 leader, so switch to 3
	s.cache.UpdateLeader(loc.Region, nil, ctx.AccessIdx)
	ctx, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, 0)
	s.Nil(err)
	s.Equal(ctx.Peer.Id, peer3)

	// verify follower to be one of store1 and store2
	ctxFollower1, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed)
	s.Nil(err)
	if seed%2 == 0 {
		s.Equal(ctxFollower1.Peer.Id, s.peer1)
	} else {
		s.Equal(ctxFollower1.Peer.Id, s.peer2)
	}
	ctxFollower2, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed)
	s.Nil(err)
	if seed%2 == 0 {
		s.Equal(ctxFollower2.Peer.Id, s.peer1)
	} else {
		s.Equal(ctxFollower2.Peer.Id, s.peer2)
	}
	s.Equal(ctxFollower1.Peer.Id, ctxFollower2.Peer.Id)

	// again peer back to 1
	ctx, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, 0)
	s.Nil(err)
	s.cache.UpdateLeader(loc.Region, nil, ctx.AccessIdx)
	ctx, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, 0)
	s.Nil(err)
	s.Equal(ctx.Peer.Id, s.peer1)

	// verify follower to be one of store2 and store3
	ctxFollower1, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed)
	s.Nil(err)
	if seed%2 == 0 {
		s.Equal(ctxFollower1.Peer.Id, s.peer2)
	} else {
		s.Equal(ctxFollower1.Peer.Id, peer3)
	}
	ctxFollower2, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed+1)
	s.Nil(err)
	if (seed+1)%2 == 0 {
		s.Equal(ctxFollower2.Peer.Id, s.peer2)
	} else {
		s.Equal(ctxFollower2.Peer.Id, peer3)
	}
	s.NotEqual(ctxFollower1.Peer.Id, ctxFollower2.Peer.Id)
}

func (s *testRegionCacheSuite) TestSendFailInvalidateRegionsInSameStore() {
	// key range: ['' - 'm' - 'z']
	region2 := s.cluster.AllocID()
	newPeers := s.cluster.AllocIDs(2)
	s.cluster.Split(s.region1, region2, []byte("m"), newPeers, newPeers[0])

	// Check the two regions.
	loc1, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	s.Equal(loc1.Region.id, s.region1)
	loc2, err := s.cache.LocateKey(s.bo, []byte("x"))
	s.Nil(err)
	s.Equal(loc2.Region.id, region2)

	// Send fail on region1
	ctx, _ := s.cache.GetTiKVRPCContext(s.bo, loc1.Region, kv.ReplicaReadLeader, 0)
	s.checkCache(2)
	s.cache.OnSendFail(s.bo, ctx, false, errors.New("test error"))

	// Get region2 cache will get nil then reload.
	ctx2, err := s.cache.GetTiKVRPCContext(s.bo, loc2.Region, kv.ReplicaReadLeader, 0)
	s.Nil(ctx2)
	s.Nil(err)
}

func (s *testRegionCacheSuite) TestSendFailedInMultipleNode() {
	// 3 nodes and no.1 is leader.
	store3 := s.cluster.AllocID()
	peer3 := s.cluster.AllocID()
	s.cluster.AddStore(store3, s.storeAddr(store3))
	s.cluster.AddPeer(s.region1, store3, peer3)
	s.cluster.ChangeLeader(s.region1, s.peer1)

	loc, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	ctx, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, 0)
	s.Nil(err)
	s.Equal(ctx.Peer.Id, s.peer1)
	s.Equal(len(ctx.Meta.Peers), 3)

	// verify follower to be one of store2 and store3
	seed := rand.Uint32()
	ctxFollower1, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed)
	s.Nil(err)
	if seed%2 == 0 {
		s.Equal(ctxFollower1.Peer.Id, s.peer2)
	} else {
		s.Equal(ctxFollower1.Peer.Id, peer3)
	}
	ctxFollower2, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed)
	s.Nil(err)
	if seed%2 == 0 {
		s.Equal(ctxFollower2.Peer.Id, s.peer2)
	} else {
		s.Equal(ctxFollower2.Peer.Id, peer3)
	}
	s.Equal(ctxFollower1.Peer.Id, ctxFollower2.Peer.Id)

	// send fail leader switch to 2
	s.cache.OnSendFail(s.bo, ctx, false, nil)
	ctx, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, 0)
	s.Nil(err)
	s.Equal(ctx.Peer.Id, s.peer2)

	// verify follower to be one of store1 and store3
	ctxFollower1, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed)
	s.Nil(err)
	if seed%2 == 0 {
		s.Equal(ctxFollower1.Peer.Id, s.peer1)
	} else {
		s.Equal(ctxFollower1.Peer.Id, peer3)
	}
	ctxFollower2, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed+1)
	s.Nil(err)
	if (seed+1)%2 == 0 {
		s.Equal(ctxFollower2.Peer.Id, s.peer1)
	} else {
		s.Equal(ctxFollower2.Peer.Id, peer3)
	}
	s.NotEqual(ctxFollower1.Peer.Id, ctxFollower2.Peer.Id)

	// send 2 fail leader switch to 3
	s.cache.OnSendFail(s.bo, ctx, false, nil)
	ctx, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, 0)
	s.Nil(err)
	s.Equal(ctx.Peer.Id, peer3)

	// verify follower to be one of store1 and store2
	ctxFollower1, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed)
	s.Nil(err)
	if seed%2 == 0 {
		s.Equal(ctxFollower1.Peer.Id, s.peer1)
	} else {
		s.Equal(ctxFollower1.Peer.Id, s.peer2)
	}
	s.True(ctxFollower1.Peer.Id == s.peer1 || ctxFollower1.Peer.Id == s.peer2)
	ctxFollower2, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed)
	s.Nil(err)
	if seed%2 == 0 {
		s.Equal(ctxFollower2.Peer.Id, s.peer1)
	} else {
		s.Equal(ctxFollower2.Peer.Id, s.peer2)
	}
	s.Equal(ctxFollower1.Peer.Id, ctxFollower2.Peer.Id)

	// 3 can be access, so switch to 1
	s.cache.UpdateLeader(loc.Region, &metapb.Peer{Id: s.peer1, StoreId: s.store1}, ctx.AccessIdx)
	ctx, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, 0)
	s.Nil(err)
	s.Equal(ctx.Peer.Id, s.peer1)

	// verify follower to be one of store2 and store3
	ctxFollower1, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed)
	s.Nil(err)
	if seed%2 == 0 {
		s.Equal(ctxFollower1.Peer.Id, s.peer2)
	} else {
		s.Equal(ctxFollower1.Peer.Id, peer3)
	}
	ctxFollower2, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed+1)
	s.Nil(err)
	if (seed+1)%2 == 0 {
		s.Equal(ctxFollower2.Peer.Id, s.peer2)
	} else {
		s.Equal(ctxFollower2.Peer.Id, peer3)
	}
	s.NotEqual(ctxFollower1.Peer.Id, ctxFollower2.Peer.Id)
}

func (s *testRegionCacheSuite) TestLabelSelectorTiKVPeer() {
	dc1Label := []*metapb.StoreLabel{
		{
			Key:   "zone",
			Value: "dc-1",
		},
	}
	dc2Label := []*metapb.StoreLabel{
		{
			Key:   "zone",
			Value: "dc-2",
		},
	}
	dc3Label := []*metapb.StoreLabel{
		{
			Key:   "zone",
			Value: "dc-3",
		},
	}
	s.cluster.UpdateStoreLabels(s.store1, dc1Label)
	s.cluster.UpdateStoreLabels(s.store2, dc2Label)

	store3 := s.cluster.AllocID()
	peer3 := s.cluster.AllocID()
	s.cluster.AddStore(store3, s.storeAddr(store3))
	s.cluster.AddPeer(s.region1, store3, peer3)
	s.cluster.UpdateStoreLabels(store3, dc1Label)
	// Region have 3 peer, leader located in dc-1, followers located in dc-1, dc-2
	loc, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	seed := rand.Uint32()

	testcases := []struct {
		name               string
		t                  kv.ReplicaReadType
		labels             []*metapb.StoreLabel
		expectStoreIDRange map[uint64]struct{}
	}{
		{
			name:   "any Peer,located in dc-1",
			t:      kv.ReplicaReadMixed,
			labels: dc1Label,
			expectStoreIDRange: map[uint64]struct{}{
				s.store1: {},
				store3:   {},
			},
		},
		{
			name:   "any Peer,located in dc-2",
			t:      kv.ReplicaReadMixed,
			labels: dc2Label,
			expectStoreIDRange: map[uint64]struct{}{
				s.store2: {},
			},
		},
		{
			name:   "only follower,located in dc-1",
			t:      kv.ReplicaReadFollower,
			labels: dc1Label,
			expectStoreIDRange: map[uint64]struct{}{
				store3: {},
			},
		},
		{
			name:   "only leader, shouldn't consider labels",
			t:      kv.ReplicaReadLeader,
			labels: dc2Label,
			expectStoreIDRange: map[uint64]struct{}{
				s.store1: {},
			},
		},
		{
			name:   "no label matching, fallback to leader",
			t:      kv.ReplicaReadMixed,
			labels: dc3Label,
			expectStoreIDRange: map[uint64]struct{}{
				s.store1: {},
			},
		},
	}

	for _, testcase := range testcases {
		s.T().Log(testcase.name)
		ctx, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, testcase.t, seed, WithMatchLabels(testcase.labels))
		s.Nil(err)
		_, exist := testcase.expectStoreIDRange[ctx.Store.storeID]
		s.Equal(exist, true)
	}
}

func (s *testRegionCacheSuite) TestSplit() {
	seed := rand.Uint32()
	r := s.getRegion([]byte("x"))
	s.Equal(r.GetID(), s.region1)
	s.Equal(s.getAddr([]byte("x"), kv.ReplicaReadLeader, 0), s.storeAddr(s.store1))
	s.Equal(s.getAddr([]byte("x"), kv.ReplicaReadFollower, seed), s.storeAddr(s.store2))

	// split to ['' - 'm' - 'z']
	region2 := s.cluster.AllocID()
	newPeers := s.cluster.AllocIDs(2)
	s.cluster.Split(s.region1, region2, []byte("m"), newPeers, newPeers[0])

	// tikv-server reports `NotInRegion`
	s.cache.InvalidateCachedRegion(r.VerID())
	s.checkCache(0)

	r = s.getRegion([]byte("x"))
	s.Equal(r.GetID(), region2)
	s.Equal(s.getAddr([]byte("x"), kv.ReplicaReadLeader, 0), s.storeAddr(s.store1))
	s.Equal(s.getAddr([]byte("x"), kv.ReplicaReadFollower, seed), s.storeAddr(s.store2))
	s.checkCache(1)

	r = s.getRegionWithEndKey([]byte("m"))
	s.Equal(r.GetID(), s.region1)
	s.checkCache(2)
}

func (s *testRegionCacheSuite) TestMerge() {
	// key range: ['' - 'm' - 'z']
	region2 := s.cluster.AllocID()
	newPeers := s.cluster.AllocIDs(2)
	s.cluster.Split(s.region1, region2, []byte("m"), newPeers, newPeers[0])

	loc, err := s.cache.LocateKey(s.bo, []byte("x"))
	s.Nil(err)
	s.Equal(loc.Region.id, region2)

	// merge to single region
	s.cluster.Merge(s.region1, region2)

	// tikv-server reports `NotInRegion`
	s.cache.InvalidateCachedRegion(loc.Region)
	s.checkCache(0)

	loc, err = s.cache.LocateKey(s.bo, []byte("x"))
	s.Nil(err)
	s.Equal(loc.Region.id, s.region1)
	s.checkCache(1)
}

func (s *testRegionCacheSuite) TestReconnect() {
	seed := rand.Uint32()
	loc, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)

	// connect tikv-server failed, cause drop cache
	s.cache.InvalidateCachedRegion(loc.Region)

	r := s.getRegion([]byte("a"))
	s.NotNil(r)
	s.Equal(r.GetID(), s.region1)
	s.Equal(s.getAddr([]byte("a"), kv.ReplicaReadLeader, 0), s.storeAddr(s.store1))
	s.Equal(s.getAddr([]byte("x"), kv.ReplicaReadFollower, seed), s.storeAddr(s.store2))
	s.checkCache(1)
}

func (s *testRegionCacheSuite) TestRegionEpochAheadOfTiKV() {
	// Create a separated region cache to do this test.
	pdCli := &CodecPDClient{mocktikv.NewPDClient(s.cluster), apicodec.NewCodecV1(apicodec.ModeTxn)}
	cache := NewRegionCache(pdCli)
	defer cache.Close()

	region := createSampleRegion([]byte("k1"), []byte("k2"))
	region.meta.Id = 1
	region.meta.RegionEpoch = &metapb.RegionEpoch{Version: 10, ConfVer: 10}
	cache.insertRegionToCache(region, true, true)

	r1 := metapb.Region{Id: 1, RegionEpoch: &metapb.RegionEpoch{Version: 9, ConfVer: 10}}
	r2 := metapb.Region{Id: 1, RegionEpoch: &metapb.RegionEpoch{Version: 10, ConfVer: 9}}

	bo := retry.NewBackofferWithVars(context.Background(), 2000000, nil)

	_, err := cache.OnRegionEpochNotMatch(bo, &RPCContext{Region: region.VerID()}, []*metapb.Region{&r1})
	s.Nil(err)
	_, err = cache.OnRegionEpochNotMatch(bo, &RPCContext{Region: region.VerID()}, []*metapb.Region{&r2})
	s.Nil(err)
	s.Equal(bo.ErrorsNum(), 2)
}

func (s *testRegionCacheSuite) TestRegionEpochOnTiFlash() {
	// add store3 as tiflash
	store3 := s.cluster.AllocID()
	peer3 := s.cluster.AllocID()
	s.cluster.UpdateStoreAddr(s.store1, s.storeAddr(s.store1), &metapb.StoreLabel{Key: "engine", Value: "tiflash"})
	s.cluster.AddStore(store3, s.storeAddr(store3))
	s.cluster.AddPeer(s.region1, store3, peer3)
	s.cluster.ChangeLeader(s.region1, peer3)

	// pre-load region cache
	loc1, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	s.Equal(loc1.Region.id, s.region1)
	lctx, err := s.cache.GetTiKVRPCContext(s.bo, loc1.Region, kv.ReplicaReadLeader, 0)
	s.Nil(err)
	s.Equal(lctx.Peer.Id, peer3)

	// epoch-not-match on tiflash
	ctxTiFlash, err := s.cache.GetTiFlashRPCContext(s.bo, loc1.Region, true, LabelFilterNoTiFlashWriteNode)
	s.Nil(err)
	s.Equal(ctxTiFlash.Peer.Id, s.peer1)
	ctxTiFlash.Peer.Role = metapb.PeerRole_Learner
	r := ctxTiFlash.Meta
	reqSend := NewRegionRequestSender(s.cache, nil)
	regionErr := &errorpb.Error{EpochNotMatch: &errorpb.EpochNotMatch{CurrentRegions: []*metapb.Region{r}}}
	reqSend.onRegionError(s.bo, ctxTiFlash, nil, regionErr)

	// check leader read should not go to tiflash
	lctx, err = s.cache.GetTiKVRPCContext(s.bo, loc1.Region, kv.ReplicaReadLeader, 0)
	s.Nil(err)
	s.NotEqual(lctx.Peer.Id, s.peer1)
}

const regionSplitKeyFormat = "t%08d"

func createClusterWithStoresAndRegions(regionCnt, storeCount int) *mocktikv.Cluster {
	cluster := mocktikv.NewCluster(mocktikv.MustNewMVCCStore())
	_, _, regionID, _ := mocktikv.BootstrapWithMultiStores(cluster, storeCount)
	for i := 0; i < regionCnt; i++ {
		rawKey := []byte(fmt.Sprintf(regionSplitKeyFormat, i))
		ids := cluster.AllocIDs(4)
		// Make leaders equally distributed on the 3 stores.
		storeID := ids[0]
		peerIDs := ids[1:]
		leaderPeerID := peerIDs[i%3]
		cluster.SplitRaw(regionID, storeID, rawKey, peerIDs, leaderPeerID)
		regionID = ids[0]
	}
	return cluster
}

func loadRegionsToCache(cache *RegionCache, regionCnt int) {
	for i := 0; i < regionCnt; i++ {
		rawKey := []byte(fmt.Sprintf(regionSplitKeyFormat, i))
		cache.LocateKey(retry.NewBackofferWithVars(context.Background(), 1, nil), rawKey)
	}
}

func (s *testRegionCacheSuite) TestListRegionIDsInCache() {
	// ['' - 'm' - 'z']
	region2 := s.cluster.AllocID()
	newPeers := s.cluster.AllocIDs(2)
	s.cluster.Split(s.region1, region2, []byte("m"), newPeers, newPeers[0])

	regionIDs, err := s.cache.ListRegionIDsInKeyRange(s.bo, []byte("a"), []byte("z"))
	s.Nil(err)
	s.Equal(regionIDs, []uint64{s.region1, region2})
	regionIDs, err = s.cache.ListRegionIDsInKeyRange(s.bo, []byte("m"), []byte("z"))
	s.Nil(err)
	s.Equal(regionIDs, []uint64{region2})

	regionIDs, err = s.cache.ListRegionIDsInKeyRange(s.bo, []byte("a"), []byte("m"))
	s.Nil(err)
	s.Equal(regionIDs, []uint64{s.region1, region2})
}

func (s *testRegionCacheSuite) TestScanRegions() {
	// Split at "a", "b", "c", "d"
	regions := s.cluster.AllocIDs(4)
	regions = append([]uint64{s.region1}, regions...)

	peers := [][]uint64{{s.peer1, s.peer2}}
	for i := 0; i < 4; i++ {
		peers = append(peers, s.cluster.AllocIDs(2))
	}

	for i := 0; i < 4; i++ {
		s.cluster.Split(regions[i], regions[i+1], []byte{'a' + byte(i)}, peers[i+1], peers[i+1][0])
	}

	scannedRegions, err := s.cache.scanRegions(s.bo, []byte(""), nil, 100)
	s.Nil(err)
	s.Equal(len(scannedRegions), 5)
	for i := 0; i < 5; i++ {
		r := scannedRegions[i]
		_, p, _, _ := r.WorkStorePeer(r.getStore())

		s.Equal(r.meta.Id, regions[i])
		s.Equal(p.Id, peers[i][0])
	}

	scannedRegions, err = s.cache.scanRegions(s.bo, []byte("a"), nil, 3)
	s.Nil(err)
	s.Equal(len(scannedRegions), 3)
	for i := 1; i < 4; i++ {
		r := scannedRegions[i-1]
		_, p, _, _ := r.WorkStorePeer(r.getStore())

		s.Equal(r.meta.Id, regions[i])
		s.Equal(p.Id, peers[i][0])
	}

	scannedRegions, err = s.cache.scanRegions(s.bo, []byte("a1"), nil, 1)
	s.Nil(err)
	s.Equal(len(scannedRegions), 1)

	r0 := scannedRegions[0]
	_, p0, _, _ := r0.WorkStorePeer(r0.getStore())
	s.Equal(r0.meta.Id, regions[1])
	s.Equal(p0.Id, peers[1][0])

	// Test region with no leader
	s.cluster.GiveUpLeader(regions[1])
	s.cluster.GiveUpLeader(regions[3])
	scannedRegions, err = s.cache.scanRegions(s.bo, []byte(""), nil, 5)
	s.Nil(err)
	for i := 0; i < 3; i++ {
		r := scannedRegions[i]
		_, p, _, _ := r.WorkStorePeer(r.getStore())

		s.Equal(r.meta.Id, regions[i*2])
		s.Equal(p.Id, peers[i*2][0])
	}
}

func (s *testRegionCacheSuite) TestBatchLoadRegions() {
	// Split at "a", "b", "c", "d"
	regions := s.cluster.AllocIDs(4)
	regions = append([]uint64{s.region1}, regions...)

	peers := [][]uint64{{s.peer1, s.peer2}}
	for i := 0; i < 4; i++ {
		peers = append(peers, s.cluster.AllocIDs(2))
	}

	for i := 0; i < 4; i++ {
		s.cluster.Split(regions[i], regions[i+1], []byte{'a' + byte(i)}, peers[i+1], peers[i+1][0])
	}

	testCases := []struct {
		startKey      []byte
		endKey        []byte
		limit         int
		expectKey     []byte
		expectRegions []uint64
	}{
		{[]byte(""), []byte("a"), 1, []byte("a"), []uint64{regions[0]}},
		{[]byte("a"), []byte("b1"), 2, []byte("c"), []uint64{regions[1], regions[2]}},
		{[]byte("a1"), []byte("d"), 2, []byte("c"), []uint64{regions[1], regions[2]}},
		{[]byte("c"), []byte("c1"), 2, nil, []uint64{regions[3]}},
		{[]byte("d"), nil, 2, nil, []uint64{regions[4]}},
	}

	for _, tc := range testCases {
		key, err := s.cache.BatchLoadRegionsFromKey(s.bo, tc.startKey, tc.limit)
		s.Nil(err)
		if tc.expectKey != nil {
			s.Equal(key, tc.expectKey)
		} else {
			s.Len(key, 0)
		}
		loadRegions, err := s.cache.BatchLoadRegionsWithKeyRange(s.bo, tc.startKey, tc.endKey, tc.limit)
		s.Nil(err)
		s.Len(loadRegions, len(tc.expectRegions))
		for i := range loadRegions {
			s.Equal(loadRegions[i].GetID(), tc.expectRegions[i])
		}
	}

	s.checkCache(len(regions))
}

func (s *testRegionCacheSuite) TestFollowerReadFallback() {
	// 3 nodes and no.1 is leader.
	store3 := s.cluster.AllocID()
	peer3 := s.cluster.AllocID()
	s.cluster.AddStore(store3, s.storeAddr(store3))
	s.cluster.AddPeer(s.region1, store3, peer3)
	s.cluster.ChangeLeader(s.region1, s.peer1)

	loc, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	ctx, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, 0)
	s.Nil(err)
	s.Equal(ctx.Peer.Id, s.peer1)
	s.Equal(len(ctx.Meta.Peers), 3)

	// verify follower to be store2 and store3
	ctxFollower1, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, 0)
	s.Nil(err)
	s.Equal(ctxFollower1.Peer.Id, s.peer2)
	ctxFollower2, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, 1)
	s.Nil(err)
	s.Equal(ctxFollower2.Peer.Id, peer3)
	s.NotEqual(ctxFollower1.Peer.Id, ctxFollower2.Peer.Id)

	// send fail on store2, next follower read is going to fallback to store3
	s.cache.OnSendFail(s.bo, ctxFollower1, false, errors.New("test error"))
	ctx, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, 0)
	s.Nil(err)
	s.Equal(ctx.Peer.Id, peer3)
}

func (s *testRegionCacheSuite) TestMixedReadFallback() {
	// 3 nodes and no.1 is leader.
	store3 := s.cluster.AllocID()
	peer3 := s.cluster.AllocID()
	s.cluster.AddStore(store3, s.storeAddr(store3))
	s.cluster.AddPeer(s.region1, store3, peer3)
	s.cluster.ChangeLeader(s.region1, s.peer1)

	loc, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	ctx, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, 0)
	s.Nil(err)
	s.Equal(ctx.Peer.Id, s.peer1)
	s.Equal(len(ctx.Meta.Peers), 3)

	// verify follower to be store1, store2 and store3
	ctxFollower1, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadMixed, 0)
	s.Nil(err)
	s.Equal(ctxFollower1.Peer.Id, s.peer1)

	ctxFollower2, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadMixed, 1)
	s.Nil(err)
	s.Equal(ctxFollower2.Peer.Id, s.peer2)

	ctxFollower3, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadMixed, 2)
	s.Nil(err)
	s.Equal(ctxFollower3.Peer.Id, peer3)

	// send fail on store2, next follower read is going to fallback to store3
	s.cache.OnSendFail(s.bo, ctxFollower1, false, errors.New("test error"))
	ctx, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadMixed, 0)
	s.Nil(err)
	s.Equal(ctx.Peer.Id, s.peer2)
}

func (s *testRegionCacheSuite) TestPeersLenChange() {
	// 2 peers [peer1, peer2] and let peer2 become leader
	loc, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	s.cache.UpdateLeader(loc.Region, &metapb.Peer{Id: s.peer2, StoreId: s.store2}, 0)

	// current leader is peer2 in [peer1, peer2]
	loc, err = s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	ctx, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, 0)
	s.Nil(err)
	s.Equal(ctx.Peer.StoreId, s.store2)

	// simulate peer1 became down in kv heartbeat and loaded before response back.
	cpMeta := &metapb.Region{
		Id:          ctx.Meta.Id,
		StartKey:    ctx.Meta.StartKey,
		EndKey:      ctx.Meta.EndKey,
		RegionEpoch: ctx.Meta.RegionEpoch,
		Peers:       make([]*metapb.Peer, len(ctx.Meta.Peers)),
	}
	copy(cpMeta.Peers, ctx.Meta.Peers)
	cpRegion := &pd.Region{
		Meta:      cpMeta,
		DownPeers: []*metapb.Peer{{Id: s.peer1, StoreId: s.store1}},
	}
	region, err := newRegion(s.bo, s.cache, cpRegion)
	s.Nil(err)
	s.cache.insertRegionToCache(region, true, true)

	// OnSendFail should not panic
	s.cache.OnSendFail(retry.NewNoopBackoff(context.Background()), ctx, false, errors.New("send fail"))
}

func (s *testRegionCacheSuite) TestPeersLenChangedByWitness() {
	// 2 peers [peer1, peer2] and let peer2 become leader
	loc, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	s.cache.UpdateLeader(loc.Region, &metapb.Peer{Id: s.peer2, StoreId: s.store2}, 0)

	// current leader is peer2 in [peer1, peer2]
	loc, err = s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	ctx, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, 0)
	s.Nil(err)
	s.Equal(ctx.Peer.StoreId, s.store2)

	// simulate peer1 become witness in kv heartbeat and loaded before response back.
	cpMeta := &metapb.Region{
		Id:          ctx.Meta.Id,
		StartKey:    ctx.Meta.StartKey,
		EndKey:      ctx.Meta.EndKey,
		RegionEpoch: ctx.Meta.RegionEpoch,
		Peers:       make([]*metapb.Peer, len(ctx.Meta.Peers)),
	}
	copy(cpMeta.Peers, ctx.Meta.Peers)
	for _, peer := range cpMeta.Peers {
		if peer.Id == s.peer1 {
			peer.IsWitness = true
		}
	}
	cpRegion := &pd.Region{Meta: cpMeta}
	region, err := newRegion(s.bo, s.cache, cpRegion)
	s.Nil(err)
	s.cache.insertRegionToCache(region, true, true)

	// OnSendFail should not panic
	s.cache.OnSendFail(retry.NewNoopBackoff(context.Background()), ctx, false, errors.New("send fail"))
}

func createSampleRegion(startKey, endKey []byte) *Region {
	return &Region{
		meta: &metapb.Region{
			StartKey: startKey,
			EndKey:   endKey,
		},
	}
}

func (s *testRegionCacheSuite) TestContains() {
	s.True(createSampleRegion(nil, nil).Contains([]byte{}))
	s.True(createSampleRegion(nil, nil).Contains([]byte{10}))
	s.False(createSampleRegion([]byte{10}, nil).Contains([]byte{}))
	s.False(createSampleRegion([]byte{10}, nil).Contains([]byte{9}))
	s.True(createSampleRegion([]byte{10}, nil).Contains([]byte{10}))
	s.True(createSampleRegion(nil, []byte{10}).Contains([]byte{}))
	s.True(createSampleRegion(nil, []byte{10}).Contains([]byte{9}))
	s.False(createSampleRegion(nil, []byte{10}).Contains([]byte{10}))
	s.False(createSampleRegion([]byte{10}, []byte{20}).Contains([]byte{}))
	s.True(createSampleRegion([]byte{10}, []byte{20}).Contains([]byte{15}))
	s.False(createSampleRegion([]byte{10}, []byte{20}).Contains([]byte{30}))
}

func (s *testRegionCacheSuite) TestContainsByEnd() {
	s.True(createSampleRegion(nil, nil).ContainsByEnd([]byte{}))
	s.True(createSampleRegion(nil, nil).ContainsByEnd([]byte{10}))
	s.True(createSampleRegion([]byte{10}, nil).ContainsByEnd([]byte{}))
	s.False(createSampleRegion([]byte{10}, nil).ContainsByEnd([]byte{10}))
	s.True(createSampleRegion([]byte{10}, nil).ContainsByEnd([]byte{11}))
	s.False(createSampleRegion(nil, []byte{10}).ContainsByEnd([]byte{}))
	s.True(createSampleRegion(nil, []byte{10}).ContainsByEnd([]byte{10}))
	s.False(createSampleRegion(nil, []byte{10}).ContainsByEnd([]byte{11}))
	s.False(createSampleRegion([]byte{10}, []byte{20}).ContainsByEnd([]byte{}))
	s.True(createSampleRegion([]byte{10}, []byte{20}).ContainsByEnd([]byte{15}))
	s.False(createSampleRegion([]byte{10}, []byte{20}).ContainsByEnd([]byte{30}))
}

func (s *testRegionCacheSuite) TestSwitchPeerWhenNoLeader() {
	var prevCtx *RPCContext
	for i := 0; i <= len(s.cluster.GetAllStores()); i++ {
		loc, err := s.cache.LocateKey(s.bo, []byte("a"))
		s.Nil(err)
		ctx, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, 0)
		s.Nil(err)
		if prevCtx == nil {
			s.Equal(i, 0)
		} else {
			s.NotEqual(ctx.AccessIdx, prevCtx.AccessIdx)
			s.NotEqual(ctx.Peer, prevCtx.Peer)
		}
		s.cache.InvalidateCachedRegionWithReason(loc.Region, NoLeader)
		s.Equal(s.cache.GetCachedRegionWithRLock(loc.Region).invalidReason, NoLeader)
		prevCtx = ctx
	}
}

func BenchmarkOnRequestFail(b *testing.B) {
	/*
			This benchmark simulate many concurrent requests call OnSendRequestFail method
			after failed on a store, validate that on this scene, requests don't get blocked on the
		    RegionCache lock.
	*/
	regionCnt, storeCount := 998, 3
	cluster := createClusterWithStoresAndRegions(regionCnt, storeCount)
	cache := NewRegionCache(mocktikv.NewPDClient(cluster))
	defer cache.Close()
	loadRegionsToCache(cache, regionCnt)
	bo := retry.NewBackofferWithVars(context.Background(), 1, nil)
	loc, err := cache.LocateKey(bo, []byte{})
	if err != nil {
		b.Fatal(err)
	}
	region, _ := cache.searchCachedRegionByID(loc.Region.id)
	b.ResetTimer()
	regionStore := region.getStore()
	store, peer, accessIdx, _ := region.WorkStorePeer(regionStore)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rpcCtx := &RPCContext{
				Region:     loc.Region,
				Meta:       region.meta,
				AccessIdx:  accessIdx,
				Peer:       peer,
				Store:      store,
				AccessMode: tiKVOnly,
			}
			r := cache.GetCachedRegionWithRLock(rpcCtx.Region)
			if r != nil {
				r.getStore().switchNextTiKVPeer(r, rpcCtx.AccessIdx)
			}
		}
	})
	if len(cache.mu.regions) != regionCnt*2/3 {
		b.Fatal(len(cache.mu.regions))
	}
}

func (s *testRegionCacheSuite) TestNoBackoffWhenFailToDecodeRegion() {
	region2 := s.cluster.AllocID()
	newPeers := s.cluster.AllocIDs(2)
	k := []byte("k")
	// Use SplitRaw to split a region with non-memcomparable range keys.
	s.cluster.SplitRaw(s.region1, region2, k, newPeers, newPeers[0])
	_, err := s.cache.LocateKey(s.bo, k)
	s.NotNil(err)
	s.Equal(0, s.bo.GetTotalBackoffTimes())
	_, err = s.cache.LocateRegionByID(s.bo, region2)
	s.NotNil(err)
	s.Equal(0, s.bo.GetTotalBackoffTimes())
	_, err = s.cache.scanRegions(s.bo, []byte{}, []byte{}, 10)
	s.NotNil(err)
	s.Equal(0, s.bo.GetTotalBackoffTimes())
}

func (s *testRegionCacheSuite) TestBuckets() {
	// proto.Clone clones []byte{} to nil and [][]byte{nil or []byte{}} to [][]byte{[]byte{}}.
	// nilToEmtpyBytes unifies it for tests.
	nilToEmtpyBytes := func(s []byte) []byte {
		if s == nil {
			s = []byte{}
		}
		return s
	}

	// 1. cached region contains buckets information fetched from PD.
	r, _ := s.cluster.GetRegion(s.region1)
	defaultBuckets := &metapb.Buckets{
		RegionId: s.region1,
		Version:  uint64(time.Now().Nanosecond()),
		Keys:     [][]byte{nilToEmtpyBytes(r.GetStartKey()), []byte("a"), []byte("b"), nilToEmtpyBytes(r.GetEndKey())},
	}
	s.cluster.SplitRegionBuckets(s.region1, defaultBuckets.Keys, defaultBuckets.Version)

	cachedRegion := s.getRegion([]byte("a"))
	s.Equal(s.region1, cachedRegion.GetID())
	buckets := cachedRegion.getStore().buckets
	s.Equal(defaultBuckets, buckets)

	// test locateBucket
	loc, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.NotNil(loc)
	s.Nil(err)
	s.Equal(buckets, loc.Buckets)
	s.Equal(buckets.GetVersion(), loc.GetBucketVersion())
	for _, key := range [][]byte{{}, {'a' - 1}, []byte("a"), []byte("a0"), []byte("b"), []byte("c")} {
		b := loc.locateBucket(key)
		s.NotNil(b)
		s.True(b.Contains(key))
	}
	// Modify the buckets manually to mock stale information.
	loc.Buckets = proto.Clone(loc.Buckets).(*metapb.Buckets)
	loc.Buckets.Keys = [][]byte{[]byte("b"), []byte("c"), []byte("d")}
	for _, key := range [][]byte{[]byte("a"), []byte("d"), []byte("e")} {
		b := loc.locateBucket(key)
		s.Nil(b)
	}

	// 2. insertRegionToCache keeps old buckets information if needed.
	fakeRegion := &Region{
		meta:          cachedRegion.meta,
		syncFlags:     cachedRegion.syncFlags,
		ttl:           cachedRegion.ttl,
		invalidReason: cachedRegion.invalidReason,
	}
	fakeRegion.setStore(cachedRegion.getStore().clone())
	// no buckets
	fakeRegion.getStore().buckets = nil
	s.cache.insertRegionToCache(fakeRegion, true, true)
	cachedRegion = s.getRegion([]byte("a"))
	s.Equal(defaultBuckets, cachedRegion.getStore().buckets)
	// stale buckets
	fakeRegion.getStore().buckets = &metapb.Buckets{Version: defaultBuckets.Version - 1}
	s.cache.insertRegionToCache(fakeRegion, true, true)
	cachedRegion = s.getRegion([]byte("a"))
	s.Equal(defaultBuckets, cachedRegion.getStore().buckets)
	// new buckets
	newBuckets := &metapb.Buckets{
		RegionId: buckets.RegionId,
		Version:  defaultBuckets.Version + 1,
		Keys:     buckets.Keys,
	}
	fakeRegion.getStore().buckets = newBuckets
	s.cache.insertRegionToCache(fakeRegion, true, true)
	cachedRegion = s.getRegion([]byte("a"))
	s.Equal(newBuckets, cachedRegion.getStore().buckets)

	// 3. epochNotMatch keeps old buckets information.
	cachedRegion = s.getRegion([]byte("a"))
	newMeta := proto.Clone(cachedRegion.meta).(*metapb.Region)
	newMeta.RegionEpoch.Version++
	newMeta.RegionEpoch.ConfVer++
	_, err = s.cache.OnRegionEpochNotMatch(s.bo, &RPCContext{Region: cachedRegion.VerID(), Store: s.cache.getStoreOrInsertDefault(s.store1)}, []*metapb.Region{newMeta})
	s.Nil(err)
	cachedRegion = s.getRegion([]byte("a"))
	s.Equal(newBuckets, cachedRegion.getStore().buckets)

	// 4. test UpdateBuckets
	waitUpdateBuckets := func(expected *metapb.Buckets, key []byte) {
		var buckets *metapb.Buckets
		for i := 0; i < 10; i++ {
			buckets = s.getRegion(key).getStore().buckets
			if reflect.DeepEqual(expected, buckets) {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		s.Equal(expected, buckets)
	}

	cachedRegion = s.getRegion([]byte("a"))
	buckets = cachedRegion.getStore().buckets
	s.cache.UpdateBucketsIfNeeded(cachedRegion.VerID(), buckets.GetVersion()-1)
	// don't update bucket if the new one's version is stale.
	waitUpdateBuckets(buckets, []byte("a"))

	// update buckets if it's nil.
	cachedRegion.getStore().buckets = nil
	// we should replace the version of `cacheRegion` because of stale.
	s.cluster.PutRegion(r.GetId(), newMeta.RegionEpoch.ConfVer, newMeta.RegionEpoch.Version, []uint64{s.store1, s.store2}, []uint64{s.peer1, s.peer2}, s.peer1)
	s.cluster.SplitRegionBuckets(cachedRegion.GetID(), defaultBuckets.Keys, defaultBuckets.Version)
	s.cache.UpdateBucketsIfNeeded(cachedRegion.VerID(), defaultBuckets.GetVersion())
	waitUpdateBuckets(defaultBuckets, []byte("a"))

	// update buckets if the new one's version is greater than old one's.
	cachedRegion = s.getRegion([]byte("a"))
	newBuckets = &metapb.Buckets{
		RegionId: cachedRegion.GetID(),
		Version:  defaultBuckets.Version + 1,
		Keys:     [][]byte{nilToEmtpyBytes(r.GetStartKey()), []byte("a"), nilToEmtpyBytes(r.GetEndKey())},
	}
	s.cluster.SplitRegionBuckets(newBuckets.RegionId, newBuckets.Keys, newBuckets.Version)
	s.cache.UpdateBucketsIfNeeded(cachedRegion.VerID(), newBuckets.GetVersion())
	waitUpdateBuckets(newBuckets, []byte("a"))
}

func (s *testRegionCacheSuite) TestLocateBucket() {
	// proto.Clone clones []byte{} to nil and [][]byte{nil or []byte{}} to [][]byte{[]byte{}}.
	// nilToEmtpyBytes unifies it for tests.
	nilToEmtpyBytes := func(s []byte) []byte {
		if s == nil {
			s = []byte{}
		}
		return s
	}
	r, _ := s.cluster.GetRegion(s.region1)

	// First test normal case: region start equals to the first bucket keys and
	// region end equals to the last bucket key
	bucketKeys := [][]byte{nilToEmtpyBytes(r.GetStartKey()), []byte("a"), []byte("b"), nilToEmtpyBytes(r.GetEndKey())}
	s.cluster.SplitRegionBuckets(s.region1, bucketKeys, uint64(time.Now().Nanosecond()))
	loc, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.NotNil(loc)
	s.Nil(err)
	for _, key := range [][]byte{{}, {'a' - 1}, []byte("a"), []byte("a0"), []byte("b"), []byte("c")} {
		b := loc.locateBucket(key)
		s.NotNil(b)
		s.True(b.Contains(key))
	}

	// Then test cases where there's some holes in region start and the first bucket key
	// and in the last bucket key and region end
	bucketKeys = [][]byte{[]byte("a"), []byte("b")}
	bucketVersion := uint64(time.Now().Nanosecond())
	s.cluster.SplitRegionBuckets(s.region1, bucketKeys, bucketVersion)
	s.cache.UpdateBucketsIfNeeded(s.getRegion([]byte("a")).VerID(), bucketVersion)
	// wait for region update
	time.Sleep(300 * time.Millisecond)
	loc, err = s.cache.LocateKey(s.bo, []byte("a"))
	s.NotNil(loc)
	s.Nil(err)
	for _, key := range [][]byte{{'a' - 1}, []byte("c")} {
		b := loc.locateBucket(key)
		s.Nil(b)
		b = loc.LocateBucket(key)
		s.NotNil(b)
		s.True(b.Contains(key))
	}
}

func (s *testRegionCacheSuite) TestRemoveIntersectingRegions() {
	// Split at "b", "c", "d", "e"
	regions := s.cluster.AllocIDs(4)
	regions = append([]uint64{s.region1}, regions...)

	peers := [][]uint64{{s.peer1, s.peer2}}
	for i := 0; i < 4; i++ {
		peers = append(peers, s.cluster.AllocIDs(2))
	}

	for i := 0; i < 4; i++ {
		s.cluster.Split(regions[i], regions[i+1], []byte{'b' + byte(i)}, peers[i+1], peers[i+1][0])
	}

	for c := 'a'; c <= 'e'; c++ {
		loc, err := s.cache.LocateKey(s.bo, []byte{byte(c)})
		s.Nil(err)
		s.Equal(loc.Region.GetID(), regions[c-'a'])
	}

	// merge all except the last region together
	for i := 1; i <= 3; i++ {
		s.cluster.Merge(regions[0], regions[i])
	}

	// Now the region cache contains stale information
	loc, err := s.cache.LocateKey(s.bo, []byte{'c'})
	s.Nil(err)
	s.NotEqual(loc.Region.GetID(), regions[0]) // This is incorrect, but is expected
	loc, err = s.cache.LocateKey(s.bo, []byte{'e'})
	s.Nil(err)
	s.Equal(loc.Region.GetID(), regions[4]) // 'e' is not merged yet, so it's still correct

	// If we insert the new region into the cache, the old intersecting regions will be removed.
	// And the result will be correct.
	region, err := s.cache.loadRegion(s.bo, []byte("c"), false)
	s.Nil(err)
	s.Equal(region.GetID(), regions[0])
	s.cache.insertRegionToCache(region, true, true)
	loc, err = s.cache.LocateKey(s.bo, []byte{'c'})
	s.Nil(err)
	s.Equal(loc.Region.GetID(), regions[0])
	s.checkCache(2)

	// Now, we merge the last region. This case tests against how we handle the empty end_key.
	s.cluster.Merge(regions[0], regions[4])
	region, err = s.cache.loadRegion(s.bo, []byte("e"), false)
	s.Nil(err)
	s.Equal(region.GetID(), regions[0])
	s.cache.insertRegionToCache(region, true, true)
	loc, err = s.cache.LocateKey(s.bo, []byte{'e'})
	s.Nil(err)
	s.Equal(loc.Region.GetID(), regions[0])
	s.checkCache(1)
}

func (s *testRegionCacheSuite) TestShouldNotRetryFlashback() {
	loc, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.NotNil(loc)
	s.NoError(err)
	ctx, err := s.cache.GetTiKVRPCContext(retry.NewBackofferWithVars(context.Background(), 100, nil), loc.Region, kv.ReplicaReadLeader, 0)
	s.NotNil(ctx)
	s.NoError(err)
	reqSend := NewRegionRequestSender(s.cache, nil)
	shouldRetry, err := reqSend.onRegionError(s.bo, ctx, nil, &errorpb.Error{FlashbackInProgress: &errorpb.FlashbackInProgress{}})
	s.Error(err)
	s.False(shouldRetry)
	shouldRetry, err = reqSend.onRegionError(s.bo, ctx, nil, &errorpb.Error{FlashbackNotPrepared: &errorpb.FlashbackNotPrepared{}})
	s.Error(err)
	s.False(shouldRetry)

	shouldRetry, err = reqSend.onRegionError(s.bo, ctx, nil, &errorpb.Error{BucketVersionNotMatch: &errorpb.BucketVersionNotMatch{Keys: [][]byte{[]byte("a")}, Version: 1}})
	s.Nil(err)
	s.False(shouldRetry)
	ctx.Region.GetID()
	key, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	s.Equal(key.Buckets.Keys, [][]byte{[]byte("a")})
	s.Equal(key.Buckets.Version, uint64(1))
}

func (s *testRegionCacheSuite) TestBackgroundCacheGC() {
	// Prepare 100 regions
	regionCnt := 100
	regions := s.cluster.AllocIDs(regionCnt)
	regions = append([]uint64{s.region1}, regions...)
	peers := [][]uint64{{s.peer1, s.peer2}}
	for i := 0; i < regionCnt; i++ {
		peers = append(peers, s.cluster.AllocIDs(2))
	}
	for i := 0; i < regionCnt; i++ {
		s.cluster.Split(regions[i], regions[i+1], []byte(fmt.Sprintf(regionSplitKeyFormat, i)), peers[i+1], peers[i+1][0])
	}
	loadRegionsToCache(s.cache, regionCnt)
	s.checkCache(regionCnt)

	var (
		regionScanCountLock sync.Mutex
		regionScanCount     = make(map[uint64]int)
	)
	s.cache.bg.ctx = context.WithValue(s.cache.bg.ctx, gcScanItemHookKey{}, func(item *btreeItem) {
		regionScanCountLock.Lock()
		regionScanCount[item.cachedRegion.GetID()]++
		regionScanCountLock.Unlock()
	})

	// Check that region items are scanned uniformly.
	time.Sleep(cleanCacheInterval*time.Duration(2*regionCnt/cleanRegionNumPerRound) + cleanCacheInterval/2)
	regionScanCountLock.Lock()
	s.Equal(regionCnt, len(regionScanCount))
	for _, count := range regionScanCount {
		s.Equal(2, count)
	}
	regionScanCountLock.Unlock()

	// Make parts of the regions stale
	remaining := 0
	s.cache.mu.Lock()
	now := time.Now().Unix()
	for verID, r := range s.cache.mu.regions {
		if verID.id%3 == 0 {
			atomic.StoreInt64(&r.ttl, now-10)
		} else {
			remaining++
		}
	}
	s.cache.mu.Unlock()

	s.Eventually(func() bool {
		s.cache.mu.RLock()
		defer s.cache.mu.RUnlock()
		return len(s.cache.mu.regions) == remaining
	}, 3*time.Second, 200*time.Millisecond)
	s.checkCache(remaining)

	// Make another part of the regions stale
	remaining = 0
	s.cache.mu.Lock()
	now = time.Now().Unix()
	for verID, r := range s.cache.mu.regions {
		if verID.id%3 == 1 {
			atomic.StoreInt64(&r.ttl, now-10)
		} else {
			remaining++
		}
	}
	s.cache.mu.Unlock()

	s.Eventually(func() bool {
		s.cache.mu.RLock()
		defer s.cache.mu.RUnlock()
		return len(s.cache.mu.regions) == remaining
	}, 3*time.Second, 200*time.Millisecond)
	s.checkCache(remaining)
}

func (s *testRegionCacheSuite) TestSlowScoreStat() {
	slowScore := SlowScoreStat{
		avgScore: 1,
	}
	s.False(slowScore.isSlow())
	slowScore.recordSlowScoreStat(time.Millisecond * 1)
	slowScore.updateSlowScore()
	s.False(slowScore.isSlow())
	for i := 2; i <= 100; i++ {
		slowScore.recordSlowScoreStat(time.Millisecond * time.Duration(i))
		if i%5 == 0 {
			slowScore.updateSlowScore()
			s.False(slowScore.isSlow())
		}
	}
	for i := 100; i >= 2; i-- {
		slowScore.recordSlowScoreStat(time.Millisecond * time.Duration(i))
		if i%5 == 0 {
			slowScore.updateSlowScore()
			s.False(slowScore.isSlow())
		}
	}
	slowScore.markAlreadySlow()
	s.True(slowScore.isSlow())
}

func (s *testRegionCacheSuite) TestHealthCheckWithStoreReplace() {
	// init region cache
	s.cache.LocateKey(s.bo, []byte("a"))

	store1, _ := s.cache.getStore(s.store1)
	s.Require().NotNil(store1)
	s.Require().Equal(resolved, store1.getResolveState())

	// setup mock liveness func
	store1Liveness := uint32(unreachable)
	s.cache.setMockRequestLiveness(func(ctx context.Context, s *Store) livenessState {
		if s.storeID == store1.storeID {
			return livenessState(atomic.LoadUint32(&store1Liveness))
		}
		return reachable
	})

	// start health check loop
	atomic.StoreUint32(&store1.livenessState, store1Liveness)
	startHealthCheckLoop(s.cache, store1, livenessState(store1Liveness), time.Second)

	// update store meta
	s.cluster.UpdateStoreAddr(store1.storeID, store1.addr+"'", store1.labels...)

	// assert that the old store should be deleted and it's not reachable
	s.Eventually(func() bool {
		return store1.getResolveState() == deleted && store1.getLivenessState() != reachable
	}, 3*time.Second, time.Second)

	// assert that the new store should be added and it's also not reachable
	newStore1, _ := s.cache.getStore(store1.storeID)
	s.Require().NotEqual(reachable, newStore1.getLivenessState())

	// recover store1
	atomic.StoreUint32(&store1Liveness, uint32(reachable))

	// assert that the new store should be reachable
	s.Eventually(func() bool {
		return newStore1.getResolveState() == resolved && newStore1.getLivenessState() == reachable
	}, 3*time.Second, time.Second)
}

func (s *testRegionRequestToSingleStoreSuite) TestRefreshCache() {
	_ = s.cache.refreshRegionIndex(s.bo)
	r, _ := s.cache.scanRegionsFromCache(s.bo, []byte{}, nil, 10)
	s.Equal(len(r), 1)

	region, _ := s.cache.LocateRegionByID(s.bo, s.region)
	v2 := region.Region.confVer + 1
	r2 := metapb.Region{Id: region.Region.id, RegionEpoch: &metapb.RegionEpoch{Version: region.Region.ver, ConfVer: v2}, StartKey: []byte{1}}
	st := &Store{storeID: s.store}
	s.cache.insertRegionToCache(&Region{meta: &r2, store: unsafe.Pointer(st), ttl: nextTTLWithoutJitter(time.Now().Unix())}, true, true)

	r, _ = s.cache.scanRegionsFromCache(s.bo, []byte{}, nil, 10)
	s.Equal(len(r), 2)

	_ = s.cache.refreshRegionIndex(s.bo)
	r, _ = s.cache.scanRegionsFromCache(s.bo, []byte{}, nil, 10)
	s.Equal(len(r), 1)
}

func (s *testRegionRequestToSingleStoreSuite) TestRefreshCacheConcurrency() {
	ctx, cancel := context.WithCancel(context.Background())
	go func(cache *RegionCache) {
		for {
			_ = cache.refreshRegionIndex(retry.NewNoopBackoff(context.Background()))
			if ctx.Err() != nil {
				return
			}
		}
	}(s.cache)

	regionID := s.region
	go func(cache *RegionCache) {
		for {
			_, _ = cache.LocateRegionByID(retry.NewNoopBackoff(context.Background()), regionID)
			if ctx.Err() != nil {
				return
			}
		}
	}(s.cache)
	time.Sleep(5 * time.Second)

	cancel()
}

func TestRegionCacheWithDelay(t *testing.T) {
	suite.Run(t, new(testRegionCacheWithDelaySuite))
}

type testRegionCacheWithDelaySuite struct {
	suite.Suite
	mvccStore mocktikv.MVCCStore
	cluster   *mocktikv.Cluster
	store     uint64 // store1 is leader
	region1   uint64
	bo        *retry.Backoffer

	delay      uatomic.Bool
	delayCache *RegionCache
	cache      *RegionCache
}

func (s *testRegionCacheWithDelaySuite) SetupTest() {
	s.mvccStore = mocktikv.MustNewMVCCStore()
	s.cluster = mocktikv.NewCluster(s.mvccStore)
	storeIDs, _, regionID, _ := mocktikv.BootstrapWithMultiStores(s.cluster, 1)
	s.region1 = regionID
	s.store = storeIDs[0]
	pdCli := &CodecPDClient{mocktikv.NewPDClient(s.cluster), apicodec.NewCodecV1(apicodec.ModeTxn)}
	s.cache = NewRegionCache(pdCli)
	pdCli2 := &CodecPDClient{mocktikv.NewPDClient(s.cluster, mocktikv.WithDelay(&s.delay)), apicodec.NewCodecV1(apicodec.ModeTxn)}
	s.delayCache = NewRegionCache(pdCli2)
	s.bo = retry.NewBackofferWithVars(context.Background(), 5000, nil)
}

func (s *testRegionCacheWithDelaySuite) TearDownTest() {
	s.cache.Close()
	s.delayCache.Close()
	s.mvccStore.Close()
}

func (s *testRegionCacheWithDelaySuite) TestInsertStaleRegion() {
	r, err := s.cache.findRegionByKey(s.bo, []byte("a"), false)
	s.NoError(err)
	fakeRegion := &Region{
		meta:          r.meta,
		syncFlags:     r.syncFlags,
		ttl:           r.ttl,
		invalidReason: r.invalidReason,
	}
	fakeRegion.setStore(r.getStore().clone())
	keya := mocktikv.NewMvccKey([]byte("a"))
	keyb := mocktikv.NewMvccKey([]byte("b"))
	keyc := mocktikv.NewMvccKey([]byte("c"))
	newRegionID := s.cluster.AllocID()
	newPeersIDs := s.cluster.AllocIDs(1)
	s.cluster.Split(r.GetID(), newRegionID, []byte("b"), newPeersIDs, newPeersIDs[0])
	newPeersIDs = s.cluster.AllocIDs(1)
	s.cluster.Split(newRegionID, s.cluster.AllocID(), []byte("c"), newPeersIDs, newPeersIDs[0])

	r.invalidate(Other)
	r2, err := s.cache.findRegionByKey(s.bo, keyc, false)
	s.NoError(err)
	s.Equal([]byte("c"), r2.StartKey())
	r2, err = s.cache.findRegionByKey(s.bo, keyb, false)
	s.NoError(err)
	s.Equal([]byte("b"), r2.StartKey())
	ra, err := s.cache.loadRegion(s.bo, keya, false)
	s.NoError(err)
	s.cache.mu.Lock()
	stale := s.cache.insertRegionToCache(ra, true, true)
	s.cache.mu.Unlock()
	s.True(stale)

	stale = !s.cache.insertRegionToCache(fakeRegion, true, true)
	s.True(stale)

	rs, err := s.cache.scanRegionsFromCache(s.bo, []byte(""), []byte(""), 100)
	s.NoError(err)
	s.Greater(len(rs), 1)
	s.NotEqual(rs[0].EndKey(), "")

	r3, err := s.cache.findRegionByKey(s.bo, []byte("a"), false)
	s.NoError(err)
	s.Equal([]byte("b"), r3.EndKey())
}

func (s *testRegionCacheWithDelaySuite) TestStaleGetRegion() {
	r1, err := s.cache.findRegionByKey(s.bo, []byte("a"), false)
	s.NoError(err)
	r2, err := s.delayCache.findRegionByKey(s.bo, []byte("a"), false)
	s.NoError(err)
	s.Equal(r1.meta, r2.meta)

	// simulates network delay
	s.delay.Store(true)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		r2.invalidate(Other)
		_, err := s.delayCache.findRegionByKey(s.bo, []byte("b"), false)
		s.NoError(err)
		wg.Done()
	}()
	time.Sleep(30 * time.Millisecond)
	newPeersIDs := s.cluster.AllocIDs(1)
	s.cluster.Split(r1.GetID(), s.cluster.AllocID(), []byte("b"), newPeersIDs, newPeersIDs[0])
	r1.invalidate(Other)
	r, err := s.cache.findRegionByKey(s.bo, []byte("b"), false)
	s.NoError(err)
	s.Equal([]byte("b"), r.meta.StartKey)
	r, err = s.cache.findRegionByKey(s.bo, []byte("c"), false)
	s.NoError(err)
	s.Equal([]byte("b"), r.meta.StartKey)

	s.delay.Store(false)
	r, err = s.delayCache.findRegionByKey(s.bo, []byte("b"), false)
	s.NoError(err)
	s.Equal([]byte("b"), r.meta.StartKey)
	wg.Wait()
	// the delay response is received, but insert failed.
	r, err = s.delayCache.findRegionByKey(s.bo, []byte("b"), false)
	s.NoError(err)
	s.Equal([]byte("b"), r.meta.StartKey)
	r, err = s.delayCache.findRegionByKey(s.bo, []byte("a"), false)
	s.NoError(err)
	s.Equal([]byte("b"), r.meta.EndKey)
}

func (s *testRegionCacheWithDelaySuite) TestFollowerGetStaleRegion() {
	var delay uatomic.Bool
	pdCli3 := &CodecPDClient{mocktikv.NewPDClient(s.cluster, mocktikv.WithDelay(&delay)), apicodec.NewCodecV1(apicodec.ModeTxn)}
	followerDelayCache := NewRegionCache(pdCli3)

	delay.Store(true)
	var wg sync.WaitGroup
	wg.Add(1)
	var final *Region
	go func() {
		var err error
		// followerDelayCache is empty now, so it will go follower.
		final, err = followerDelayCache.findRegionByKey(s.bo, []byte("z"), false)
		s.NoError(err)
		wg.Done()
	}()
	time.Sleep(30 * time.Millisecond)
	delay.Store(false)
	r, err := followerDelayCache.findRegionByKey(s.bo, []byte("y"), false)
	s.NoError(err)
	newPeersIDs := s.cluster.AllocIDs(1)
	s.cluster.Split(r.GetID(), s.cluster.AllocID(), []byte("z"), newPeersIDs, newPeersIDs[0])
	r.invalidate(Other)
	r, err = followerDelayCache.findRegionByKey(s.bo, []byte("y"), false)
	s.NoError(err)
	s.Equal([]byte("z"), r.meta.EndKey)

	// no need to retry because
	wg.Wait()
	s.Equal([]byte("z"), final.meta.StartKey)

	followerDelayCache.Close()
}

func generateKeyForSimulator(id int, keyLen int) []byte {
	k := make([]byte, keyLen)
	copy(k, fmt.Sprintf("%010d", id))
	return k
}

func BenchmarkInsertRegionToCache(b *testing.B) {
	b.StopTimer()
	cache := newTestRegionCache()
	r := &Region{
		meta: &metapb.Region{
			Id:          1,
			RegionEpoch: &metapb.RegionEpoch{},
		},
	}
	rs := &regionStore{
		workTiKVIdx:  0,
		proxyTiKVIdx: -1,
		stores:       make([]*Store, 0, len(r.meta.Peers)),
		storeEpochs:  make([]uint32, 0, len(r.meta.Peers)),
	}
	r.setStore(rs)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		newMeta := proto.Clone(r.meta).(*metapb.Region)
		newMeta.Id = uint64(i + 1)
		newMeta.RegionEpoch.ConfVer = uint64(i+1) - uint64(rand.Intn(i+1))
		newMeta.RegionEpoch.Version = uint64(i+1) - uint64(rand.Intn(i+1))
		if i%2 == 0 {
			newMeta.StartKey = generateKeyForSimulator(rand.Intn(i+1), 56)
			newMeta.EndKey = []byte("")
		} else {
			newMeta.EndKey = generateKeyForSimulator(rand.Intn(i+1), 56)
			newMeta.StartKey = []byte("")
		}
		region := &Region{
			meta: newMeta,
		}
		region.setStore(r.getStore())
		cache.insertRegionToCache(region, true, true)
	}
}

func BenchmarkInsertRegionToCache2(b *testing.B) {
	b.StopTimer()
	cache := newTestRegionCache()
	r := &Region{
		meta: &metapb.Region{
			Id:          1,
			RegionEpoch: &metapb.RegionEpoch{},
		},
	}
	rs := &regionStore{
		workTiKVIdx:  0,
		proxyTiKVIdx: -1,
		stores:       make([]*Store, 0, len(r.meta.Peers)),
		storeEpochs:  make([]uint32, 0, len(r.meta.Peers)),
	}
	r.setStore(rs)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		newMeta := proto.Clone(r.meta).(*metapb.Region)
		newMeta.RegionEpoch.ConfVer = uint64(i + 1)
		newMeta.RegionEpoch.Version = uint64(i + 1)
		region := &Region{
			meta: newMeta,
		}
		region.setStore(r.getStore())
		cache.insertRegionToCache(region, true, true)
	}
}
