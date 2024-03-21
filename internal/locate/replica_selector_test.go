package locate

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/config/retry"
	"github.com/tikv/client-go/v2/internal/apicodec"
	"github.com/tikv/client-go/v2/internal/client"
	"github.com/tikv/client-go/v2/internal/client/mockserver"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/internal/mockstore/mocktikv"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util/israce"
	"go.uber.org/zap"
)

type testReplicaSelectorSuite struct {
	suite.Suite
	cluster    *mocktikv.Cluster
	storeIDs   []uint64
	peerIDs    []uint64
	regionID   uint64
	leaderPeer uint64
	cache      *RegionCache
	bo         *retry.Backoffer
	mvccStore  mocktikv.MVCCStore
}

func (s *testReplicaSelectorSuite) SetupTest(t *testing.T) {
	s.mvccStore = mocktikv.MustNewMVCCStore()
	s.cluster = mocktikv.NewCluster(s.mvccStore)
	s.storeIDs, s.peerIDs, s.regionID, s.leaderPeer = mocktikv.BootstrapWithMultiStores(s.cluster, 3)
	pdCli := &CodecPDClient{mocktikv.NewPDClient(s.cluster), apicodec.NewCodecV1(apicodec.ModeTxn)}
	s.cache = NewRegionCache(pdCli)
	s.bo = retry.NewNoopBackoff(context.Background())
	s.SetT(t)
	s.SetS(s)

	randIntn = func(n int) int { return 0 }
	s.NoError(failpoint.Enable("tikvclient/fastBackoffBySkipSleep", `return`))
	s.NoError(failpoint.Enable("tikvclient/skipStoreCheckUntilHealth", `return`))

	loc, err := s.cache.LocateKey(s.bo, []byte("key"))
	s.Nil(err)
	r := s.cache.GetCachedRegionWithRLock(loc.Region)
	s.NotNil(r)
	// The following assumptions are made in the latter tests, which should be checked in advance:
	s.Equal(r.GetLeaderStoreID(), uint64(1)) // region's leader in store1.
	s.Equal(len(r.getStore().stores), 3)     // region has 3 peer(stores).
	for _, store := range r.getStore().stores {
		s.Equal(store.labels[0].Key, "id") // Each store has a label "id", and the value is the store's ID.
		s.Equal(store.labels[0].Value, fmt.Sprintf("%v", store.storeID))
	}
}

func (s *testReplicaSelectorSuite) TearDownTest() {
	s.cache.Close()
	s.mvccStore.Close()

	randIntn = rand.Intn
	s.NoError(failpoint.Disable("tikvclient/fastBackoffBySkipSleep"))
	s.NoError(failpoint.Disable("tikvclient/skipStoreCheckUntilHealth"))
}

type replicaSelectorAccessPathCase struct {
	reqType          tikvrpc.CmdType
	readType         kv.ReplicaReadType
	staleRead        bool
	timeout          time.Duration
	busyThresholdMs  uint32
	label            *metapb.StoreLabel
	accessErr        []RegionErrorType
	accessErrInValid bool
	expect           *accessPathResult
	result           accessPathResult
	beforeRun        func() // beforeRun will be called before the test case execute, if it is nil, resetStoreState will be called.
	afterRun         func() // afterRun will be called after the test case execute, if it is nil, invalidateRegion will be called.
}

type accessPathResult struct {
	accessPath      []string
	respErr         string
	respRegionError *errorpb.Error
	backoffCnt      int
	backoffDetail   []string
	regionIsValid   bool
}

func TestReplicaSelectorBasic(t *testing.T) {
	s := new(testReplicaSelectorSuite)
	s.SetupTest(t)
	defer s.TearDownTest()

	req := tikvrpc.NewReplicaReadRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("a")}, kv.ReplicaReadMixed, nil, kvrpcpb.Context{})
	req.EnableStaleWithMixedReplicaRead()
	rc := s.getRegion()
	s.NotNil(rc)
	rc.invalidate(Other)
	selector, err := newReplicaSelectorV2(s.cache, rc.VerID(), req)
	s.NotNil(err)
	s.Equal("cached region invalid", err.Error())
	s.Nil(selector)
	s.Equal("", selector.String())

	rc = s.getRegion()
	selector, err = newReplicaSelectorV2(s.cache, rc.VerID(), req)
	s.Nil(err)
	s.NotNil(selector)
	for _, reqSource := range []string{"leader", "follower", "follower", "unknown"} {
		ctx, err := selector.next(s.bo, req)
		s.Nil(err)
		s.Equal(reqSource, selector.replicaType(ctx))
	}

	rc = s.getRegion()
	selector, err = newReplicaSelectorV2(s.cache, rc.VerID(), req)
	s.Nil(err)
	s.NotNil(selector)
	ctx, err := selector.next(s.bo, req)
	s.Nil(err)
	s.NotNil(ctx)
	rc.invalidate(Other)
	ctx, err = selector.next(s.bo, req)
	s.Nil(err)
	s.Nil(ctx)
}

func TestReplicaSelectorCalculateScore(t *testing.T) {
	s := new(testReplicaSelectorSuite)
	s.SetupTest(t)
	defer s.TearDownTest()

	req := tikvrpc.NewReplicaReadRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("a")}, kv.ReplicaReadMixed, nil, kvrpcpb.Context{})
	region, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	s.NotNil(region)
	selector, err := NewReplicaSelector(s.cache, region.Region, req)
	s.Nil(err)
	for i, r := range selector.getBaseReplicaSelector().replicas {
		rc := s.cache.GetCachedRegionWithRLock(region.Region)
		s.NotNil(rc)
		isLeader := r.peer.Id == rc.GetLeaderPeerID()
		s.Equal(isLeader, AccessIndex(i) == rc.getStore().workTiKVIdx)
		strategy := ReplicaSelectMixedStrategy{leaderIdx: rc.getStore().workTiKVIdx}
		score := strategy.calculateScore(r, isLeader)
		s.Equal(r.store.healthStatus.IsSlow(), false)
		if isLeader {
			s.Equal(score, flagLabelMatches+flagNotSlow+flagNotAttempt)
		} else {
			s.Equal(score, flagLabelMatches+flagNormalPeer+flagNotSlow+flagNotAttempt)
		}
		r.store.healthStatus.markAlreadySlow()
		s.Equal(r.store.healthStatus.IsSlow(), true)
		score = strategy.calculateScore(r, isLeader)
		if isLeader {
			s.Equal(score, flagLabelMatches+flagNotAttempt)
		} else {
			s.Equal(score, flagLabelMatches+flagNormalPeer+flagNotAttempt)
		}
		strategy.tryLeader = true
		score = strategy.calculateScore(r, isLeader)
		s.Equal(score, flagLabelMatches+flagNormalPeer+flagNotAttempt)
		strategy.preferLeader = true
		score = strategy.calculateScore(r, isLeader)
		s.Equal(score, flagLabelMatches+flagNormalPeer+flagNotAttempt)
		strategy.learnerOnly = true
		strategy.tryLeader = false
		strategy.preferLeader = false
		score = strategy.calculateScore(r, isLeader)
		s.Equal(score, flagLabelMatches+flagNotAttempt)
		labels := []*metapb.StoreLabel{
			{
				Key:   "zone",
				Value: "us-west-1",
			},
		}
		strategy.labels = labels
		score = strategy.calculateScore(r, isLeader)
		s.Equal(score, flagNotAttempt)

		strategy = ReplicaSelectMixedStrategy{
			leaderIdx: rc.getStore().workTiKVIdx,
			tryLeader: true,
			labels:    labels,
		}
		score = strategy.calculateScore(r, isLeader)
		if isLeader {
			s.Equal(score, flagPreferLeader+flagNotAttempt)
		} else {
			s.Equal(score, flagNormalPeer+flagNotAttempt)
		}

		strategy = ReplicaSelectMixedStrategy{
			leaderIdx:    rc.getStore().workTiKVIdx,
			preferLeader: true,
			labels:       labels,
		}
		score = strategy.calculateScore(r, isLeader)
		s.Equal(score, flagNormalPeer+flagNotAttempt)
		r.store.labels = labels
		score = strategy.calculateScore(r, isLeader)
		s.Equal(score, flagLabelMatches+flagNormalPeer+flagNotAttempt)
		r.store.labels = nil
	}
}

func TestCanFastRetry(t *testing.T) {
	s := new(testReplicaSelectorSuite)
	s.SetupTest(t)
	defer s.TearDownTest()

	// Test for non-leader read.
	loc, err := s.cache.LocateKey(s.bo, []byte("key"))
	s.Nil(err)
	req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("key")})
	req.EnableStaleWithMixedReplicaRead()
	selector, err := newReplicaSelector(s.cache, loc.Region, req)
	s.Nil(err)
	for i := 0; i < 3; i++ {
		_, err = selector.next(s.bo, req)
		s.Nil(err)
		selector.canFastRetry()
		s.True(selector.canFastRetry())
	}

	// Test for leader read.
	req = tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("key")})
	req.ReplicaReadType = kv.ReplicaReadLeader
	selector, err = newReplicaSelector(s.cache, loc.Region, req)
	s.Nil(err)
	for i := 0; i < 12; i++ {
		_, err = selector.next(s.bo, req)
		s.Nil(err)
		ok := selector.canFastRetry()
		if i <= 8 {
			s.False(ok) // can't skip since leader is available.
		} else {
			s.True(ok)
		}
	}
}

func TestPendingBackoff(t *testing.T) {
	s := new(testReplicaSelectorSuite)
	s.SetupTest(t)
	defer s.TearDownTest()

	loc, err := s.cache.LocateKey(s.bo, []byte("key"))
	s.Nil(err)
	req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("key")})
	req.EnableStaleWithMixedReplicaRead()
	selector, err := newReplicaSelector(s.cache, loc.Region, req)
	s.Nil(err)
	bo := retry.NewNoopBackoff(context.Background())
	err = selector.backoffOnRetry(nil, bo)
	s.Nil(err)
	err = selector.backoffOnRetry(&Store{storeID: 1}, bo)
	s.Nil(err)
	err = selector.backoffOnNoCandidate(bo)
	s.Nil(err)
	selector.addPendingBackoff(nil, retry.BoRegionScheduling, errors.New("err-0"))
	s.Equal(1, len(selector.pendingBackoffs))
	selector.addPendingBackoff(&Store{storeID: 1}, retry.BoTiKVRPC, errors.New("err-1"))
	s.Equal(2, len(selector.pendingBackoffs))
	selector.addPendingBackoff(&Store{storeID: 2}, retry.BoTiKVDiskFull, errors.New("err-2"))
	s.Equal(3, len(selector.pendingBackoffs))
	selector.addPendingBackoff(&Store{storeID: 1}, retry.BoTiKVServerBusy, errors.New("err-3"))
	s.Equal(3, len(selector.pendingBackoffs))
	_, ok := selector.pendingBackoffs[0]
	s.True(ok)
	err = selector.backoffOnRetry(nil, bo)
	s.NotNil(err)
	s.Equal("err-0", err.Error())
	_, ok = selector.pendingBackoffs[0]
	s.False(ok)
	s.Equal(2, len(selector.pendingBackoffs))
	err = selector.backoffOnRetry(&Store{storeID: 10}, bo)
	s.Nil(err)
	s.Equal(2, len(selector.pendingBackoffs))
	err = selector.backoffOnNoCandidate(bo)
	s.NotNil(err)
	s.Equal("err-3", err.Error())
}

func TestReplicaReadAccessPathByCase(t *testing.T) {
	s := new(testReplicaSelectorSuite)
	s.SetupTest(t)
	defer s.TearDownTest()

	fakeEpochNotMatch := &errorpb.Error{EpochNotMatch: &errorpb.EpochNotMatch{}} // fake region error, cause by no replica is available.
	ca := replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadMixed,
		staleRead: true,
		accessErr: []RegionErrorType{DataIsNotReadyErr, ServerIsBusyErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: true}",
				"{addr: store2, replica-read: true, stale-read: false}",
				"{addr: store3, replica-read: true, stale-read: false}",
			},
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      0,
			backoffDetail:   []string{},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	// test stale read with label.
	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadMixed,
		staleRead: true,
		label:     &metapb.StoreLabel{Key: "id", Value: "2"},
		accessErr: []RegionErrorType{DataIsNotReadyErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store2, replica-read: false, stale-read: true}",
				"{addr: store1, replica-read: false, stale-read: false}", // try leader with leader read.
			},
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      0,
			backoffDetail:   []string{},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadMixed,
		staleRead: true,
		label:     &metapb.StoreLabel{Key: "id", Value: "2"},
		accessErr: []RegionErrorType{DataIsNotReadyErr, ServerIsBusyErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store2, replica-read: false, stale-read: true}",
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store2, replica-read: true, stale-read: false}"},
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      0,
			backoffDetail:   []string{},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadMixed,
		staleRead: true,
		timeout:   time.Second,
		label:     &metapb.StoreLabel{Key: "id", Value: "2"},
		accessErr: []RegionErrorType{DataIsNotReadyErr, NotLeaderWithNewLeader2Err},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store2, replica-read: false, stale-read: true}",
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store2, replica-read: false, stale-read: false}"}, // retry the new leader.
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      0,
			backoffDetail:   []string{},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadMixed,
		staleRead: true,
		timeout:   time.Second,
		label:     &metapb.StoreLabel{Key: "id", Value: "2"},
		accessErr: []RegionErrorType{DeadLineExceededErr, NotLeaderWithNewLeader2Err},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store2, replica-read: false, stale-read: true}",
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store3, replica-read: true, stale-read: false}"}, // store2 has DeadLineExceededErr, so don't retry store2 even it is new leader.
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      0,
			backoffDetail:   []string{},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadLeader,
		staleRead: false,
		timeout:   time.Second,
		accessErr: []RegionErrorType{DeadLineExceededErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store2, replica-read: true, stale-read: false}"},
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      0,
			backoffDetail:   []string{},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadLeader,
		staleRead: false,
		timeout:   time.Second,
		accessErr: []RegionErrorType{NotLeaderWithNewLeader3Err, DeadLineExceededErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store3, replica-read: false, stale-read: false}",
				"{addr: store2, replica-read: true, stale-read: false}"},
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      0,
			backoffDetail:   []string{},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdPrewrite,
		readType:  kv.ReplicaReadLeader,
		staleRead: false,
		timeout:   time.Second, // this actually has no effect on write req. since tikv_client_read_timeout is used for read req only.
		accessErr: []RegionErrorType{NotLeaderWithNewLeader3Err, DeadLineExceededErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store3, replica-read: false, stale-read: false}",  // try new leader in store3, but got DeadLineExceededErr, and this store's liveness will be mock to unreachable in test case running.
				"{addr: store2, replica-read: false, stale-read: false}"}, // try remaining replica in store2.
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      1,
			backoffDetail:   []string{"tikvRPC+1"},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadLeader,
		staleRead: false,
		timeout:   time.Second,
		accessErr: []RegionErrorType{NotLeaderErr, DeadLineExceededErr, NotLeaderWithNewLeader2Err},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store2, replica-read: false, stale-read: false}",
				"{addr: store3, replica-read: false, stale-read: false}"},
			respErr:         "",
			respRegionError: fakeEpochNotMatch,
			backoffCnt:      1,
			backoffDetail:   []string{"regionScheduling+1"},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	// Don't invalid region in tryFollowers, since leader meets deadlineExceededErr.
	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadMixed,
		staleRead: true,
		timeout:   time.Second,
		label:     nil,
		accessErr: []RegionErrorType{DeadLineExceededErr, ServerIsBusyErr, ServerIsBusyErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: true}",
				"{addr: store2, replica-read: true, stale-read: false}",
				"{addr: store3, replica-read: true, stale-read: false}"},
			respErr:         "",
			respRegionError: fakeEpochNotMatch,
			backoffCnt:      1,
			backoffDetail:   []string{"tikvServerBusy+1"},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	// Don't invalid region in accessFollower, since leader meets deadlineExceededErr.
	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadMixed,
		staleRead: false,
		timeout:   time.Second,
		label:     nil,
		accessErr: []RegionErrorType{ServerIsBusyErr, ServerIsBusyErr, DeadLineExceededErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: true, stale-read: false}",
				"{addr: store2, replica-read: true, stale-read: false}",
				"{addr: store3, replica-read: true, stale-read: false}"},
			respErr:         "",
			respRegionError: fakeEpochNotMatch,
			backoffCnt:      1,
			backoffDetail:   []string{"tikvServerBusy+1"},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadMixed,
		staleRead: true,
		timeout:   0,
		label:     &metapb.StoreLabel{Key: "id", Value: "3"},
		accessErr: []RegionErrorType{ServerIsBusyErr, ServerIsBusyErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store3, replica-read: false, stale-read: true}",
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store2, replica-read: true, stale-read: false}"},
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      0,
			backoffDetail:   []string{},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadMixed,
		staleRead: true,
		label:     &metapb.StoreLabel{Key: "id", Value: "2"},
		accessErr: []RegionErrorType{DataIsNotReadyErr, ServerIsBusyErr, ServerIsBusyErr, ServerIsBusyErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store2, replica-read: false, stale-read: true}",
				"{addr: store1, replica-read: false, stale-read: false}", // try leader with leader read.
				"{addr: store2, replica-read: true, stale-read: false}",
				"{addr: store3, replica-read: true, stale-read: false}",
			},
			respErr:         "",
			respRegionError: fakeEpochNotMatch,
			backoffCnt:      1,
			backoffDetail:   []string{"tikvServerBusy+1"},
			regionIsValid:   false,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	s.changeRegionLeader(2)
	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadMixed,
		staleRead: true,
		accessErr: []RegionErrorType{DataIsNotReadyErr, ServerIsBusyErr, ServerIsBusyErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: true}",
				"{addr: store2, replica-read: false, stale-read: false}", // try leader with leader read.
				"{addr: store3, replica-read: true, stale-read: false}",
				"{addr: store1, replica-read: true, stale-read: false}",
			},
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      0, // no backoff since request success.
			backoffDetail:   []string{},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadMixed,
		staleRead: true,
		accessErr: []RegionErrorType{DataIsNotReadyErr, ServerIsBusyErr, ServerIsBusyErr, ServerIsBusyErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: true}",
				"{addr: store2, replica-read: false, stale-read: false}", // try leader with leader read.
				"{addr: store3, replica-read: true, stale-read: false}",
				"{addr: store1, replica-read: true, stale-read: false}",
			},
			respErr:         "",
			respRegionError: fakeEpochNotMatch,
			backoffCnt:      1,
			backoffDetail:   []string{"tikvServerBusy+1"},
			regionIsValid:   false,
		},
	}
	s.True(s.runCaseAndCompare(ca))
	s.changeRegionLeader(1)

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadMixed,
		staleRead: false,
		timeout:   time.Second,
		label:     &metapb.StoreLabel{Key: "id", Value: "2"},
		accessErr: []RegionErrorType{DeadLineExceededErr, ServerIsBusyErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store2, replica-read: true, stale-read: false}",
				"{addr: store1, replica-read: true, stale-read: false}",
				"{addr: store3, replica-read: true, stale-read: false}"},
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      0,
			backoffDetail:   []string{},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadMixed,
		staleRead: true,
		timeout:   time.Microsecond * 100,
		accessErr: []RegionErrorType{ServerIsBusyErr, ServerIsBusyWithEstimatedWaitMsErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: true}",
				"{addr: store2, replica-read: true, stale-read: false}",
				"{addr: store3, replica-read: true, stale-read: false}",
			},
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      0,
			backoffDetail:   []string{},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))
}

func TestReplicaReadAccessPathByCase2(t *testing.T) {
	s := new(testReplicaSelectorSuite)
	s.SetupTest(t)
	defer s.TearDownTest()

	fakeEpochNotMatch := &errorpb.Error{EpochNotMatch: &errorpb.EpochNotMatch{}}
	// Following cases are found by other test, careful.
	ca := replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadMixed,
		staleRead: false,
		timeout:   time.Second,
		label:     &metapb.StoreLabel{Key: "id", Value: "2"},
		accessErr: []RegionErrorType{DeadLineExceededErr, DiskFullErr, FlashbackNotPreparedErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store2, replica-read: true, stale-read: false}",
				"{addr: store1, replica-read: true, stale-read: false}",
				"{addr: store3, replica-read: true, stale-read: false}"},
			respErr:         "region 0 is not prepared for the flashback",
			respRegionError: nil,
			backoffCnt:      1,
			backoffDetail:   []string{"tikvDiskFull+1"},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	// Don't invalid region in tryFollowers, since leader meets deadlineExceededErr.
	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadLeader,
		staleRead: false,
		timeout:   time.Second,
		label:     nil,
		accessErr: []RegionErrorType{NotLeaderErr, NotLeaderWithNewLeader3Err, DeadLineExceededErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store2, replica-read: false, stale-read: false}",
				"{addr: store3, replica-read: false, stale-read: false}"},
			respErr:         "",
			respRegionError: fakeEpochNotMatch,
			backoffCnt:      1,
			backoffDetail:   []string{"regionScheduling+1"},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	// Don't invalid region in accessFollower, since leader meets deadlineExceededErr.
	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadMixed,
		staleRead: false,
		timeout:   time.Second,
		label:     nil,
		accessErr: []RegionErrorType{ServerIsBusyErr, ServerIsBusyErr, DeadLineExceededErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: true, stale-read: false}",
				"{addr: store2, replica-read: true, stale-read: false}",
				"{addr: store3, replica-read: true, stale-read: false}"},
			respErr:         "",
			respRegionError: fakeEpochNotMatch,
			backoffCnt:      1,
			backoffDetail:   []string{"tikvServerBusy+1"},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadMixed,
		staleRead: true,
		timeout:   0,
		label:     &metapb.StoreLabel{Key: "id", Value: "3"},
		accessErr: []RegionErrorType{DataIsNotReadyErr, NotLeaderErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store3, replica-read: false, stale-read: true}",
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store3, replica-read: true, stale-read: false}"},
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      1,
			backoffDetail:   []string{"regionScheduling+1"},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadMixed,
		staleRead: false,
		timeout:   time.Second,
		label:     &metapb.StoreLabel{Key: "id", Value: "2"},
		accessErr: []RegionErrorType{FlashbackInProgressErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store2, replica-read: true, stale-read: false}",
				"{addr: store1, replica-read: true, stale-read: false}"},
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      0,
			backoffDetail:   []string{},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadMixed,
		staleRead: true,
		timeout:   time.Microsecond * 100,
		label:     &metapb.StoreLabel{Key: "id", Value: "1"},
		accessErr: []RegionErrorType{DataIsNotReadyErr, ServerIsBusyErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: true}",
				// don't retry leader(store1), since leader won't return DataIsNotReadyErr, so retry it with leader-read may got NotLeaderErr.
				"{addr: store2, replica-read: true, stale-read: false}",
				"{addr: store3, replica-read: true, stale-read: false}"},
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      0,
			backoffDetail:   []string{},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdPrewrite,
		readType:  kv.ReplicaReadLeader,
		staleRead: false,
		timeout:   0,
		label:     nil,
		accessErr: []RegionErrorType{DeadLineExceededErr, NotLeaderErr, NotLeaderWithNewLeader2Err, ServerIsBusyErr, DeadLineExceededErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store2, replica-read: false, stale-read: false}",
				"{addr: store3, replica-read: false, stale-read: false}",
				"{addr: store2, replica-read: false, stale-read: false}",
				"{addr: store2, replica-read: false, stale-read: false}"},
			respErr:         "",
			respRegionError: fakeEpochNotMatch,
			backoffCnt:      4,
			backoffDetail:   []string{"regionScheduling+1", "tikvRPC+2", "tikvServerBusy+1"},
			regionIsValid:   false,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadMixed,
		staleRead: true,
		timeout:   0,
		label:     nil,
		accessErr: []RegionErrorType{DeadLineExceededErr, RegionNotInitializedErr, DeadLineExceededErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: true}",
				"{addr: store2, replica-read: true, stale-read: false}",
				"{addr: store3, replica-read: true, stale-read: false}"},
			respErr:         "",
			respRegionError: fakeEpochNotMatch,
			backoffCnt:      3,
			backoffDetail:   []string{"regionNotInitialized+1", "tikvRPC+2"},
			regionIsValid:   false,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadMixed,
		staleRead: true,
		timeout:   time.Second,
		label:     &metapb.StoreLabel{Key: "id", Value: "2"},
		accessErr: []RegionErrorType{DeadLineExceededErr, NotLeaderWithNewLeader2Err},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store2, replica-read: false, stale-read: true}",
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store3, replica-read: true, stale-read: false}"},
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      0,
			backoffDetail:   []string{},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadMixed,
		staleRead: false,
		timeout:   time.Second,
		label:     &metapb.StoreLabel{Key: "id", Value: "2"},
		accessErr: []RegionErrorType{DeadLineExceededErr, ServerIsBusyErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store2, replica-read: true, stale-read: false}",
				"{addr: store1, replica-read: true, stale-read: false}",
				"{addr: store3, replica-read: true, stale-read: false}"},
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      0,
			backoffDetail:   []string{},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	s.changeRegionLeader(2)
	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadMixed,
		staleRead: false,
		timeout:   time.Second,
		label:     &metapb.StoreLabel{Key: "id", Value: "3"},
		accessErr: []RegionErrorType{DeadLineExceededErr, ServerIsBusyErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store3, replica-read: true, stale-read: false}",
				"{addr: store2, replica-read: true, stale-read: false}",
				"{addr: store1, replica-read: true, stale-read: false}"},
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      0,
			backoffDetail:   []string{},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))
	s.changeRegionLeader(1)

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadLeader,
		staleRead: false,
		timeout:   time.Second,
		accessErr: []RegionErrorType{NotLeaderErr, DeadLineExceededErr, NotLeaderWithNewLeader2Err},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store2, replica-read: false, stale-read: false}",
				"{addr: store3, replica-read: false, stale-read: false}"},
			respErr:         "",
			respRegionError: fakeEpochNotMatch,
			backoffCnt:      1,
			backoffDetail:   []string{"regionScheduling+1"},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))
}

func TestReplicaReadAccessPathByBasicCase(t *testing.T) {
	s := new(testReplicaSelectorSuite)
	s.SetupTest(t)
	defer s.TearDownTest()

	retryableErrors := []RegionErrorType{ServerIsBusyErr, ServerIsBusyWithEstimatedWaitMsErr, StaleCommandErr, MaxTimestampNotSyncedErr, ProposalInMergingModeErr, ReadIndexNotReadyErr, RegionNotInitializedErr, DiskFullErr}
	noRetryErrors := []RegionErrorType{RegionNotFoundErr, KeyNotInRegionErr, EpochNotMatchErr, StoreNotMatchErr, RaftEntryTooLargeErr, RecoveryInProgressErr, FlashbackNotPreparedErr, IsWitnessErr, MismatchPeerIdErr, BucketVersionNotMatchErr}
	for _, reqType := range []tikvrpc.CmdType{tikvrpc.CmdGet, tikvrpc.CmdPrewrite} {
		for _, readType := range []kv.ReplicaReadType{kv.ReplicaReadLeader, kv.ReplicaReadFollower, kv.ReplicaReadMixed, kv.ReplicaReadPreferLeader} {
			if reqType == tikvrpc.CmdPrewrite && readType != kv.ReplicaReadLeader {
				// write req only support leader read.
				continue
			}
			for _, staleRead := range []bool{false, true} {
				if staleRead && readType != kv.ReplicaReadMixed {
					// stale read only support mixed read.
					continue
				}
				for _, tp := range retryableErrors {
					backoff := []string{}
					switch tp {
					case ServerIsBusyErr, ServerIsBusyWithEstimatedWaitMsErr:
						if readType == kv.ReplicaReadLeader {
							backoff = []string{"tikvServerBusy+1"}
						}
					case DiskFullErr:
						backoff = []string{"tikvDiskFull+1"}
					case RegionNotInitializedErr:
						backoff = []string{"regionNotInitialized+1"}
					case ReadIndexNotReadyErr, ProposalInMergingModeErr:
						backoff = []string{"regionScheduling+1"}
					case MaxTimestampNotSyncedErr:
						backoff = []string{"maxTsNotSynced+1"}
					}
					accessPath := []string{
						"{addr: store1, replica-read: true, stale-read: false}",
						"{addr: store2, replica-read: true, stale-read: false}",
					}
					switch readType {
					case kv.ReplicaReadLeader:
						accessPath = []string{
							"{addr: store1, replica-read: false, stale-read: false}",
							"{addr: store1, replica-read: false, stale-read: false}",
						}
					case kv.ReplicaReadFollower:
						if tp == ServerIsBusyErr {
							backoff = []string{}
						}
						accessPath = []string{
							"{addr: store2, replica-read: true, stale-read: false}",
							"{addr: store3, replica-read: true, stale-read: false}",
						}
					case kv.ReplicaReadMixed:
						if tp == ServerIsBusyErr {
							backoff = []string{}
						}
						if staleRead {
							accessPath = []string{
								"{addr: store1, replica-read: false, stale-read: true}",
								"{addr: store2, replica-read: true, stale-read: false}",
							}
						}
					default:
						if tp == ServerIsBusyErr {
							backoff = []string{}
						}
					}
					ca := replicaSelectorAccessPathCase{
						reqType:   reqType,
						readType:  readType,
						staleRead: staleRead,
						accessErr: []RegionErrorType{tp},
						expect: &accessPathResult{
							accessPath:      accessPath,
							respErr:         "",
							respRegionError: nil,
							backoffCnt:      len(backoff),
							backoffDetail:   backoff,
							regionIsValid:   true,
						},
					}
					s.True(s.runCaseAndCompare(ca))
				}

				for _, tp := range noRetryErrors {
					backoff := []string{}
					regionIsValid := false
					respErr := ""
					respRegionError := tp.GenRegionError()
					accessPath := []string{"{addr: store1, replica-read: true, stale-read: false}"}
					switch tp {
					case RecoveryInProgressErr:
						backoff = []string{"regionRecoveryInProgress+1"}
					case IsWitnessErr:
						backoff = []string{"isWitness+1"}
					case BucketVersionNotMatchErr:
						regionIsValid = true
					case RaftEntryTooLargeErr:
						respErr = RaftEntryTooLargeErr.GenRegionError().String()
						respRegionError = nil
						regionIsValid = true
					case FlashbackNotPreparedErr:
						respErr = "region 0 is not prepared for the flashback"
						respRegionError = nil
						regionIsValid = true
					}
					switch readType {
					case kv.ReplicaReadLeader:
						accessPath = []string{"{addr: store1, replica-read: false, stale-read: false}"}
					case kv.ReplicaReadFollower:
						accessPath = []string{"{addr: store2, replica-read: true, stale-read: false}"}
					case kv.ReplicaReadMixed:
						if staleRead {
							accessPath = []string{"{addr: store1, replica-read: false, stale-read: true}"}
						}
					}
					ca := replicaSelectorAccessPathCase{
						reqType:   reqType,
						readType:  readType,
						staleRead: staleRead,
						accessErr: []RegionErrorType{tp},
						expect: &accessPathResult{
							accessPath:      accessPath,
							respErr:         respErr,
							respRegionError: respRegionError,
							backoffCnt:      len(backoff),
							backoffDetail:   backoff,
							regionIsValid:   regionIsValid,
						},
					}
					s.True(s.runCaseAndCompare(ca))
				}
			}
		}
	}
}

func TestReplicaReadAccessPathByLeaderCase(t *testing.T) {
	s := new(testReplicaSelectorSuite)
	s.SetupTest(t)
	defer s.TearDownTest()

	fakeEpochNotMatch := &errorpb.Error{EpochNotMatch: &errorpb.EpochNotMatch{}} // fake region error, cause by no replica is available.
	ca := replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadLeader,
		accessErr: nil,
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: false}",
			},
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      0,
			backoffDetail:   []string{},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadLeader,
		accessErr: []RegionErrorType{ServerIsBusyWithEstimatedWaitMsErr, ServerIsBusyErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store1, replica-read: false, stale-read: false}",
			},
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      2,
			backoffDetail:   []string{"tikvServerBusy+2"},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:  tikvrpc.CmdGet,
		readType: kv.ReplicaReadLeader,
		accessErr: []RegionErrorType{ServerIsBusyErr, ServerIsBusyErr, ServerIsBusyErr, ServerIsBusyErr, ServerIsBusyErr,
			ServerIsBusyErr, ServerIsBusyErr, ServerIsBusyErr, ServerIsBusyErr, ServerIsBusyErr, ServerIsBusyErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store2, replica-read: false, stale-read: false}",
				"{addr: store3, replica-read: false, stale-read: false}",
			},
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      9,
			backoffDetail:   []string{"tikvServerBusy+9"},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadLeader,
		accessErr: []RegionErrorType{NotLeaderErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store2, replica-read: false, stale-read: false}",
			},
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      1,
			backoffDetail:   []string{"regionScheduling+1"},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadLeader,
		accessErr: []RegionErrorType{NotLeaderErr, NotLeaderErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store2, replica-read: false, stale-read: false}",
				"{addr: store3, replica-read: false, stale-read: false}",
			},
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      2,
			backoffDetail:   []string{"regionScheduling+2"},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadLeader,
		accessErr: []RegionErrorType{NotLeaderErr, NotLeaderErr, NotLeaderErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store2, replica-read: false, stale-read: false}",
				"{addr: store3, replica-read: false, stale-read: false}",
			},
			respErr:         "",
			respRegionError: fakeEpochNotMatch,
			backoffCnt:      3,
			backoffDetail:   []string{"regionScheduling+3"},
			regionIsValid:   false,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadLeader,
		accessErr: []RegionErrorType{NotLeaderWithNewLeader3Err},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store3, replica-read: false, stale-read: false}",
			},
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      0, // no backoff, fast retry.
			backoffDetail:   []string{},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadLeader,
		accessErr: []RegionErrorType{NotLeaderWithNewLeader3Err, ServerIsBusyErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store3, replica-read: false, stale-read: false}",
				"{addr: store3, replica-read: false, stale-read: false}",
			},
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      1,
			backoffDetail:   []string{"tikvServerBusy+1"},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadLeader,
		accessErr: []RegionErrorType{DeadLineExceededErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store2, replica-read: false, stale-read: false}",
			},
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      1,
			backoffDetail:   []string{"tikvRPC+1"},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadLeader,
		accessErr: []RegionErrorType{DeadLineExceededErr, NotLeaderWithNewLeader1Err, NotLeaderWithNewLeader1Err},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: false}", // Suppose there is network partition between TiDB and store1
				"{addr: store2, replica-read: false, stale-read: false}", // TODO(crazycs520): is this expected, should retry with replica-read?
				"{addr: store3, replica-read: false, stale-read: false}", // ditto.
			},
			respErr:         "",
			respRegionError: fakeEpochNotMatch,
			backoffCnt:      1,
			backoffDetail:   []string{"tikvRPC+1"},
			regionIsValid:   false,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadLeader,
		accessErr: []RegionErrorType{DeadLineExceededErr, ServerIsBusyErr, ServerIsBusyErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store2, replica-read: false, stale-read: false}",
				"{addr: store3, replica-read: false, stale-read: false}"},
			respErr:         "",
			respRegionError: fakeEpochNotMatch,
			backoffCnt:      2,
			backoffDetail:   []string{"tikvRPC+1", "tikvServerBusy+1"},
			regionIsValid:   false,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:         tikvrpc.CmdGet,
		readType:        kv.ReplicaReadLeader,
		busyThresholdMs: 10,
		accessErr:       []RegionErrorType{NotLeaderErr, NotLeaderWithNewLeader3Err, ServerIsBusyWithEstimatedWaitMsErr, NotLeaderErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store2, replica-read: false, stale-read: false}",
				"{addr: store3, replica-read: false, stale-read: false}",
				"{addr: store3, replica-read: false, stale-read: false}"},
			respErr:         "",
			respRegionError: fakeEpochNotMatch,
			backoffCnt:      3,
			backoffDetail:   []string{"regionScheduling+2", "tikvServerBusy+1"},
			regionIsValid:   false,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:         tikvrpc.CmdGet,
		readType:        kv.ReplicaReadLeader,
		busyThresholdMs: 10,
		accessErr:       []RegionErrorType{NotLeaderErr, NotLeaderWithNewLeader3Err, ServerIsBusyWithEstimatedWaitMsErr, ServerIsBusyErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store2, replica-read: false, stale-read: false}",
				"{addr: store3, replica-read: false, stale-read: false}",
				"{addr: store3, replica-read: false, stale-read: false}",
				"{addr: store3, replica-read: false, stale-read: false}"},
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      3,
			backoffDetail:   []string{"regionScheduling+1", "tikvServerBusy+2"},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	// Test for switch leader.
	cas := []replicaSelectorAccessPathCase{
		{
			reqType:   tikvrpc.CmdGet,
			readType:  kv.ReplicaReadLeader,
			accessErr: []RegionErrorType{NotLeaderErr},
			expect: &accessPathResult{
				accessPath: []string{
					"{addr: store1, replica-read: false, stale-read: false}",
					"{addr: store2, replica-read: false, stale-read: false}",
				},
				respErr:         "",
				respRegionError: nil,
				backoffCnt:      1,
				backoffDetail:   []string{"regionScheduling+1"},
				regionIsValid:   true,
			},
			afterRun: func() { /* don't invalid region */ },
		},
		{
			reqType:   tikvrpc.CmdGet,
			readType:  kv.ReplicaReadLeader,
			accessErr: []RegionErrorType{NotLeaderWithNewLeader3Err},
			beforeRun: func() { /* don't resetStoreState */ },
			expect: &accessPathResult{
				accessPath: []string{
					"{addr: store2, replica-read: false, stale-read: false}", // try new leader directly.
					"{addr: store3, replica-read: false, stale-read: false}",
				},
				respErr:         "",
				respRegionError: nil,
				backoffCnt:      0,
				backoffDetail:   []string{},
				regionIsValid:   true,
			},
			afterRun: func() { /* don't invalid region */ },
		},
		{
			reqType:   tikvrpc.CmdGet,
			readType:  kv.ReplicaReadLeader,
			accessErr: []RegionErrorType{ServerIsBusyErr},
			beforeRun: func() { /* don't resetStoreState */ },
			expect: &accessPathResult{
				accessPath: []string{
					"{addr: store3, replica-read: false, stale-read: false}", // try new leader directly.
					"{addr: store3, replica-read: false, stale-read: false}",
				},
				respErr:         "",
				respRegionError: nil,
				backoffCnt:      1,
				backoffDetail:   []string{"tikvServerBusy+1"},
				regionIsValid:   true,
			},
		},
	}
	s.True(s.runMultiCaseAndCompare(cas))
}

func TestReplicaReadAccessPathByFollowerCase(t *testing.T) {
	s := new(testReplicaSelectorSuite)
	s.SetupTest(t)
	defer s.TearDownTest()

	fakeEpochNotMatch := &errorpb.Error{EpochNotMatch: &errorpb.EpochNotMatch{}}
	ca := replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadFollower,
		accessErr: nil,
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store2, replica-read: true, stale-read: false}",
			},
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      0,
			backoffDetail:   []string{},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadFollower,
		accessErr: []RegionErrorType{ServerIsBusyWithEstimatedWaitMsErr, ServerIsBusyErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store2, replica-read: true, stale-read: false}",
				"{addr: store3, replica-read: true, stale-read: false}",
				"{addr: store1, replica-read: true, stale-read: false}",
			},
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      0,
			backoffDetail:   []string{},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadFollower,
		accessErr: []RegionErrorType{ServerIsBusyErr, ServerIsBusyWithEstimatedWaitMsErr, ServerIsBusyErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store2, replica-read: true, stale-read: false}",
				"{addr: store3, replica-read: true, stale-read: false}",
				"{addr: store1, replica-read: true, stale-read: false}",
			},
			respErr:         "",
			respRegionError: fakeEpochNotMatch,
			backoffCnt:      1,
			backoffDetail:   []string{"tikvServerBusy+1"},
			regionIsValid:   false,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadFollower,
		accessErr: []RegionErrorType{DeadLineExceededErr, ServerIsBusyErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store2, replica-read: true, stale-read: false}",
				"{addr: store3, replica-read: true, stale-read: false}",
				"{addr: store1, replica-read: true, stale-read: false}",
			},
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      1,
			backoffDetail:   []string{"tikvRPC+1"},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadFollower,
		timeout:   time.Second,
		accessErr: []RegionErrorType{DeadLineExceededErr, ServerIsBusyErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store2, replica-read: true, stale-read: false}",
				"{addr: store3, replica-read: true, stale-read: false}",
				"{addr: store1, replica-read: true, stale-read: false}",
			},
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      0,
			backoffDetail:   []string{},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))
}

func TestReplicaReadAccessPathByMixedAndPreferLeaderCase(t *testing.T) {
	s := new(testReplicaSelectorSuite)
	s.SetupTest(t)
	defer s.TearDownTest()

	fakeEpochNotMatch := &errorpb.Error{EpochNotMatch: &errorpb.EpochNotMatch{}}
	var ca replicaSelectorAccessPathCase
	// since leader in store1, so ReplicaReadMixed and ReplicaReadPreferLeader will have the same access path.
	for _, readType := range []kv.ReplicaReadType{kv.ReplicaReadMixed, kv.ReplicaReadPreferLeader} {
		ca = replicaSelectorAccessPathCase{
			reqType:   tikvrpc.CmdGet,
			readType:  readType,
			accessErr: nil,
			expect: &accessPathResult{
				accessPath: []string{
					"{addr: store1, replica-read: true, stale-read: false}",
				},
				respErr:         "",
				respRegionError: nil,
				backoffCnt:      0,
				backoffDetail:   []string{},
				regionIsValid:   true,
			},
		}
		s.True(s.runCaseAndCompare(ca))

		ca = replicaSelectorAccessPathCase{
			reqType:   tikvrpc.CmdGet,
			readType:  readType,
			accessErr: []RegionErrorType{ServerIsBusyWithEstimatedWaitMsErr, StaleCommandErr, ServerIsBusyErr},
			expect: &accessPathResult{
				accessPath: []string{
					"{addr: store1, replica-read: true, stale-read: false}",
					"{addr: store2, replica-read: true, stale-read: false}",
					"{addr: store3, replica-read: true, stale-read: false}",
				},
				respErr:         "",
				respRegionError: fakeEpochNotMatch,
				backoffCnt:      1,
				backoffDetail:   []string{"tikvServerBusy+1"},
				regionIsValid:   false,
			},
		}
		s.True(s.runCaseAndCompare(ca))

		ca = replicaSelectorAccessPathCase{
			reqType:   tikvrpc.CmdGet,
			readType:  readType,
			accessErr: []RegionErrorType{ServerIsBusyErr, RegionNotFoundErr},
			expect: &accessPathResult{
				accessPath: []string{
					"{addr: store1, replica-read: true, stale-read: false}",
					"{addr: store2, replica-read: true, stale-read: false}",
				},
				respErr:         "",
				respRegionError: RegionNotFoundErr.GenRegionError(),
				backoffCnt:      0,
				backoffDetail:   []string{},
				regionIsValid:   false,
			},
		}
		s.True(s.runCaseAndCompare(ca))

		ca = replicaSelectorAccessPathCase{
			reqType:   tikvrpc.CmdGet,
			readType:  readType,
			accessErr: []RegionErrorType{DeadLineExceededErr, ServerIsBusyErr},
			expect: &accessPathResult{
				accessPath: []string{
					"{addr: store1, replica-read: true, stale-read: false}",
					"{addr: store2, replica-read: true, stale-read: false}",
					"{addr: store3, replica-read: true, stale-read: false}",
				},
				respErr:         "",
				respRegionError: nil,
				backoffCnt:      1,
				backoffDetail:   []string{"tikvRPC+1"},
				regionIsValid:   true,
			},
		}
		s.True(s.runCaseAndCompare(ca))

		ca = replicaSelectorAccessPathCase{
			reqType:   tikvrpc.CmdGet,
			readType:  readType,
			timeout:   time.Second,
			accessErr: []RegionErrorType{DeadLineExceededErr, ServerIsBusyErr},
			expect: &accessPathResult{
				accessPath: []string{
					"{addr: store1, replica-read: true, stale-read: false}",
					"{addr: store2, replica-read: true, stale-read: false}",
					"{addr: store3, replica-read: true, stale-read: false}",
				},
				respErr:         "",
				respRegionError: nil,
				backoffCnt:      0,
				backoffDetail:   []string{},
				regionIsValid:   true,
			},
		}
		s.True(s.runCaseAndCompare(ca))
	}

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadMixed,
		label:     &metapb.StoreLabel{Key: "id", Value: "2"},
		accessErr: []RegionErrorType{DeadLineExceededErr, ServerIsBusyErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store2, replica-read: true, stale-read: false}", // try match label first.
				"{addr: store1, replica-read: true, stale-read: false}",
				"{addr: store3, replica-read: true, stale-read: false}",
			},
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      1,
			backoffDetail:   []string{"tikvRPC+1"},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	s.changeRegionLeader(3)
	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadPreferLeader,
		accessErr: []RegionErrorType{DeadLineExceededErr, ServerIsBusyErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store3, replica-read: true, stale-read: false}", // try leader first.
				"{addr: store1, replica-read: true, stale-read: false}",
				"{addr: store2, replica-read: true, stale-read: false}",
			},
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      1,
			backoffDetail:   []string{"tikvRPC+1"},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadPreferLeader,
		label:     &metapb.StoreLabel{Key: "id", Value: "2"},
		accessErr: []RegionErrorType{DeadLineExceededErr, ServerIsBusyErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store2, replica-read: true, stale-read: false}", // try match label first, since match label has higher priority.
				"{addr: store3, replica-read: true, stale-read: false}", // try leader.
				"{addr: store1, replica-read: true, stale-read: false}",
			},
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      1,
			backoffDetail:   []string{"tikvRPC+1"},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))
	s.changeRegionLeader(1)

	cas := []replicaSelectorAccessPathCase{
		{
			reqType:   tikvrpc.CmdGet,
			readType:  kv.ReplicaReadPreferLeader,
			staleRead: false,
			timeout:   0,
			accessErr: []RegionErrorType{ServerIsBusyErr},
			expect: &accessPathResult{
				accessPath: []string{
					"{addr: store1, replica-read: true, stale-read: false}", // store1 will be marked already slow.
					"{addr: store2, replica-read: true, stale-read: false}",
				},
				respErr:         "",
				respRegionError: nil,
				backoffCnt:      0,
				backoffDetail:   []string{},
				regionIsValid:   true,
			},
			afterRun: func() { /* don't invalid region */ },
		},
		{
			reqType:   tikvrpc.CmdGet,
			readType:  kv.ReplicaReadPreferLeader,
			staleRead: false,
			timeout:   0,
			accessErr: []RegionErrorType{ServerIsBusyErr, ServerIsBusyErr, ServerIsBusyErr},
			beforeRun: func() { /* don't resetStoreState */ },
			expect: &accessPathResult{
				accessPath: []string{
					"{addr: store2, replica-read: true, stale-read: false}", // won't try leader in store1, since it is slow.
					"{addr: store3, replica-read: true, stale-read: false}",
					"{addr: store1, replica-read: true, stale-read: false}",
				},
				respErr:         "",
				respRegionError: fakeEpochNotMatch,
				backoffCnt:      1,
				backoffDetail:   []string{"tikvServerBusy+1"},
				regionIsValid:   false,
			},
		},
	}
	s.True(s.runMultiCaseAndCompare(cas))

	cas = []replicaSelectorAccessPathCase{
		{
			reqType:   tikvrpc.CmdGet,
			readType:  kv.ReplicaReadPreferLeader,
			staleRead: false,
			timeout:   0,
			accessErr: []RegionErrorType{ServerIsBusyErr, ServerIsBusyErr},
			expect: &accessPathResult{
				accessPath: []string{
					"{addr: store1, replica-read: true, stale-read: false}", // store1 will be marked already slow.
					"{addr: store2, replica-read: true, stale-read: false}", // store2 will be marked already slow.
					"{addr: store3, replica-read: true, stale-read: false}",
				},
				respErr:         "",
				respRegionError: nil,
				backoffCnt:      0,
				backoffDetail:   []string{},
				regionIsValid:   true,
			},
			afterRun: func() { /* don't invalid region */ },
		},
		{
			reqType:   tikvrpc.CmdGet,
			readType:  kv.ReplicaReadPreferLeader,
			staleRead: false,
			timeout:   0,
			accessErr: []RegionErrorType{ServerIsBusyErr, ServerIsBusyErr, ServerIsBusyErr},
			beforeRun: func() { /* don't resetStoreState */ },
			expect: &accessPathResult{
				accessPath: []string{
					"{addr: store3, replica-read: true, stale-read: false}", // won't try leader in store1, since it is slow, ditto for store2.
					"{addr: store1, replica-read: true, stale-read: false}",
					// won't retry store2, since it is slow, and it is not leader replica.
				},
				respErr:         "",
				respRegionError: fakeEpochNotMatch,
				backoffCnt:      1,
				backoffDetail:   []string{"tikvServerBusy+1"},
				regionIsValid:   false,
			},
		},
	}
	s.True(s.runMultiCaseAndCompare(cas))
}

func TestReplicaReadAccessPathByStaleReadCase(t *testing.T) {
	s := new(testReplicaSelectorSuite)
	s.SetupTest(t)
	defer s.TearDownTest()

	fakeEpochNotMatch := &errorpb.Error{EpochNotMatch: &errorpb.EpochNotMatch{}}
	ca := replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadMixed,
		staleRead: true,
		accessErr: nil,
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: true}",
			},
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      0,
			backoffDetail:   []string{},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadMixed,
		staleRead: true,
		accessErr: []RegionErrorType{DataIsNotReadyErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: true}",
				"{addr: store2, replica-read: true, stale-read: false}",
			},
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      0,
			backoffDetail:   []string{},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	// test stale read with label.
	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadMixed,
		staleRead: true,
		label:     &metapb.StoreLabel{Key: "id", Value: "2"},
		accessErr: []RegionErrorType{DataIsNotReadyErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store2, replica-read: false, stale-read: true}",
				"{addr: store1, replica-read: false, stale-read: false}", // try leader with leader read.
			},
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      0,
			backoffDetail:   []string{},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadMixed,
		staleRead: true,
		accessErr: []RegionErrorType{DataIsNotReadyErr, ServerIsBusyErr, ServerIsBusyErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: true}",
				"{addr: store2, replica-read: true, stale-read: false}",
				"{addr: store3, replica-read: true, stale-read: false}",
			},
			respErr:         "",
			respRegionError: fakeEpochNotMatch,
			backoffCnt:      1,
			backoffDetail:   []string{"tikvServerBusy+1"},
			regionIsValid:   false,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadMixed,
		staleRead: true,
		label:     &metapb.StoreLabel{Key: "id", Value: "2"},
		accessErr: []RegionErrorType{DataIsNotReadyErr, NotLeaderWithNewLeader3Err},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store2, replica-read: false, stale-read: true}",
				"{addr: store1, replica-read: false, stale-read: false}", // try leader with leader read.
				"{addr: store3, replica-read: false, stale-read: false}", // try new leader.
			},
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      0,
			backoffDetail:   []string{},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadMixed,
		staleRead: true,
		label:     &metapb.StoreLabel{Key: "id", Value: "2"},
		accessErr: []RegionErrorType{DataIsNotReadyErr, ServerIsBusyErr, ServerIsBusyErr, ServerIsBusyErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store2, replica-read: false, stale-read: true}",
				"{addr: store1, replica-read: false, stale-read: false}", // try leader with leader read.
				"{addr: store2, replica-read: true, stale-read: false}",
				"{addr: store3, replica-read: true, stale-read: false}",
			},
			respErr:         "",
			respRegionError: fakeEpochNotMatch,
			backoffCnt:      1,
			backoffDetail:   []string{"tikvServerBusy+1"},
			regionIsValid:   false,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	s.changeRegionLeader(2)
	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadMixed,
		staleRead: true,
		accessErr: []RegionErrorType{DataIsNotReadyErr, ServerIsBusyErr, ServerIsBusyErr, ServerIsBusyErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: true}",
				"{addr: store2, replica-read: false, stale-read: false}", // try leader with leader read.
				"{addr: store3, replica-read: true, stale-read: false}",
				"{addr: store1, replica-read: true, stale-read: false}",
			},
			respErr:         "",
			respRegionError: fakeEpochNotMatch,
			backoffCnt:      1,
			backoffDetail:   []string{"tikvServerBusy+1"},
			regionIsValid:   false,
		},
	}
	s.True(s.runCaseAndCompare(ca))
	s.changeRegionLeader(1)

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadMixed,
		staleRead: true,
		label:     &metapb.StoreLabel{Key: "id", Value: "2"},
		accessErr: []RegionErrorType{DataIsNotReadyErr, NotLeaderErr, ServerIsBusyErr, ServerIsBusyErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store2, replica-read: false, stale-read: true}",
				"{addr: store1, replica-read: false, stale-read: false}", // try leader with leader read.
				"{addr: store2, replica-read: true, stale-read: false}",  // retry store2 with replica-read.
				"{addr: store3, replica-read: true, stale-read: false}",
			},
			respErr:         "",
			respRegionError: fakeEpochNotMatch,
			backoffCnt:      2,
			backoffDetail:   []string{"regionScheduling+1", "tikvServerBusy+1"},
			regionIsValid:   false,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadMixed,
		staleRead: true,
		label:     &metapb.StoreLabel{Key: "id", Value: "2"},
		accessErr: []RegionErrorType{DataIsNotReadyErr, NotLeaderWithNewLeader3Err, ServerIsBusyErr, NotLeaderWithNewLeader2Err, ServerIsBusyErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store2, replica-read: false, stale-read: true}",
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store3, replica-read: false, stale-read: false}",
				"{addr: store3, replica-read: false, stale-read: false}",
				"{addr: store2, replica-read: false, stale-read: false}",
				"{addr: store2, replica-read: false, stale-read: false}",
			},
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      2,
			backoffDetail:   []string{"tikvServerBusy+2"},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadMixed,
		staleRead: true,
		label:     &metapb.StoreLabel{Key: "id", Value: "2"},
		accessErr: []RegionErrorType{DataIsNotReadyErr, NotLeaderErr, ServerIsBusyErr, ServerIsBusyErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store2, replica-read: false, stale-read: true}",
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store2, replica-read: true, stale-read: false}",
				"{addr: store3, replica-read: true, stale-read: false}",
			},
			respErr:         "",
			respRegionError: fakeEpochNotMatch,
			backoffCnt:      2,
			backoffDetail:   []string{"regionScheduling+1", "tikvServerBusy+1"},
			regionIsValid:   false,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadMixed,
		staleRead: true,
		label:     &metapb.StoreLabel{Key: "id", Value: "2"},
		accessErr: []RegionErrorType{DataIsNotReadyErr, RegionNotFoundErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store2, replica-read: false, stale-read: true}",
				"{addr: store1, replica-read: false, stale-read: false}",
			},
			respErr:         "",
			respRegionError: RegionNotFoundErr.GenRegionError(),
			backoffCnt:      0,
			backoffDetail:   []string{},
			regionIsValid:   false,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadMixed,
		staleRead: true,
		timeout:   time.Second,
		accessErr: []RegionErrorType{DeadLineExceededErr, ServerIsBusyErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: true}",
				"{addr: store2, replica-read: true, stale-read: false}",
				"{addr: store3, replica-read: true, stale-read: false}",
			},
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      0,
			backoffDetail:   []string{},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadMixed,
		staleRead: true,
		timeout:   time.Second,
		label:     &metapb.StoreLabel{Key: "id", Value: "2"},
		accessErr: []RegionErrorType{DeadLineExceededErr, ServerIsBusyErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store2, replica-read: false, stale-read: true}",
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store3, replica-read: true, stale-read: false}",
			},
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      0,
			backoffDetail:   []string{},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadMixed,
		staleRead: true,
		timeout:   time.Second,
		label:     &metapb.StoreLabel{Key: "id", Value: "2"},
		accessErr: []RegionErrorType{DeadLineExceededErr, DeadLineExceededErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store2, replica-read: false, stale-read: true}",
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store3, replica-read: true, stale-read: false}",
			},
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      0,
			backoffDetail:   []string{},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	cas := []replicaSelectorAccessPathCase{
		{
			reqType:   tikvrpc.CmdGet,
			readType:  kv.ReplicaReadMixed,
			staleRead: true,
			timeout:   0,
			label:     &metapb.StoreLabel{Key: "id", Value: "2"},
			accessErr: []RegionErrorType{DeadLineExceededErr, DeadLineExceededErr},
			expect: &accessPathResult{
				accessPath: []string{
					"{addr: store2, replica-read: false, stale-read: true}",
					"{addr: store1, replica-read: false, stale-read: false}",
					"{addr: store3, replica-read: true, stale-read: false}",
				},
				respErr:         "",
				respRegionError: nil,
				backoffCnt:      2,
				backoffDetail:   []string{"tikvRPC+2"},
				regionIsValid:   true,
			},
			afterRun: func() { /* don't invalid region */ },
		},
		{
			reqType:   tikvrpc.CmdGet,
			readType:  kv.ReplicaReadMixed,
			staleRead: true,
			label:     &metapb.StoreLabel{Key: "id", Value: "2"},
			accessErr: []RegionErrorType{ServerIsBusyErr},
			beforeRun: func() { /* don't resetStoreState */ },
			expect: &accessPathResult{
				accessPath: []string{
					"{addr: store3, replica-read: true, stale-read: false}",
				},
				respErr:         "",
				respRegionError: fakeEpochNotMatch,
				backoffCnt:      1,
				backoffDetail:   []string{"tikvServerBusy+1"},
				regionIsValid:   false,
			},
			afterRun: func() { /* don't invalid region */ },
		},
	}
	s.True(s.runMultiCaseAndCompare(cas))
}

func TestReplicaReadAccessPathByTryIdleReplicaCase(t *testing.T) {
	s := new(testReplicaSelectorSuite)
	s.SetupTest(t)
	defer s.TearDownTest()

	fakeEpochNotMatch := &errorpb.Error{EpochNotMatch: &errorpb.EpochNotMatch{}}
	var ca replicaSelectorAccessPathCase
	// Try idle replica strategy not support write request.
	ca = replicaSelectorAccessPathCase{
		reqType:         tikvrpc.CmdPrewrite,
		readType:        kv.ReplicaReadLeader,
		busyThresholdMs: 10,
		accessErr:       []RegionErrorType{ServerIsBusyErr, ServerIsBusyErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store1, replica-read: false, stale-read: false}",
			},
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      2,
			backoffDetail:   []string{"tikvServerBusy+2"},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:         tikvrpc.CmdGet,
		readType:        kv.ReplicaReadLeader,
		busyThresholdMs: 10,
		accessErr:       []RegionErrorType{ServerIsBusyWithEstimatedWaitMsErr, ServerIsBusyWithEstimatedWaitMsErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store2, replica-read: true, stale-read: false}",
				"{addr: store3, replica-read: true, stale-read: false}",
			},
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      0,
			backoffDetail:   []string{},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:         tikvrpc.CmdGet,
		readType:        kv.ReplicaReadLeader,
		busyThresholdMs: 10,
		accessErr:       []RegionErrorType{ServerIsBusyWithEstimatedWaitMsErr, ServerIsBusyWithEstimatedWaitMsErr, ServerIsBusyWithEstimatedWaitMsErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store2, replica-read: true, stale-read: false}",
				"{addr: store3, replica-read: true, stale-read: false}",
				"{addr: store1, replica-read: false, stale-read: false}",
			},
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      1,
			backoffDetail:   []string{"tikvServerBusy+1"},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:         tikvrpc.CmdGet,
		readType:        kv.ReplicaReadLeader,
		busyThresholdMs: 10,
		accessErr:       []RegionErrorType{NotLeaderErr, NotLeaderWithNewLeader3Err, ServerIsBusyWithEstimatedWaitMsErr, NotLeaderErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store2, replica-read: false, stale-read: false}",
				"{addr: store3, replica-read: false, stale-read: false}",
				"{addr: store3, replica-read: false, stale-read: false}"},
			respErr:         "",
			respRegionError: fakeEpochNotMatch,
			backoffCnt:      3,
			backoffDetail:   []string{"regionScheduling+2", "tikvServerBusy+1"},
			regionIsValid:   false,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:         tikvrpc.CmdGet,
		readType:        kv.ReplicaReadLeader,
		busyThresholdMs: 10,
		accessErr:       []RegionErrorType{NotLeaderErr, NotLeaderWithNewLeader3Err, ServerIsBusyWithEstimatedWaitMsErr, ServerIsBusyErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store2, replica-read: false, stale-read: false}",
				"{addr: store3, replica-read: false, stale-read: false}",
				"{addr: store3, replica-read: false, stale-read: false}",
				"{addr: store3, replica-read: false, stale-read: false}"},
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      3,
			backoffDetail:   []string{"regionScheduling+1", "tikvServerBusy+2"},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))
}

func TestReplicaReadAccessPathByFlashbackInProgressCase(t *testing.T) {
	s := new(testReplicaSelectorSuite)
	s.SetupTest(t)
	defer s.TearDownTest()

	var ca replicaSelectorAccessPathCase
	fakeEpochNotMatch := &errorpb.Error{EpochNotMatch: &errorpb.EpochNotMatch{}}
	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadLeader,
		staleRead: false,
		timeout:   time.Second,
		label:     nil,
		accessErr: []RegionErrorType{FlashbackInProgressErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: false}",
			},
			respErr:         "region 0 is in flashback progress, FlashbackStartTS is 0",
			respRegionError: nil,
			backoffCnt:      0,
			backoffDetail:   []string{},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadLeader,
		staleRead: false,
		timeout:   time.Second,
		label:     nil,
		accessErr: []RegionErrorType{DeadLineExceededErr, FlashbackInProgressErr, FlashbackInProgressErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store2, replica-read: true, stale-read: false}",
				"{addr: store3, replica-read: true, stale-read: false}",
			},
			respErr:         "",
			respRegionError: fakeEpochNotMatch,
			backoffCnt:      0,
			backoffDetail:   []string{},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	// not compatible case.
	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadMixed,
		staleRead: false,
		timeout:   0,
		label:     nil,
		accessErr: []RegionErrorType{FlashbackInProgressErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: true, stale-read: false}",
				"{addr: store1, replica-read: true, stale-read: false}",
			},
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      0,
			backoffDetail:   []string{},
			regionIsValid:   true,
		},
	}
	s.True(s.runCase(ca, false))
	ca.expect = &accessPathResult{
		accessPath: []string{
			"{addr: store1, replica-read: true, stale-read: false}",
		},
		respErr:         "region 0 is in flashback progress, FlashbackStartTS is 0",
		respRegionError: nil,
		backoffCnt:      0,
		backoffDetail:   []string{},
		regionIsValid:   true,
	}
	s.True(s.runCase(ca, true))

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadMixed,
		staleRead: false,
		timeout:   time.Second,
		label:     nil,
		accessErr: []RegionErrorType{DeadLineExceededErr, FlashbackInProgressErr, FlashbackInProgressErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: true, stale-read: false}",
				"{addr: store2, replica-read: true, stale-read: false}",
				"{addr: store1, replica-read: true, stale-read: false}",
			},
			respErr:         "region 0 is in flashback progress, FlashbackStartTS is 0",
			respRegionError: nil,
			backoffCnt:      0,
			backoffDetail:   []string{},
			regionIsValid:   true,
		},
	}
	s.True(s.runCase(ca, false))
	ca.expect = &accessPathResult{
		accessPath: []string{
			"{addr: store1, replica-read: true, stale-read: false}",
			"{addr: store2, replica-read: true, stale-read: false}",
			"{addr: store3, replica-read: true, stale-read: false}",
		},
		respErr:         "",
		respRegionError: fakeEpochNotMatch,
		backoffCnt:      0,
		backoffDetail:   []string{},
		regionIsValid:   true,
	}
	s.True(s.runCase(ca, true))
}

func TestReplicaReadAccessPathByProxyCase(t *testing.T) {
	s := new(testReplicaSelectorSuite)
	s.SetupTest(t)
	defer s.TearDownTest()

	// Enable forwarding.
	s.cache.enableForwarding = true
	fakeEpochNotMatch := &errorpb.Error{EpochNotMatch: &errorpb.EpochNotMatch{}}
	var ca replicaSelectorAccessPathCase
	cas := []replicaSelectorAccessPathCase{
		{
			reqType:   tikvrpc.CmdGet,
			readType:  kv.ReplicaReadLeader,
			accessErr: []RegionErrorType{DeadLineExceededErr},
			expect: &accessPathResult{
				accessPath: []string{
					"{addr: store1, replica-read: false, stale-read: false}",
					"{addr: store2, replica-read: false, stale-read: false, forward_addr: store1}",
				},
				respErr:         "",
				respRegionError: nil,
				backoffCnt:      1,
				backoffDetail:   []string{"tikvRPC+1"},
				regionIsValid:   true,
			},
			afterRun: func() { /* don't invalid region */ },
		},
		{
			reqType:   tikvrpc.CmdGet,
			readType:  kv.ReplicaReadLeader,
			accessErr: []RegionErrorType{},
			beforeRun: func() { /* don't resetStoreState */ },
			expect: &accessPathResult{
				accessPath: []string{
					"{addr: store2, replica-read: false, stale-read: false, forward_addr: store1}", // access to known proxy direct.
				},
				respErr:         "",
				respRegionError: nil,
				backoffCnt:      0,
				backoffDetail:   []string{},
				regionIsValid:   true,
			},
		},
	}
	s.True(s.runMultiCaseAndCompare(cas))

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadLeader,
		accessErr: []RegionErrorType{DeadLineExceededErr, DeadLineExceededErr, DeadLineExceededErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store2, replica-read: false, stale-read: false, forward_addr: store1}",
				"{addr: store3, replica-read: false, stale-read: false, forward_addr: store1}",
			},
			respErr:         "",
			respRegionError: fakeEpochNotMatch,
			backoffCnt:      3,
			backoffDetail:   []string{"tikvRPC+3"},
			regionIsValid:   false,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadLeader,
		accessErr: []RegionErrorType{DeadLineExceededErr, NotLeaderWithNewLeader2Err, DeadLineExceededErr, ServerIsBusyErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store2, replica-read: false, stale-read: false, forward_addr: store1}",
				"{addr: store2, replica-read: false, stale-read: false}",
				"{addr: store3, replica-read: false, stale-read: false, forward_addr: store2}",
			},
			respErr:         "",
			respRegionError: fakeEpochNotMatch,
			backoffCnt:      3,
			backoffDetail:   []string{"tikvRPC+2", "tikvServerBusy+1"},
			regionIsValid:   false,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadLeader,
		accessErr: []RegionErrorType{DeadLineExceededErr, ServerIsBusyWithEstimatedWaitMsErr, ServerIsBusyWithEstimatedWaitMsErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store2, replica-read: false, stale-read: false, forward_addr: store1}",
				"{addr: store3, replica-read: false, stale-read: false, forward_addr: store1}",
			},
			respErr:         "",
			respRegionError: fakeEpochNotMatch,
			backoffCnt:      3,
			backoffDetail:   []string{"tikvRPC+1", "tikvServerBusy+2"},
			regionIsValid:   false,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdPrewrite,
		readType:  kv.ReplicaReadLeader,
		accessErr: []RegionErrorType{NotLeaderErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store2, replica-read: false, stale-read: false}",
			},
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      1,
			backoffDetail:   []string{"regionScheduling+1"},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	// TODO: maybe we can optimize the proxy strategy in future.
	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdPrewrite,
		readType:  kv.ReplicaReadLeader,
		accessErr: []RegionErrorType{DeadLineExceededErr, NotLeaderErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store2, replica-read: false, stale-read: false, forward_addr: store1}",
				"{addr: store3, replica-read: false, stale-read: false}",
			},
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      2,
			backoffDetail:   []string{"regionScheduling+1", "tikvRPC+1"},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdPrewrite,
		readType:  kv.ReplicaReadLeader,
		accessErr: []RegionErrorType{NotLeaderWithNewLeader2Err, DeadLineExceededErr, NotLeaderErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store2, replica-read: false, stale-read: false}",
				"{addr: store3, replica-read: false, stale-read: false, forward_addr: store2}"},
			respErr:         "",
			respRegionError: fakeEpochNotMatch,
			backoffCnt:      2,
			backoffDetail:   []string{"regionScheduling+1", "tikvRPC+1"},
			regionIsValid:   false,
		},
	}
	s.True(s.runCaseAndCompare(ca))
}

func TestReplicaReadAccessPathByLearnerCase(t *testing.T) {
	s := new(testReplicaSelectorSuite)
	s.SetupTest(t)
	defer s.TearDownTest()

	// Add a TiKV learner peer to the region.
	rc := s.getRegion()
	storeID := uint64(4)
	s.cluster.AddStore(storeID, fmt.Sprintf("store%d", storeID))
	s.cluster.AddLearner(rc.meta.Id, storeID, s.cluster.AllocID())
	rc.invalidate(Other) // invalid region cache to reload region.

	ca := replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadLearner,
		accessErr: []RegionErrorType{ServerIsBusyErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store4, replica-read: true, stale-read: false}",
				"{addr: store1, replica-read: true, stale-read: false}",
			},
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      0,
			backoffDetail:   []string{},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))
}

func TestReplicaReadAccessPathByGenError(t *testing.T) {
	s := new(testReplicaSelectorSuite)
	s.SetupTest(t)
	defer s.TearDownTest()

	maxAccessErrCnt := 6
	if israce.RaceEnabled {
		// When run this test with race, it will take a long time, so we reduce the maxAccessErrCnt to 3 to speed up test to avoid timeout.
		maxAccessErrCnt = 3
	}
	totalValidCaseCount := 0
	totalCaseCount := 0
	lastLogCnt := 0
	testCase := func(req tikvrpc.CmdType, readType kv.ReplicaReadType, staleRead bool, timeout time.Duration, busyThresholdMs uint32, label *metapb.StoreLabel) {
		isRead := isReadReq(req)
		accessErrGen := newAccessErrGenerator(isRead, staleRead, maxAccessErrCnt)
		for {
			accessErr, done := accessErrGen.genAccessErr(staleRead)
			if done {
				break
			}
			ca := replicaSelectorAccessPathCase{
				reqType:         req,
				readType:        readType,
				staleRead:       staleRead,
				timeout:         timeout,
				busyThresholdMs: busyThresholdMs,
				label:           label,
				accessErr:       accessErr,
			}
			valid := s.runCaseAndCompare(ca)
			if valid {
				totalValidCaseCount++
			}
			totalCaseCount++
			if totalCaseCount-lastLogCnt > 100000 {
				lastLogCnt = totalCaseCount
				logutil.BgLogger().Info("TestReplicaReadAccessPathByGenError is running",
					zap.Int("total-case", totalCaseCount),
					zap.Int("valid-case", totalValidCaseCount),
					zap.Int("invalid-case", totalCaseCount-totalValidCaseCount),
					zap.String("req", req.String()),
					zap.String("read-type", readType.String()),
					zap.Bool("stale-read", staleRead),
					zap.Duration("timeout", timeout),
					zap.Any("label", label),
				)
			}
		}
	}

	testCase(tikvrpc.CmdPrewrite, kv.ReplicaReadLeader, false, 0, 0, nil)
	testCase(tikvrpc.CmdPrewrite, kv.ReplicaReadLeader, false, 0, 10, nil)
	testCase(tikvrpc.CmdGet, kv.ReplicaReadLeader, false, 0, 0, nil)
	testCase(tikvrpc.CmdGet, kv.ReplicaReadLeader, false, 0, 10, nil)
	testCase(tikvrpc.CmdGet, kv.ReplicaReadFollower, false, 0, 0, nil)
	testCase(tikvrpc.CmdGet, kv.ReplicaReadPreferLeader, false, 0, 0, nil)
	testCase(tikvrpc.CmdGet, kv.ReplicaReadMixed, false, 0, 0, nil)
	testCase(tikvrpc.CmdGet, kv.ReplicaReadLeader, false, time.Second, 0, nil)
	testCase(tikvrpc.CmdGet, kv.ReplicaReadMixed, false, time.Second, 0, nil)
	testCase(tikvrpc.CmdGet, kv.ReplicaReadMixed, false, time.Second, 0, &metapb.StoreLabel{Key: "id", Value: "1"})
	testCase(tikvrpc.CmdGet, kv.ReplicaReadMixed, false, time.Second, 0, &metapb.StoreLabel{Key: "id", Value: "2"})
	testCase(tikvrpc.CmdGet, kv.ReplicaReadMixed, false, time.Second, 0, &metapb.StoreLabel{Key: "id", Value: "3"})
	testCase(tikvrpc.CmdGet, kv.ReplicaReadMixed, true, 0, 0, nil)
	testCase(tikvrpc.CmdGet, kv.ReplicaReadMixed, true, 0, 0, &metapb.StoreLabel{Key: "id", Value: "1"})
	testCase(tikvrpc.CmdGet, kv.ReplicaReadMixed, true, 0, 0, &metapb.StoreLabel{Key: "id", Value: "2"})
	testCase(tikvrpc.CmdGet, kv.ReplicaReadMixed, true, 0, 0, &metapb.StoreLabel{Key: "id", Value: "3"})
	testCase(tikvrpc.CmdGet, kv.ReplicaReadMixed, true, time.Second, 0, &metapb.StoreLabel{Key: "id", Value: "1"})
	testCase(tikvrpc.CmdGet, kv.ReplicaReadMixed, true, time.Second, 0, &metapb.StoreLabel{Key: "id", Value: "2"})
	testCase(tikvrpc.CmdGet, kv.ReplicaReadMixed, true, time.Second, 0, &metapb.StoreLabel{Key: "id", Value: "3"})

	// Test for forwarding proxy.
	s.cache.enableForwarding = true
	testCase(tikvrpc.CmdPrewrite, kv.ReplicaReadLeader, false, 0, 0, nil)
	testCase(tikvrpc.CmdPrewrite, kv.ReplicaReadLeader, false, 0, 10, nil)
	testCase(tikvrpc.CmdGet, kv.ReplicaReadLeader, false, 0, 0, nil)
	testCase(tikvrpc.CmdGet, kv.ReplicaReadLeader, false, 0, 10, nil)

	logutil.BgLogger().Info("TestReplicaReadAccessPathByGenError Finished",
		zap.Int("total-case", totalCaseCount),
		zap.Int("valid-case", totalValidCaseCount),
		zap.Int("invalid-case", totalCaseCount-totalValidCaseCount))
}

func (s *testReplicaSelectorSuite) changeRegionLeader(storeId uint64) {
	loc, err := s.cache.LocateKey(s.bo, []byte("key"))
	s.Nil(err)
	rc := s.cache.GetCachedRegionWithRLock(loc.Region)
	for _, peer := range rc.meta.Peers {
		if peer.StoreId == storeId {
			s.cluster.ChangeLeader(rc.meta.Id, peer.Id)
		}
	}
	// Invalidate region cache to reload.
	s.cache.InvalidateCachedRegion(loc.Region)
}

func (s *testReplicaSelectorSuite) runCaseAndCompare(ca1 replicaSelectorAccessPathCase) bool {
	ca2 := ca1
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.EnableReplicaSelectorV2 = false
	})
	sender := ca1.run(s)
	ca1.checkResult(s, "v1", sender)

	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.EnableReplicaSelectorV2 = true
	})
	sender = ca2.run(s)
	if ca2.expect == nil {
		// compare with ca1 result.
		ca2.expect = &ca1.result
	}
	ca2.checkResult(s, "v2", sender)
	return !ca1.accessErrInValid
}

func (s *testReplicaSelectorSuite) runCase(ca replicaSelectorAccessPathCase, v2 bool) bool {
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.EnableReplicaSelectorV2 = v2
	})
	sender := ca.run(s)
	version := "v1"
	if v2 {
		version = "v2"
	}
	ca.checkResult(s, version, sender)
	return !ca.accessErrInValid
}

func (s *testReplicaSelectorSuite) runMultiCaseAndCompare(cas []replicaSelectorAccessPathCase) bool {
	expects := make([]accessPathResult, 0, len(cas))
	valid := true
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.EnableReplicaSelectorV2 = false
	})
	for _, ca1 := range cas {
		sender := ca1.run(s)
		ca1.checkResult(s, "v1", sender)
		expects = append(expects, ca1.result)
		valid = valid && !ca1.accessErrInValid
	}

	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.EnableReplicaSelectorV2 = true
	})
	for i, ca2 := range cas {
		sender := ca2.run(s)
		if ca2.expect == nil {
			// compare with ca1 result.
			ca2.expect = &expects[i]
		}
		ca2.checkResult(s, "v2", sender)
		valid = valid && !ca2.accessErrInValid
	}
	return valid
}

func (ca *replicaSelectorAccessPathCase) checkResult(s *testReplicaSelectorSuite, version string, sender *RegionRequestSender) {
	if ca.expect == nil {
		return
	}
	msg := fmt.Sprintf("enable_forwarding: %v\nversion: %v\n%v\nsender: %v\n", s.cache.enableForwarding, version, ca.Format(), sender.String())
	expect := ca.expect
	result := ca.result
	s.Equal(expect.accessPath, result.accessPath, msg)
	s.Equal(expect.respErr, result.respErr, msg)
	s.Equal(expect.respRegionError, result.respRegionError, msg)
	s.Equal(expect.regionIsValid, result.regionIsValid, msg)
	s.Equal(expect.backoffCnt, result.backoffCnt, msg)
	s.Equal(expect.backoffDetail, result.backoffDetail, msg)
}

func (ca *replicaSelectorAccessPathCase) run(s *testReplicaSelectorSuite) *RegionRequestSender {
	access := []string{}
	fnClient := &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (response *tikvrpc.Response, err error) {
		idx := len(access)
		if req.ForwardedHost == "" {
			access = append(access, fmt.Sprintf("{addr: %v, replica-read: %v, stale-read: %v}", addr, req.ReplicaRead, req.StaleRead))
		} else {
			access = append(access, fmt.Sprintf("{addr: %v, replica-read: %v, stale-read: %v, forward_addr: %v}", addr, req.ReplicaRead, req.StaleRead, req.ForwardedHost))
			addr = req.ForwardedHost
		}
		if idx < len(ca.accessErr) {
			if !ca.accessErr[idx].Valid(addr, req) {
				// mark this case is invalid. just ignore this case.
				ca.accessErrInValid = true
			} else {
				rc := s.getRegion()
				s.NotNil(rc)
				regionErr, err := ca.genAccessErr(s.cache, rc, ca.accessErr[idx])
				if regionErr != nil {
					return &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{
						RegionError: regionErr,
					}}, nil
				}
				if err != nil {
					return nil, err
				}
			}
		}
		return &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{
			Value: []byte("hello world"),
		}}, nil
	}}
	sender := NewRegionRequestSender(s.cache, fnClient)
	req, opts, timeout := ca.buildRequest(s)
	beforeRun(s, ca)
	rc := s.getRegion()
	s.NotNil(rc)
	bo := retry.NewBackofferWithVars(context.Background(), 40000, nil)
	resp, _, _, err := sender.SendReqCtx(bo, req, rc.VerID(), timeout, tikvrpc.TiKV, opts...)
	ca.recordResult(s, bo, sender.replicaSelector.getBaseReplicaSelector().region, access, resp, err)
	afterRun(ca, sender)
	return sender
}

func beforeRun(s *testReplicaSelectorSuite, ca *replicaSelectorAccessPathCase) {
	if ca.beforeRun != nil {
		ca.beforeRun()
	} else {
		s.resetStoreState()
	}
}

func afterRun(ca *replicaSelectorAccessPathCase, sender *RegionRequestSender) {
	if ca.afterRun != nil {
		ca.afterRun()
	} else {
		sender.replicaSelector.invalidateRegion() // invalidate region to reload for next test case.
	}
}

func (ca *replicaSelectorAccessPathCase) buildRequest(s *testReplicaSelectorSuite) (*tikvrpc.Request, []StoreSelectorOption, time.Duration) {
	var req *tikvrpc.Request
	switch ca.reqType {
	case tikvrpc.CmdGet:
		req = tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{
			Key: []byte("key"),
		})
	case tikvrpc.CmdPrewrite:
		req = tikvrpc.NewRequest(tikvrpc.CmdPrewrite, &kvrpcpb.PrewriteRequest{})
	default:
		s.FailNow("unsupported reqType " + ca.reqType.String())
	}
	if ca.staleRead {
		req.EnableStaleWithMixedReplicaRead()
		req.ReadReplicaScope = oracle.GlobalTxnScope
		req.TxnScope = oracle.GlobalTxnScope
	} else {
		req.ReplicaReadType = ca.readType
		req.ReplicaRead = ca.readType.IsFollowerRead()
	}
	if ca.busyThresholdMs > 0 {
		req.BusyThresholdMs = ca.busyThresholdMs
	}
	opts := []StoreSelectorOption{}
	if ca.label != nil {
		opts = append(opts, WithMatchLabels([]*metapb.StoreLabel{ca.label}))
	}
	timeout := ca.timeout
	if timeout == 0 {
		timeout = client.ReadTimeoutShort
	}
	return req, opts, timeout
}

func (ca *replicaSelectorAccessPathCase) recordResult(s *testReplicaSelectorSuite, bo *retry.Backoffer, region *Region, access []string, resp *tikvrpc.Response, err error) {
	ca.result.accessPath = access
	ca.result.regionIsValid = region.isValid()
	msg := ca.Format()
	if err == nil {
		s.NotNil(resp, msg)
		regionErr, err := resp.GetRegionError()
		s.Nil(err, msg)
		ca.result.respRegionError = regionErr
	} else {
		ca.result.respErr = err.Error()
	}
	ca.result.backoffCnt = bo.GetTotalBackoffTimes()
	detail := make([]string, 0, len(bo.GetBackoffTimes()))
	for tp, cnt := range bo.GetBackoffTimes() {
		detail = append(detail, fmt.Sprintf("%v+%v", tp, cnt))
	}
	sort.Strings(detail)
	ca.result.backoffDetail = detail
}

func (ca *replicaSelectorAccessPathCase) genAccessErr(regionCache *RegionCache, r *Region, accessErr RegionErrorType) (regionErr *errorpb.Error, err error) {
	genNotLeaderErr := func(storeID uint64) *errorpb.Error {
		var peerInStore *metapb.Peer
		for _, peer := range r.meta.Peers {
			if peer.StoreId == storeID {
				peerInStore = peer
				break
			}
		}
		return &errorpb.Error{
			NotLeader: &errorpb.NotLeader{
				RegionId: r.meta.Id,
				Leader:   peerInStore,
			},
		}
	}
	switch accessErr {
	case NotLeaderWithNewLeader1Err:
		regionErr = genNotLeaderErr(1)
	case NotLeaderWithNewLeader2Err:
		regionErr = genNotLeaderErr(2)
	case NotLeaderWithNewLeader3Err:
		regionErr = genNotLeaderErr(3)
	default:
		regionErr, err = accessErr.GenError()
	}
	if err != nil {
		// inject unreachable liveness.
		unreachable.injectConstantLiveness(regionCache)
	}
	return regionErr, err
}

func (c *replicaSelectorAccessPathCase) Format() string {
	label := ""
	if c.label != nil {
		label = fmt.Sprintf("%v->%v", c.label.Key, c.label.Value)
	}
	respRegionError := ""
	if c.result.respRegionError != nil {
		respRegionError = c.result.respRegionError.String()
	}
	accessErr := make([]string, len(c.accessErr))
	for i := range c.accessErr {
		accessErr[i] = c.accessErr[i].String()
	}
	return fmt.Sprintf("{\n"+
		"\treq: %v\n"+
		"\tread_type: %v\n"+
		"\tstale_read: %v\n"+
		"\ttimeout: %v\n"+
		"\tbusy_threshold_ms: %v\n"+
		"\tlabel: %v\n"+
		"\taccess_err: %v\n"+
		"\taccess_path: %v\n"+
		"\tresp_err: %v\n"+
		"\tresp_region_err: %v\n"+
		"\tbackoff_cnt: %v\n"+
		"\tbackoff_detail: %v\n"+
		"\tregion_is_valid: %v\n}",
		c.reqType, c.readType, c.staleRead, c.timeout, c.busyThresholdMs, label, strings.Join(accessErr, ", "), strings.Join(c.result.accessPath, ", "),
		c.result.respErr, respRegionError, c.result.backoffCnt, strings.Join(c.result.backoffDetail, ", "), c.result.regionIsValid)
}

func (s *testReplicaSelectorSuite) resetStoreState() {
	// reset slow score, since serverIsBusyErr will mark the store is slow, and affect remaining test cases.
	reachable.injectConstantLiveness(s.cache) // inject reachable liveness.
	rc := s.getRegion()
	s.NotNil(rc)
	for _, store := range rc.getStore().stores {
		store.loadStats.Store(nil)
		store.healthStatus.clientSideSlowScore.resetSlowScore()
		store.healthStatus.updateTiKVServerSideSlowScore(0, time.Now())
		store.healthStatus.updateSlowFlag()
		atomic.StoreUint32(&store.livenessState, uint32(reachable))
		store.setResolveState(resolved)
	}
	regionStore := rc.getStore()
	for _, storeIdx := range regionStore.accessIndex[tiKVOnly] {
		epoch := regionStore.storeEpochs[storeIdx]
		storeEpoch := regionStore.stores[storeIdx].epoch
		if epoch != storeEpoch {
			rc.invalidate(EpochNotMatch)
			break
		}
	}
}

type RegionErrorType int

const (
	NotLeaderErr RegionErrorType = iota + 1
	NotLeaderWithNewLeader1Err
	NotLeaderWithNewLeader2Err
	NotLeaderWithNewLeader3Err
	RegionNotFoundErr
	KeyNotInRegionErr
	EpochNotMatchErr
	ServerIsBusyErr
	ServerIsBusyWithEstimatedWaitMsErr
	StaleCommandErr
	StoreNotMatchErr
	RaftEntryTooLargeErr
	MaxTimestampNotSyncedErr
	ReadIndexNotReadyErr
	ProposalInMergingModeErr
	DataIsNotReadyErr
	RegionNotInitializedErr
	DiskFullErr
	RecoveryInProgressErr
	FlashbackInProgressErr
	FlashbackNotPreparedErr
	IsWitnessErr
	MismatchPeerIdErr
	BucketVersionNotMatchErr
	// following error type is not region error.
	DeadLineExceededErr
	RegionErrorTypeMax
)

func (tp RegionErrorType) GenRegionError() *errorpb.Error {
	err := &errorpb.Error{}
	switch tp {
	case NotLeaderErr:
		err.NotLeader = &errorpb.NotLeader{}
	case RegionNotFoundErr:
		err.RegionNotFound = &errorpb.RegionNotFound{}
	case KeyNotInRegionErr:
		err.KeyNotInRegion = &errorpb.KeyNotInRegion{}
	case EpochNotMatchErr:
		err.EpochNotMatch = &errorpb.EpochNotMatch{}
	case ServerIsBusyErr:
		err.ServerIsBusy = &errorpb.ServerIsBusy{}
	case ServerIsBusyWithEstimatedWaitMsErr:
		err.ServerIsBusy = &errorpb.ServerIsBusy{EstimatedWaitMs: 10}
	case StaleCommandErr:
		err.StaleCommand = &errorpb.StaleCommand{}
	case StoreNotMatchErr:
		err.StoreNotMatch = &errorpb.StoreNotMatch{}
	case RaftEntryTooLargeErr:
		err.RaftEntryTooLarge = &errorpb.RaftEntryTooLarge{}
	case MaxTimestampNotSyncedErr:
		err.MaxTimestampNotSynced = &errorpb.MaxTimestampNotSynced{}
	case ReadIndexNotReadyErr:
		err.ReadIndexNotReady = &errorpb.ReadIndexNotReady{}
	case ProposalInMergingModeErr:
		err.ProposalInMergingMode = &errorpb.ProposalInMergingMode{}
	case DataIsNotReadyErr:
		err.DataIsNotReady = &errorpb.DataIsNotReady{}
	case RegionNotInitializedErr:
		err.RegionNotInitialized = &errorpb.RegionNotInitialized{}
	case DiskFullErr:
		err.DiskFull = &errorpb.DiskFull{}
	case RecoveryInProgressErr:
		err.RecoveryInProgress = &errorpb.RecoveryInProgress{}
	case FlashbackInProgressErr:
		err.FlashbackInProgress = &errorpb.FlashbackInProgress{}
	case FlashbackNotPreparedErr:
		err.FlashbackNotPrepared = &errorpb.FlashbackNotPrepared{}
	case IsWitnessErr:
		err.IsWitness = &errorpb.IsWitness{}
	case MismatchPeerIdErr:
		err.MismatchPeerId = &errorpb.MismatchPeerId{}
	case BucketVersionNotMatchErr:
		err.BucketVersionNotMatch = &errorpb.BucketVersionNotMatch{}
	default:
		return nil
	}
	return err
}

func (tp RegionErrorType) GenError() (*errorpb.Error, error) {
	regionErr := tp.GenRegionError()
	if regionErr != nil {
		return regionErr, nil
	}
	switch tp {
	case DeadLineExceededErr:
		return nil, context.DeadlineExceeded
	}
	return nil, nil
}

func (tp RegionErrorType) Valid(addr string, req *tikvrpc.Request) bool {
	// leader-read.
	if !req.StaleRead && !req.ReplicaRead {
		switch tp {
		case DataIsNotReadyErr:
			// DataIsNotReadyErr only return when req is a stale read.
			return false
		}
	}
	// replica-read.
	if !req.StaleRead && req.ReplicaRead {
		switch tp {
		case NotLeaderErr, NotLeaderWithNewLeader1Err, NotLeaderWithNewLeader2Err, NotLeaderWithNewLeader3Err:
			// NotLeaderErr will not return in replica read.
			return false
		case DataIsNotReadyErr:
			// DataIsNotReadyErr only return when req is a stale read.
			return false
		}
	}
	// stale-read.
	if req.StaleRead && !req.ReplicaRead {
		switch tp {
		case NotLeaderErr, NotLeaderWithNewLeader1Err, NotLeaderWithNewLeader2Err, NotLeaderWithNewLeader3Err:
			// NotLeaderErr will not return in stale read.
			return false
		}
	}
	// store1 can't return a not leader error with new leader in store1.
	if addr == "store1" && tp == NotLeaderWithNewLeader1Err {
		return false
	}
	// ditto.
	if addr == "store2" && tp == NotLeaderWithNewLeader2Err {
		return false
	}
	// ditto.
	if addr == "store3" && tp == NotLeaderWithNewLeader3Err {
		return false
	}
	return true
}

func (tp RegionErrorType) String() string {
	switch tp {
	case NotLeaderErr:
		return "NotLeaderErr"
	case NotLeaderWithNewLeader1Err:
		return "NotLeaderWithNewLeader1Err"
	case NotLeaderWithNewLeader2Err:
		return "NotLeaderWithNewLeader2Err"
	case NotLeaderWithNewLeader3Err:
		return "NotLeaderWithNewLeader3Err"
	case RegionNotFoundErr:
		return "RegionNotFoundErr"
	case KeyNotInRegionErr:
		return "KeyNotInRegionErr"
	case EpochNotMatchErr:
		return "EpochNotMatchErr"
	case ServerIsBusyErr:
		return "ServerIsBusyErr"
	case ServerIsBusyWithEstimatedWaitMsErr:
		return "ServerIsBusyWithEstimatedWaitMsErr"
	case StaleCommandErr:
		return "StaleCommandErr"
	case StoreNotMatchErr:
		return "StoreNotMatchErr"
	case RaftEntryTooLargeErr:
		return "RaftEntryTooLargeErr"
	case MaxTimestampNotSyncedErr:
		return "MaxTimestampNotSyncedErr"
	case ReadIndexNotReadyErr:
		return "ReadIndexNotReadyErr"
	case ProposalInMergingModeErr:
		return "ProposalInMergingModeErr"
	case DataIsNotReadyErr:
		return "DataIsNotReadyErr"
	case RegionNotInitializedErr:
		return "RegionNotInitializedErr"
	case DiskFullErr:
		return "DiskFullErr"
	case RecoveryInProgressErr:
		return "RecoveryInProgressErr"
	case FlashbackInProgressErr:
		return "FlashbackInProgressErr"
	case FlashbackNotPreparedErr:
		return "FlashbackNotPreparedErr"
	case IsWitnessErr:
		return "IsWitnessErr"
	case MismatchPeerIdErr:
		return "MismatchPeerIdErr"
	case BucketVersionNotMatchErr:
		return "BucketVersionNotMatchErr"
	case DeadLineExceededErr:
		return "DeadLineExceededErr"
	default:
		return "unknown_" + strconv.Itoa(int(tp))
	}
}

type accessErrGenerator struct {
	maxAccessErrCnt int
	mode            int
	idx             int
	baseIdx         int
	allErrs         []RegionErrorType
	retryErrs       []RegionErrorType
}

func newAccessErrGenerator(isRead, staleRead bool, maxAccessErrCnt int) *accessErrGenerator {
	filter := func(tp RegionErrorType) bool {
		// read request won't meet RaftEntryTooLargeErr.
		if isRead && tp == RaftEntryTooLargeErr {
			return false
		}
		if staleRead == false && tp == DataIsNotReadyErr {
			return false
		}
		// TODO: since v2 has come compatibility issue with v1, so skip FlashbackInProgressErr.
		if tp == FlashbackInProgressErr {
			return false
		}
		return true
	}
	allErrs := getAllRegionErrors(filter)
	retryableErr := getAllRegionErrors(func(tp RegionErrorType) bool {
		return filter(tp) && isRegionErrorRetryable(tp)
	})
	return &accessErrGenerator{
		maxAccessErrCnt: maxAccessErrCnt,
		mode:            0,
		idx:             0,
		allErrs:         allErrs,
		retryErrs:       retryableErr,
	}
}

func getAllRegionErrors(filter func(errorType RegionErrorType) bool) []RegionErrorType {
	errs := make([]RegionErrorType, 0, int(RegionErrorTypeMax))
	for tp := NotLeaderErr; tp < RegionErrorTypeMax; tp++ {
		if filter != nil && filter(tp) == false {
			continue
		}
		errs = append(errs, tp)
	}
	return errs
}

func isRegionErrorRetryable(tp RegionErrorType) bool {
	switch tp {
	case NotLeaderErr, NotLeaderWithNewLeader1Err, NotLeaderWithNewLeader2Err, NotLeaderWithNewLeader3Err, ServerIsBusyErr, ServerIsBusyWithEstimatedWaitMsErr,
		StaleCommandErr, MaxTimestampNotSyncedErr, ReadIndexNotReadyErr, ProposalInMergingModeErr, DataIsNotReadyErr,
		RegionNotInitializedErr, FlashbackInProgressErr, DiskFullErr, DeadLineExceededErr:
		return true
	}
	return false
}

func (a *accessErrGenerator) genAccessErr(staleRead bool) ([]RegionErrorType, bool) {
	if a.mode == 0 {
		a.idx = 0
		a.mode = 1
		return nil, false
	}
	if a.mode == 1 {
		idx := a.idx
		a.idx++
		if a.idx >= len(a.allErrs) {
			a.idx = 0
			a.baseIdx = 0
			a.mode = 2
		}
		return []RegionErrorType{a.allErrs[idx]}, false
	}
	for a.mode <= a.maxAccessErrCnt {
		errs := a.genAccessErrs(a.allErrs, a.retryErrs)
		if len(errs) > 0 {
			return errs, false
		}
		a.baseIdx = 0
		a.idx = 0
		a.mode++
		// if mode >= 4 , reduce the error type to avoid generating too many combinations.
		if a.mode > 4 {
			if a.mode > 8 {
				a.allErrs = []RegionErrorType{ServerIsBusyErr, ServerIsBusyWithEstimatedWaitMsErr}
			} else if a.mode > 7 {
				a.allErrs = []RegionErrorType{ServerIsBusyWithEstimatedWaitMsErr, DeadLineExceededErr}
			} else if a.mode > 6 {
				a.allErrs = []RegionErrorType{NotLeaderWithNewLeader2Err, ServerIsBusyWithEstimatedWaitMsErr, DeadLineExceededErr}
			} else if a.mode > 5 {
				a.allErrs = []RegionErrorType{NotLeaderErr, NotLeaderWithNewLeader2Err, ServerIsBusyWithEstimatedWaitMsErr, DeadLineExceededErr}
			} else {
				a.allErrs = []RegionErrorType{NotLeaderErr, NotLeaderWithNewLeader1Err, NotLeaderWithNewLeader2Err, NotLeaderWithNewLeader3Err, ServerIsBusyWithEstimatedWaitMsErr, RegionNotInitializedErr, DeadLineExceededErr}
			}
			if staleRead {
				a.allErrs = append(a.allErrs, DataIsNotReadyErr)
			}
			a.retryErrs = a.allErrs
		}
	}
	return nil, true
}

func (a *accessErrGenerator) genAccessErrs(allErrs, retryErrs []RegionErrorType) []RegionErrorType {
	defer func() {
		a.baseIdx++
		if a.baseIdx >= len(allErrs) {
			a.baseIdx = 0
			a.idx++
		}
	}()
	mode := a.mode
	errs := make([]RegionErrorType, mode)
	errs[mode-1] = allErrs[a.baseIdx%len(allErrs)]
	value := a.idx
	for i := mode - 2; i >= 0; i-- {
		if i == 0 && value > len(retryErrs) {
			return nil
		}
		errs[i] = retryErrs[value%len(retryErrs)]
		value = value / len(retryErrs)
	}
	return errs
}

func (s *testReplicaSelectorSuite) getRegion() *Region {
	for i := 0; i < 100; i++ {
		loc, err := s.cache.LocateKey(s.bo, []byte("key"))
		s.Nil(err)
		rc := s.cache.GetCachedRegionWithRLock(loc.Region)
		if rc == nil {
			time.Sleep(time.Millisecond * 10)
			continue
		}
		return rc
	}
	return nil
}

func TestTiKVClientReadTimeout(t *testing.T) {
	if israce.RaceEnabled {
		t.Skip("the test run with race will failed, so skip it")
	}
	config.UpdateGlobal(func(conf *config.Config) {
		// enable batch client.
		conf.TiKVClient.MaxBatchSize = 128
	})()
	s := new(testReplicaSelectorSuite)
	s.SetupTest(t)
	defer s.TearDownTest()

	server, port := mockserver.StartMockTikvService()
	s.True(port > 0)
	server.SetMetaChecker(func(ctx context.Context) error {
		time.Sleep(time.Millisecond * 10)
		return nil
	})
	rpcClient := client.NewRPCClient()
	defer func() {
		rpcClient.Close()
		server.Stop()
	}()

	accessPath := []string{}
	fnClient := &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (response *tikvrpc.Response, err error) {
		accessPath = append(accessPath, addr)
		accessPath = append(accessPath, fmt.Sprintf("{addr: %v, replica-read: %v, stale-read: %v, timeout: %v}", addr, req.ReplicaRead, req.StaleRead, req.MaxExecutionDurationMs))
		return rpcClient.SendRequest(ctx, server.Addr(), req, timeout)
	}}
	rc := s.getRegion()
	s.NotNil(rc)
	req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("key"), Version: 1})
	req.ReplicaReadType = kv.ReplicaReadLeader
	req.MaxExecutionDurationMs = 1
	bo := retry.NewBackofferWithVars(context.Background(), 2000, nil)
	sender := NewRegionRequestSender(s.cache, fnClient)
	resp, _, err := sender.SendReq(bo, req, rc.VerID(), time.Millisecond)
	s.Nil(err)
	s.NotNil(resp)
	regionErr, _ := resp.GetRegionError()
	s.True(IsFakeRegionError(regionErr))
	s.Equal(0, bo.GetTotalBackoffTimes())
	s.Equal([]string{
		"store1", "{addr: store1, replica-read: false, stale-read: false, timeout: 1}",
		"store2", "{addr: store2, replica-read: true, stale-read: false, timeout: 1}",
		"store3", "{addr: store3, replica-read: true, stale-read: false, timeout: 1}",
	}, accessPath)
	// clear max execution duration for retry.
	req.MaxExecutionDurationMs = 0
	sender = NewRegionRequestSender(s.cache, fnClient)
	resp, _, err = sender.SendReq(bo, req, rc.VerID(), time.Second) // use a longer timeout.
	s.Nil(err)
	s.NotNil(resp)
	regionErr, _ = resp.GetRegionError()
	s.Nil(regionErr)
	s.Equal(0, bo.GetTotalBackoffTimes())
	s.Equal([]string{
		"store1", "{addr: store1, replica-read: false, stale-read: false, timeout: 1}",
		"store2", "{addr: store2, replica-read: true, stale-read: false, timeout: 1}",
		"store3", "{addr: store3, replica-read: true, stale-read: false, timeout: 1}",
		"store1", "{addr: store1, replica-read: true, stale-read: false, timeout: 1000}",
	}, accessPath)
}

func BenchmarkReplicaSelector(b *testing.B) {
	mvccStore := mocktikv.MustNewMVCCStore()
	cluster := mocktikv.NewCluster(mvccStore)
	mocktikv.BootstrapWithMultiStores(cluster, 3)
	pdCli := &CodecPDClient{mocktikv.NewPDClient(cluster), apicodec.NewCodecV1(apicodec.ModeTxn)}
	cache := NewRegionCache(pdCli)
	defer func() {
		cache.Close()
		mvccStore.Close()
	}()

	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.EnableReplicaSelectorV2 = true
	})
	cnt := 0
	allErrs := getAllRegionErrors(nil)
	fnClient := &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (response *tikvrpc.Response, err error) {
		pberr, err := allErrs[cnt%len(allErrs)].GenError()
		cnt++
		return &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{
			RegionError: pberr,
			Value:       []byte("value"),
		}}, err
	}}
	f, _ := os.Create("cpu.profile")
	pprof.StartCPUProfile(f)
	defer func() {
		pprof.StopCPUProfile()
		f.Close()
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bo := retry.NewBackofferWithVars(context.Background(), 40000, nil)
		req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{
			Key: []byte("key"),
		})
		req.ReplicaReadType = kv.ReplicaReadMixed
		loc, err := cache.LocateKey(bo, []byte("key"))
		if err != nil {
			b.Fail()
		}
		sender := NewRegionRequestSender(cache, fnClient)
		sender.SendReqCtx(bo, req, loc.Region, client.ReadTimeoutShort, tikvrpc.TiKV)
	}
}
