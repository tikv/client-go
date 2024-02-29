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
	"github.com/stretchr/testify/suite"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/config/retry"
	"github.com/tikv/client-go/v2/internal/apicodec"
	"github.com/tikv/client-go/v2/internal/client"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/internal/mockstore/mocktikv"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikvrpc"
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
	label            *metapb.StoreLabel
	accessErr        []RegionErrorType
	accessErrInValid bool
	accessPathResult                   // use to record the execution result.
	expect           *accessPathResult //
}

type accessPathResult struct {
	accessPath      []string
	respErr         string
	respRegionError *errorpb.Error
	backoffCnt      int
	backoffDetail   []string
	regionIsValid   bool
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
			s.Equal(score, 4)
		} else {
			s.Equal(score, 5)
		}
		r.store.healthStatus.markAlreadySlow()
		s.Equal(r.store.healthStatus.IsSlow(), true)
		score = strategy.calculateScore(r, isLeader)
		if isLeader {
			s.Equal(score, 3)
		} else {
			s.Equal(score, 4)
		}
		strategy.tryLeader = true
		score = strategy.calculateScore(r, isLeader)
		s.Equal(score, 4)
		strategy.preferLeader = true
		score = strategy.calculateScore(r, isLeader)
		if isLeader {
			s.Equal(score, 5)
		} else {
			s.Equal(score, 4)
		}
		strategy.learnerOnly = true
		strategy.tryLeader = false
		strategy.preferLeader = false
		score = strategy.calculateScore(r, isLeader)
		s.Equal(score, 3)
		labels := []*metapb.StoreLabel{
			{
				Key:   "zone",
				Value: "us-west-1",
			},
		}
		strategy.labels = labels
		score = strategy.calculateScore(r, isLeader)
		s.Equal(score, 0)

		strategy = ReplicaSelectMixedStrategy{
			leaderIdx: rc.getStore().workTiKVIdx,
			tryLeader: true,
			labels:    labels,
		}
		score = strategy.calculateScore(r, isLeader)
		if isLeader {
			s.Equal(score, 2)
		} else {
			s.Equal(score, 1)
		}

		strategy = ReplicaSelectMixedStrategy{
			leaderIdx:    rc.getStore().workTiKVIdx,
			preferLeader: true,
			labels:       labels,
		}
		score = strategy.calculateScore(r, isLeader)
		if isLeader {
			s.Equal(score, 2)
		} else {
			s.Equal(score, 1)
		}
		r.store.labels = labels
		score = strategy.calculateScore(r, isLeader)
		if isLeader {
			s.Equal(score, 5)
		} else {
			s.Equal(score, 4)
		}
		r.store.labels = nil
	}
}

func TestReplicaReadAccessPathByCase(t *testing.T) {
	s := new(testReplicaSelectorSuite)
	s.SetupTest(t)
	defer s.TearDownTest()

	fakeEpochNotMatch := &errorpb.Error{EpochNotMatch: &errorpb.EpochNotMatch{}} // fake region error, cause by no replica is available.
	var ca replicaSelectorAccessPathCase
	ca = replicaSelectorAccessPathCase{
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
			backoffCnt:      1,
			backoffDetail:   []string{"tikvServerBusy+1"},
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
			backoffCnt:      2,
			backoffDetail:   []string{"tikvServerBusy+2"},
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
			backoffCnt:      2,
			backoffDetail:   []string{"tikvServerBusy+2"},
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
			backoffCnt:      2,
			backoffDetail:   []string{"tikvServerBusy+2"},
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
			backoffCnt:      1,
			backoffDetail:   []string{"tikvServerBusy+1"},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))
}

func TestReplicaReadAccessPathByCase2(t *testing.T) {
	s := new(testReplicaSelectorSuite)
	s.SetupTest(t)
	defer s.TearDownTest()

	var ca replicaSelectorAccessPathCase
	fakeEpochNotMatch := &errorpb.Error{EpochNotMatch: &errorpb.EpochNotMatch{}}
	// Following cases are found by other test, careful.
	ca = replicaSelectorAccessPathCase{
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
		accessErr: []RegionErrorType{DeadLineExceededErr, NotLeaderErr, NotLeaderWithNewLeader2Err, ServerIsBusyWithEstimatedWaitMsErr, DeadLineExceededErr},
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

func TestReplicaReadAccessPathNotCompatibleCase(t *testing.T) {
	s := new(testReplicaSelectorSuite)
	s.SetupTest(t)
	defer s.TearDownTest()

	var ca replicaSelectorAccessPathCase
	//fakeEpochNotMatch := &errorpb.Error{EpochNotMatch: &errorpb.EpochNotMatch{}}
	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadLeader,
		staleRead: false,
		timeout:   time.Second,
		label:     nil,
		accessErr: []RegionErrorType{DeadLineExceededErr, FlashbackInProgressErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store2, replica-read: false, stale-read: false}"},
			respErr:         "region 0 is in flashback progress, FlashbackStartTS is 0",
			respRegionError: nil,
			backoffCnt:      0,
			backoffDetail:   []string{},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))

}

func TestReplicaReadAccessPathBasic(t *testing.T) {
	s := new(testReplicaSelectorSuite)
	s.SetupTest(t)
	defer s.TearDownTest()

	retryableErrors := []RegionErrorType{ServerIsBusyErr, ServerIsBusyWithEstimatedWaitMsErr, StaleCommandErr, MaxTimestampNotSyncedErr, ProposalInMergingModeErr, ReadIndexNotReadyErr, RegionNotInitializedErr, DiskFullErr}
	noRetryErrors := []RegionErrorType{RegionNotFoundErr, KeyNotInRegionErr, EpochNotMatchErr, StoreNotMatchErr, RaftEntryTooLargeErr, RecoveryInProgressErr, FlashbackInProgressErr, FlashbackNotPreparedErr, IsWitnessErr, MismatchPeerIdErr, BucketVersionNotMatchErr}
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
						backoff = []string{"tikvServerBusy+1"}
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
					case FlashbackInProgressErr:
						respErr = "region 0 is in flashback progress, FlashbackStartTS is 0"
						respRegionError = nil
						regionIsValid = true
					case FlashbackNotPreparedErr:
						respErr = "region 0 is not prepared for the flashback"
						respRegionError = nil
						regionIsValid = true
					}
					accessPath := []string{"{addr: store1, replica-read: true, stale-read: false}"}
					switch readType {
					case kv.ReplicaReadLeader:
						accessPath = []string{"{addr: store1, replica-read: false, stale-read: false}"}
					case kv.ReplicaReadFollower:
						accessPath = []string{"{addr: store2, replica-read: true, stale-read: false}"}
						if tp == FlashbackInProgressErr {
							respErr = ""
							respRegionError = nil
							regionIsValid = true
							accessPath = []string{
								"{addr: store2, replica-read: true, stale-read: false}",
								"{addr: store1, replica-read: true, stale-read: false}",
							}
						}
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
	var ca replicaSelectorAccessPathCase
	ca = replicaSelectorAccessPathCase{
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
			backoffCnt:      11,
			backoffDetail:   []string{"tikvServerBusy+11"},
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
			backoffCnt:      3,
			backoffDetail:   []string{"tikvRPC+1", "tikvServerBusy+2"},
			regionIsValid:   false,
		},
	}
	s.True(s.runCaseAndCompare(ca))
}

func TestReplicaReadAccessPathByFollowerCase(t *testing.T) {
	s := new(testReplicaSelectorSuite)
	s.SetupTest(t)
	defer s.TearDownTest()

	fakeEpochNotMatch := &errorpb.Error{EpochNotMatch: &errorpb.EpochNotMatch{}}
	var ca replicaSelectorAccessPathCase
	ca = replicaSelectorAccessPathCase{
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
			backoffCnt:      2,
			backoffDetail:   []string{"tikvServerBusy+2"},
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
			backoffCnt:      3,
			backoffDetail:   []string{"tikvServerBusy+3"},
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
			backoffCnt:      2,
			backoffDetail:   []string{"tikvRPC+1", "tikvServerBusy+1"},
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
			backoffCnt:      1,
			backoffDetail:   []string{"tikvServerBusy+1"},
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
				backoffCnt:      2,
				backoffDetail:   []string{"tikvServerBusy+2"},
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
				backoffCnt:      1,
				backoffDetail:   []string{"tikvServerBusy+1"},
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
				backoffCnt:      2,
				backoffDetail:   []string{"tikvRPC+1", "tikvServerBusy+1"},
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
				backoffCnt:      1,
				backoffDetail:   []string{"tikvServerBusy+1"},
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
			backoffCnt:      2,
			backoffDetail:   []string{"tikvRPC+1", "tikvServerBusy+1"},
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
			backoffCnt:      2,
			backoffDetail:   []string{"tikvRPC+1", "tikvServerBusy+1"},
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
			backoffCnt:      2,
			backoffDetail:   []string{"tikvRPC+1", "tikvServerBusy+1"},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))
	s.changeRegionLeader(1)
}

func TestReplicaReadDebug(t *testing.T) {
	s := new(testReplicaSelectorSuite)
	s.SetupTest(t)
	defer s.TearDownTest()

	var ca replicaSelectorAccessPathCase
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
			backoffCnt:      2,
			backoffDetail:   []string{"tikvRPC+1", "tikvServerBusy+1"},
			regionIsValid:   true,
		},
	}
	s.True(s.runCaseAndCompare(ca))
	s.changeRegionLeader(1)
}

// TODO: Fix backoff cnt.
func TestReplicaReadAccessPathByStaleReadCase(t *testing.T) {
	s := new(testReplicaSelectorSuite)
	s.SetupTest(t)
	defer s.TearDownTest()

	fakeEpochNotMatch := &errorpb.Error{EpochNotMatch: &errorpb.EpochNotMatch{}}
	var ca replicaSelectorAccessPathCase
	ca = replicaSelectorAccessPathCase{
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
			backoffCnt:      2,
			backoffDetail:   []string{"tikvServerBusy+2"},
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
			backoffCnt:      2,
			backoffDetail:   []string{"tikvServerBusy+2"},
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
			backoffCnt:      2,
			backoffDetail:   []string{"tikvServerBusy+2"},
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
			backoffCnt:      1,
			backoffDetail:   []string{"regionScheduling+1"},
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
			backoffCnt:      1,
			backoffDetail:   []string{"regionScheduling+1"},
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
}

func TestReplicaSelectorAccessPath2(t *testing.T) {
	s := new(testReplicaSelectorSuite)
	s.SetupTest(t)
	defer s.TearDownTest()

	totalValidCaseCount := 0
	totalCaseCount := 0
	testCase := func(req tikvrpc.CmdType, readType kv.ReplicaReadType, staleRead bool, timeout time.Duration, label *metapb.StoreLabel) {
		isRead := isReadReq(req)
		accessErrGen := newAccessErrGenerator(isRead, staleRead)
		for {
			accessErr, done := accessErrGen.genAccessErr(staleRead)
			if done {
				break
			}
			ca := replicaSelectorAccessPathCase{
				reqType:   req,
				readType:  readType,
				staleRead: staleRead,
				timeout:   timeout,
				label:     label,
				accessErr: accessErr,
			}
			valid := s.runCaseAndCompare(ca)
			if valid {
				totalValidCaseCount++
			}
			totalCaseCount++
		}
	}

	testCase(tikvrpc.CmdPrewrite, kv.ReplicaReadLeader, false, 0, nil)
	testCase(tikvrpc.CmdGet, kv.ReplicaReadLeader, false, 0, nil)
	testCase(tikvrpc.CmdGet, kv.ReplicaReadFollower, false, 0, nil)
	testCase(tikvrpc.CmdGet, kv.ReplicaReadPreferLeader, false, 0, nil)
	testCase(tikvrpc.CmdGet, kv.ReplicaReadMixed, false, 0, nil)
	testCase(tikvrpc.CmdGet, kv.ReplicaReadLeader, false, time.Second, nil)
	testCase(tikvrpc.CmdGet, kv.ReplicaReadMixed, false, time.Second, nil)
	testCase(tikvrpc.CmdGet, kv.ReplicaReadMixed, false, time.Second, &metapb.StoreLabel{Key: "id", Value: "1"})
	testCase(tikvrpc.CmdGet, kv.ReplicaReadMixed, false, time.Second, &metapb.StoreLabel{Key: "id", Value: "2"})
	testCase(tikvrpc.CmdGet, kv.ReplicaReadMixed, false, time.Second, &metapb.StoreLabel{Key: "id", Value: "3"})
	testCase(tikvrpc.CmdGet, kv.ReplicaReadMixed, true, 0, nil)
	testCase(tikvrpc.CmdGet, kv.ReplicaReadMixed, true, 0, &metapb.StoreLabel{Key: "id", Value: "1"})
	testCase(tikvrpc.CmdGet, kv.ReplicaReadMixed, true, 0, &metapb.StoreLabel{Key: "id", Value: "2"})
	testCase(tikvrpc.CmdGet, kv.ReplicaReadMixed, true, 0, &metapb.StoreLabel{Key: "id", Value: "3"})
	testCase(tikvrpc.CmdGet, kv.ReplicaReadMixed, true, time.Second, &metapb.StoreLabel{Key: "id", Value: "1"})
	testCase(tikvrpc.CmdGet, kv.ReplicaReadMixed, true, time.Second, &metapb.StoreLabel{Key: "id", Value: "2"})
	testCase(tikvrpc.CmdGet, kv.ReplicaReadMixed, true, time.Second, &metapb.StoreLabel{Key: "id", Value: "3"})

	logutil.BgLogger().Info("TestReplicaSelectorAccessPath2 finished",
		zap.Int("total-case", totalCaseCount),
		zap.Int("valid-case", totalValidCaseCount),
		zap.Int("skipped-case", totalCaseCount-totalValidCaseCount))
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
		conf.EnableReplicaSelectorV2 = false
	})
	ca1.run(s)
	if ca1.accessErrInValid {
		// the case has been marked as invalid, just ignore it.
		return false
	}
	ca1.checkResult(s, false)

	config.UpdateGlobal(func(conf *config.Config) {
		conf.EnableReplicaSelectorV2 = true
	})
	ca2.run(s)
	ca2.checkResult(s, true)
	return true
}

func (ca *replicaSelectorAccessPathCase) checkResult(s *testReplicaSelectorSuite, v2 bool) {
	if ca.expect == nil {
		return
	}
	version := "v1"
	if v2 {
		version = "v2"
	}
	msg := fmt.Sprintf("%v\n%v\n\n", version, ca.Format())
	expect := ca.expect
	result := ca.accessPathResult
	s.Equal(expect.accessPath, result.accessPath, msg)
	s.Equal(expect.respErr, result.respErr, msg)
	s.Equal(expect.respRegionError, result.respRegionError, msg)
	s.Equal(expect.regionIsValid, result.regionIsValid, msg)
	if !v2 {
		// Currently, v2 has compatible backoff strategy with v1, so we don't compare backoff detail.
		s.Equal(expect.backoffCnt, result.backoffCnt, msg)
		s.Equal(expect.backoffDetail, result.backoffDetail, msg)
	}
}

func (ca *replicaSelectorAccessPathCase) run(s *testReplicaSelectorSuite) {
	access := []string{}
	fnClient := &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (response *tikvrpc.Response, err error) {
		idx := len(access)
		access = append(access, fmt.Sprintf("{addr: %v, replica-read: %v, stale-read: %v}", addr, req.ReplicaRead, req.StaleRead))
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
	rc := s.prepareBeforeRun()
	bo := retry.NewBackofferWithVars(context.Background(), 40000, nil)
	resp, _, _, err := sender.SendReqCtx(bo, req, rc.VerID(), timeout, tikvrpc.TiKV, opts...)
	ca.recordResult(s, bo, sender.replicaSelector.getBaseReplicaSelector().region, access, resp, err)
	sender.replicaSelector.invalidateRegion() // invalidate region to reload for next test case.
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
	ca.accessPath = access
	ca.regionIsValid = region.isValid()
	msg := ca.Format()
	if err == nil {
		s.NotNil(resp, msg)
		regionErr, err := resp.GetRegionError()
		s.Nil(err, msg)
		ca.respRegionError = regionErr
	} else {
		ca.respErr = err.Error()
	}
	ca.backoffCnt = bo.GetTotalBackoffTimes()
	detail := make([]string, 0, len(bo.GetBackoffTimes()))
	for tp, cnt := range bo.GetBackoffTimes() {
		detail = append(detail, fmt.Sprintf("%v+%v", tp, cnt))
	}
	sort.Strings(detail)
	ca.backoffDetail = detail
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
	if c.respRegionError != nil {
		respRegionError = c.respRegionError.String()
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
		"\tlabel: %v\n"+
		"\taccess_err: %v\n"+
		"\taccess_path: %v\n"+
		"\tresp_err: %v\n"+
		"\tresp_region_err: %v\n"+
		"\tbackoff_cnt: %v\n"+
		"\tbackoff_detail: %v\n"+
		"\tregion_is_valid: %v\n}",
		c.reqType, c.readType, c.staleRead, c.timeout, label, strings.Join(accessErr, ", "), strings.Join(c.accessPath, ", "),
		c.respErr, respRegionError, c.backoffCnt, strings.Join(c.backoffDetail, ", "), c.regionIsValid)
}

func (s *testReplicaSelectorSuite) prepareBeforeRun() *Region {
	// reset slow score, since serverIsBusyErr will mark the store is slow, and affect remaining test cases.
	reachable.injectConstantLiveness(s.cache) // inject reachable liveness.
	rc := s.getRegion()
	s.NotNil(rc)
	for _, store := range rc.getStore().stores {
		store.healthStatus.resetSlowScore()
		atomic.StoreUint32(&store.livenessState, uint32(reachable))
		store.setResolveState(resolved)
	}
	return rc
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
	mode      int
	idx       int
	baseIdx   int
	allErrs   []RegionErrorType
	retryErrs []RegionErrorType
}

func newAccessErrGenerator(isRead, staleRead bool) *accessErrGenerator {
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
		idx:       0,
		mode:      0,
		allErrs:   allErrs,
		retryErrs: retryableErr,
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
	for a.mode <= 5 {
		allErrs := a.allErrs
		retryErrs := a.retryErrs
		if a.mode > 4 {
			// if mode > 4, reduce the error type to avoid generating too many combinations.
			allErrs = getAllRegionErrors(func(tp RegionErrorType) bool {
				if staleRead == false && tp == DataIsNotReadyErr {
					return false
				}
				switch tp {
				case NotLeaderErr, NotLeaderWithNewLeader1Err, NotLeaderWithNewLeader2Err, NotLeaderWithNewLeader3Err, ServerIsBusyWithEstimatedWaitMsErr, DataIsNotReadyErr,
					RegionNotInitializedErr, DeadLineExceededErr:
					return true
				}
				return false
			})
			retryErrs = allErrs
		}
		errs := a.genAccessErrs(allErrs, retryErrs)
		if len(errs) > 0 {
			return errs, false
		}
		a.baseIdx = 0
		a.idx = 0
		a.mode++
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
		conf.EnableReplicaSelectorV2 = true
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
