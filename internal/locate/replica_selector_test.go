package locate

import (
	"context"
	"fmt"
	"math/rand"
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
	"github.com/tikv/client-go/v2/config/retry"
	"github.com/tikv/client-go/v2/internal/apicodec"
	"github.com/tikv/client-go/v2/internal/client"
	"github.com/tikv/client-go/v2/internal/mockstore/mocktikv"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikvrpc"
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
		timeout:   time.Microsecond * 100,
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
}

func (s *testReplicaSelectorSuite) runCaseAndCompare(ca2 replicaSelectorAccessPathCase) bool {
	ca2.run(s)
	if ca2.accessErrInValid {
		// the case has been marked as invalid, just ignore it.
		return false
	}
	if ca2.expect != nil {
		msg := fmt.Sprintf("%v\n\n", ca2.Format())
		expect := ca2.expect
		result := ca2.accessPathResult
		s.Equal(expect.accessPath, result.accessPath, msg)
		s.Equal(expect.respErr, result.respErr, msg)
		s.Equal(expect.respRegionError, result.respRegionError, msg)
		s.Equal(expect.regionIsValid, result.regionIsValid, msg)
		s.Equal(expect.backoffCnt, result.backoffCnt, msg)
		s.Equal(expect.backoffDetail, result.backoffDetail, msg)
	}
	return true
}

func (ca *replicaSelectorAccessPathCase) run(s *testReplicaSelectorSuite) {
	reachable.injectConstantLiveness(s.cache) // inject reachable liveness.
	msg := ca.Format()
	access := []string{}
	fnClient := &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (response *tikvrpc.Response, err error) {
		idx := len(access)
		access = append(access, fmt.Sprintf("{addr: %v, replica-read: %v, stale-read: %v}", addr, req.ReplicaRead, req.StaleRead))
		if idx < len(ca.accessErr) {
			if !ca.accessErr[idx].Valid(addr, req) {
				// mark this case is invalid. just ignore this case.
				ca.accessErrInValid = true
			} else {
				loc, err := s.cache.LocateKey(s.bo, []byte("key"))
				s.Nil(err)
				rc := s.cache.GetCachedRegionWithRLock(loc.Region)
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
	// reset slow score, since serverIsBusyErr will mark the store is slow, and affect remaining test cases.
	loc, err := s.cache.LocateKey(s.bo, []byte("key"))
	s.Nil(err)
	rc := s.cache.GetCachedRegionWithRLock(loc.Region)
	s.NotNil(rc)
	for _, store := range rc.getStore().stores {
		store.slowScore.resetSlowScore()
		atomic.StoreUint32(&store.livenessState, uint32(reachable))
		store.setResolveState(resolved)
	}

	bo := retry.NewBackofferWithVars(context.Background(), 40000, nil)
	timeout := ca.timeout
	if timeout == 0 {
		timeout = client.ReadTimeoutShort
	}
	resp, _, _, err := sender.SendReqCtx(bo, req, loc.Region, timeout, tikvrpc.TiKV, opts...)
	if err == nil {
		s.NotNil(resp, msg)
		regionErr, err := resp.GetRegionError()
		s.Nil(err, msg)
		ca.respRegionError = regionErr
	} else {
		ca.respErr = err.Error()
	}
	ca.accessPath = access
	ca.backoffCnt = bo.GetTotalBackoffTimes()
	detail := make([]string, 0, len(bo.GetBackoffTimes()))
	for tp, cnt := range bo.GetBackoffTimes() {
		detail = append(detail, fmt.Sprintf("%v+%v", tp, cnt))
	}
	sort.Strings(detail)
	ca.backoffDetail = detail
	ca.regionIsValid = sender.replicaSelector.region.isValid()
	sender.replicaSelector.invalidateRegion() // invalidate region to reload for next test case.
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
	if req.StaleRead == false && req.ReplicaRead == false {
		switch tp {
		case DataIsNotReadyErr:
			// DataIsNotReadyErr only return when req is a stale read.
			return false
		}
	}
	// replica-read.
	if req.StaleRead == false && req.ReplicaRead == true {
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
	if req.StaleRead == true && req.ReplicaRead == false {
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
