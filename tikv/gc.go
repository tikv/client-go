package tikv

import (
	"bytes"
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/locate"
	"github.com/tikv/client-go/v2/logutil"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/retry"
	"github.com/tikv/client-go/v2/tikvrpc"
	zap "go.uber.org/zap"
)

/// GC does garbage collection (GC) of the TiKV cluster.
/// GC deletes MVCC records whose timestamp is lower than the given `safepoint`. We must guarantee
///  that all transactions started before this timestamp had committed. We can keep an active
/// transaction list in application to decide which is the minimal start timestamp of them.
///
/// For each key, the last mutation record (unless it's a deletion) before `safepoint` is retained.
///
/// GC is performed by:
/// 1. resolving all locks with timestamp <= `safepoint`
/// 2. updating PD's known safepoint
///
/// This is a simplified version of [GC in TiDB](https://docs.pingcap.com/tidb/stable/garbage-collection-overview).
/// We skip the second step "delete ranges" which is an optimization for TiDB.
func (s *KVStore) GC(ctx context.Context, safepoint time.Time) (newSafePoint time.Time, err error) {
	safepointTS := oracle.GoTimeToTS(safepoint)
	err = s.resolveLocks(ctx, safepointTS, 8)
	if err != nil {
		return
	}

	t, err := s.pdClient.UpdateGCSafePoint(ctx, safepointTS)
	if err != nil {
		return
	}
	newSafePoint = oracle.GetTimeFromTS(t)
	return
}

func (s *KVStore) resolveLocks(ctx context.Context, safePoint uint64, concurrency int) error {
	handler := func(ctx context.Context, r kv.KeyRange) (RangeTaskStat, error) {
		return s.resolveLocksForRange(ctx, safePoint, r.StartKey, r.EndKey)
	}

	runner := NewRangeTaskRunner("resolve-locks-runner", s, concurrency, handler)
	// Run resolve lock on the whole TiKV cluster. Empty keys means the range is unbounded.
	err := runner.RunOnRange(ctx, []byte(""), []byte(""))
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// We don't want gc to sweep out the cached info belong to other processes, like coprocessor.
const gcScanLockLimit = ResolvedCacheSize / 2

func (s *KVStore) resolveLocksForRange(ctx context.Context, safePoint uint64, startKey []byte, endKey []byte) (RangeTaskStat, error) {
	// for scan lock request, we must return all locks even if they are generated
	// by the same transaction. because gc worker need to make sure all locks have been
	// cleaned.
	req := tikvrpc.NewRequest(tikvrpc.CmdScanLock, &kvrpcpb.ScanLockRequest{
		MaxVersion: safePoint,
		Limit:      gcScanLockLimit,
	})

	var stat RangeTaskStat
	key := startKey
	bo := NewGcResolveLockMaxBackoffer(ctx)

	for {
		select {
		case <-ctx.Done():
			return stat, errors.New("[gc worker] gc job canceled")
		default:
		}

		req.ScanLock().StartKey = key
		loc, err := s.GetRegionCache().LocateKey(bo, key)
		if err != nil {
			return stat, errors.Trace(err)
		}
		req.ScanLock().EndKey = loc.EndKey
		resp, err := s.SendReq(bo, req, loc.Region, ReadTimeoutMedium)
		if err != nil {
			return stat, errors.Trace(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return stat, errors.Trace(err)
		}
		if regionErr != nil {
			err = bo.Backoff(BoRegionMiss(), errors.New(regionErr.String()))
			if err != nil {
				return stat, errors.Trace(err)
			}
			continue
		}
		if resp.Resp == nil {
			return stat, errors.Trace(tikverr.ErrBodyMissing)
		}
		locksResp := resp.Resp.(*kvrpcpb.ScanLockResponse)
		if locksResp.GetError() != nil {
			return stat, errors.Errorf("unexpected scanlock error: %s", locksResp)
		}
		locksInfo := locksResp.GetLocks()
		locks := make([]*Lock, len(locksInfo))
		for i := range locksInfo {
			locks[i] = NewLock(locksInfo[i])
		}

		ok, err1 := s.BatchResolveLockWithRetry(bo, locks, loc.Region, 10)
		if err1 != nil {
			return stat, errors.Trace(err1)
		}
		// Resolve current locks failed, retry to scan locks again.
		if !ok {
			continue
		}

		if len(locks) < gcScanLockLimit {
			stat.CompletedRegions++
			key = loc.EndKey
			logutil.Logger(ctx).Info("[gc worker] one region finshed ",
				zap.Int("resolvedLocksNum", len(locks)))
		} else {
			logutil.Logger(ctx).Info("[gc worker] region has more than limit locks",
				zap.Int("resolvedLocksNum", len(locks)),
				zap.Int("scan lock limit", gcScanLockLimit))
			key = locks[len(locks)-1].Key
		}

		if len(key) == 0 || (len(endKey) != 0 && bytes.Compare(key, endKey) >= 0) {
			break
		}
		bo = NewGcResolveLockMaxBackoffer(ctx)
	}
	return stat, nil
}

// BatchResolveLockWithRetry resolve locks in a region with retry.
// It returns true if resolve locks success. ok is false if resolve locks failed.
// When resolve locks failed, if err is nil means the locks are not in the same region any more and the client should retry to scan these locks.
// Used it in gcworker only!
func (s *KVStore) BatchResolveLockWithRetry(bo *Backoffer, locks []*Lock, loc locate.RegionVerID, retryTime int) (ok bool, err error) {
	for t := 0; t < retryTime; t++ {
		ok, err = s.GetLockResolver().BatchResolveLocks(bo, locks, loc)
		if ok {
			return
		}
		err = bo.Backoff(retry.BoTxnLock, errors.Errorf("remain locks: %d", len(locks)))
		if err != nil {
			return ok, errors.Trace(err)
		}
		region, err1 := s.GetRegionCache().LocateKey(bo, locks[0].Key)
		if err1 != nil {
			return ok, errors.Trace(err1)
		}
		if !region.Contains(locks[len(locks)-1].Key) {
			// retry scan since the locks are not in the same region anymore.
			return
		}
		loc = region.Region
	}
	//retry scan since failed with too much retry.
	return false, nil
}
