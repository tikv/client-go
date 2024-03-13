package transaction

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"strconv"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/config"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/client"
	"github.com/tikv/client-go/v2/internal/locate"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/internal/retry"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
	atomicutil "go.uber.org/atomic"
	"go.uber.org/zap"
)

var (
	// BuildTxnFileMaxBackoff is max sleep time to build TxnFile.
	BuildTxnFileMaxBackoff = atomicutil.NewUint64(60000)
)

const MaxTxnChunkSize = 128 * 1024 * 1024
const PreSplitRegionChunks = 4

type txnFileCtx struct {
	slice txnChunkSlice
}

type chunkBatch struct {
	txnChunkSlice
	region    *locate.Region
	isPrimary bool
}

type txnChunkSlice struct {
	chunkIDs    []uint64
	chunkRanges []txnChunkRange
}

func (cs *txnChunkSlice) appendSlice(other *txnChunkSlice) {
	if cs.len() == 0 {
		cs.chunkIDs = append(cs.chunkIDs, other.chunkIDs...)
		cs.chunkRanges = append(cs.chunkRanges, other.chunkRanges...)
		return
	}
	lastChunkRange := cs.chunkRanges[len(cs.chunkIDs)-1]
	for i, chunkRange := range other.chunkRanges {
		if bytes.Compare(lastChunkRange.smallest, chunkRange.smallest) < 0 {
			cs.chunkIDs = append(cs.chunkIDs, other.chunkIDs[i:]...)
			cs.chunkRanges = append(cs.chunkRanges, other.chunkRanges[i:]...)
			break
		}
	}
}

func (cs *txnChunkSlice) append(chunkID uint64, chunkRange txnChunkRange) {
	cs.chunkIDs = append(cs.chunkIDs, chunkID)
	cs.chunkRanges = append(cs.chunkRanges, chunkRange)
}

func (cs *txnChunkSlice) len() int {
	return len(cs.chunkIDs)
}

// func (cs *txnChunkSlice) slice(i, j int) txnChunkSlice {
// 	return txnChunkSlice{
// 		chunkIDs:    cs.chunkIDs[i:j],
// 		chunkRanges: cs.chunkRanges[i:j],
// 	}
// }

func (cs *txnChunkSlice) groupToBatches(c *locate.RegionCache, bo *retry.Backoffer) ([]chunkBatch, error) {
	var batches []chunkBatch
	endKey := kv.NextKey(cs.chunkRanges[cs.len()-1].biggest)
	regions, err := c.LoadRegionsInKeyRange(bo, cs.chunkRanges[0].smallest, endKey)
	if err != nil {
		return nil, err
	}
	for _, region := range regions {
		batch := chunkBatch{
			region: region,
		}
		for i, chunkRange := range cs.chunkRanges {
			if chunkRange.overlapRegion(region) {
				batch.append(cs.chunkIDs[i], chunkRange)
			}
		}
		if batch.len() > 0 {
			batches = append(batches, batch)
		}
	}
	return batches, nil
}

type txnChunkRange struct {
	smallest []byte
	biggest  []byte
}

func newTxnChunkRange(smallest []byte, biggest []byte) txnChunkRange {
	return txnChunkRange{
		smallest: smallest,
		biggest:  biggest,
	}
}

func (r *txnChunkRange) overlapRegion(region *locate.Region) bool {
	return bytes.Compare(r.smallest, region.EndKey()) < 0 && bytes.Compare(r.biggest, region.StartKey()) >= 0
}

type txnFileAction interface {
	executeBatch(c *twoPhaseCommitter, bo *retry.Backoffer, batch chunkBatch) (*tikvrpc.Response, error)
	onPrimarySuccess(c *twoPhaseCommitter)
	extractKeyError(resp *tikvrpc.Response) *kvrpcpb.KeyError
}

type txnFilePrewriteAction struct{}

func (a txnFilePrewriteAction) executeBatch(c *twoPhaseCommitter, bo *retry.Backoffer, batch chunkBatch) (*tikvrpc.Response, error) {
	primaryLock := c.txnFileCtx.slice.chunkRanges[0].smallest
	req := tikvrpc.NewRequest(tikvrpc.CmdPrewrite, &kvrpcpb.PrewriteRequest{
		StartVersion:   c.startTS,
		PrimaryLock:    primaryLock,
		LockTtl:        c.lockTTL,
		MaxCommitTs:    c.maxCommitTS,
		AssertionLevel: kvrpcpb.AssertionLevel_Off,
		TxnFileChunks:  batch.chunkIDs,
	}, kvrpcpb.Context{
		Priority:               c.priority,
		SyncLog:                c.syncLog,
		ResourceGroupTag:       c.resourceGroupTag,
		DiskFullOpt:            c.diskFullOpt,
		TxnSource:              c.txnSource,
		MaxExecutionDurationMs: uint64(client.ReadTimeoutMedium.Milliseconds()),
		RequestSource:          c.txn.GetRequestSource(),
		ResourceControlContext: &kvrpcpb.ResourceControlContext{
			ResourceGroupName: c.resourceGroupName,
		},
	})
	sender := locate.NewRegionRequestSender(c.store.GetRegionCache(), c.store.GetTiKVClient())
	var resolvingRecordToken *int
	for {
		resp, _, err := sender.SendReq(bo, req, batch.region.VerID(), client.ReadTimeoutMedium)
		if err != nil {
			return nil, err
		}
		if resp.Resp == nil {
			return nil, errors.WithStack(tikverr.ErrBodyMissing)
		}
		prewriteResp := resp.Resp.(*kvrpcpb.PrewriteResponse)
		keyErrs := prewriteResp.GetErrors()
		if len(keyErrs) == 0 {
			return resp, nil
		}
		var locks []*txnlock.Lock
		for _, keyErr := range keyErrs {
			// Check already exists error
			if alreadyExist := keyErr.GetAlreadyExist(); alreadyExist != nil {
				e := &tikverr.ErrKeyExist{AlreadyExist: alreadyExist}
				return nil, c.extractKeyExistsErr(e)
			}

			// Extract lock from key error
			lock, err1 := txnlock.ExtractLockFromKeyErr(keyErr)
			if err1 != nil {
				return nil, err1
			}
			logutil.BgLogger().Info(
				"prewrite txn file encounters lock",
				zap.Uint64("session", c.sessionID),
				zap.Uint64("txnID", c.startTS),
				zap.Stringer("lock", lock),
			)
			// If an optimistic transaction encounters a lock with larger TS, this transaction will certainly
			// fail due to a WriteConflict error. So we can construct and return an error here early.
			// Pessimistic transactions don't need such an optimization. If this key needs a pessimistic lock,
			// TiKV will return a PessimisticLockNotFound error directly if it encounters a different lock. Otherwise,
			// TiKV returns lock.TTL = 0, and we still need to resolve the lock.
			if lock.TxnID > c.startTS {
				return nil, tikverr.NewErrWriteConflictWithArgs(
					c.startTS,
					lock.TxnID,
					0,
					lock.Key,
					kvrpcpb.WriteConflict_Optimistic,
				)
			}
			locks = append(locks, lock)
		}
		if resolvingRecordToken == nil {
			token := c.store.GetLockResolver().RecordResolvingLocks(locks, c.startTS)
			resolvingRecordToken = &token
			defer c.store.GetLockResolver().ResolveLocksDone(c.startTS, *resolvingRecordToken)
		} else {
			c.store.GetLockResolver().UpdateResolvingLocks(locks, c.startTS, *resolvingRecordToken)
		}
		resolveLockOpts := txnlock.ResolveLocksOptions{
			CallerStartTS: c.startTS,
			Locks:         locks,
			Detail:        &c.getDetail().ResolveLock,
		}
		resolveLockRes, err := c.store.GetLockResolver().ResolveLocksWithOpts(bo, resolveLockOpts)
		if err != nil {
			return nil, err
		}
		msBeforeExpired := resolveLockRes.TTL
		if msBeforeExpired > 0 {
			err = bo.BackoffWithCfgAndMaxSleep(
				retry.BoTxnLock,
				int(msBeforeExpired),
				errors.Errorf("2PC txn file prewrite lockedKeys: %d", len(locks)),
			)
			if err != nil {
				return nil, err
			}
		}
	}
}

func (a txnFilePrewriteAction) onPrimarySuccess(c *twoPhaseCommitter) {
	c.run(c, nil)
}

func (a txnFilePrewriteAction) extractKeyError(resp *tikvrpc.Response) *kvrpcpb.KeyError {
	prewriteResp, _ := resp.Resp.(*kvrpcpb.PrewriteResponse)
	errs := prewriteResp.GetErrors()
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

type txnFileCommitAction struct {
	commitTS uint64
}

func (a txnFileCommitAction) executeBatch(c *twoPhaseCommitter, bo *retry.Backoffer, batch chunkBatch) (*tikvrpc.Response, error) {
	req := tikvrpc.NewRequest(tikvrpc.CmdCommit, &kvrpcpb.CommitRequest{
		StartVersion:  c.startTS,
		CommitVersion: a.commitTS,
		IsTxnFile:     true,
	}, kvrpcpb.Context{
		Priority:               c.priority,
		SyncLog:                c.syncLog,
		ResourceGroupTag:       c.resourceGroupTag,
		DiskFullOpt:            c.diskFullOpt,
		TxnSource:              c.txnSource,
		MaxExecutionDurationMs: uint64(client.ReadTimeoutMedium.Milliseconds()),
		RequestSource:          c.txn.GetRequestSource(),
		ResourceControlContext: &kvrpcpb.ResourceControlContext{
			ResourceGroupName: c.resourceGroupName,
		},
	})
	sender := locate.NewRegionRequestSender(c.store.GetRegionCache(), c.store.GetTiKVClient())
	for {
		resp, _, err := sender.SendReq(bo, req, batch.region.VerID(), client.ReadTimeoutMedium)
		if batch.isPrimary && sender.GetRPCError() != nil {
			c.setUndeterminedErr(errors.WithStack(sender.GetRPCError()))
		}
		// Unexpected error occurs, return it.
		if err != nil {
			return nil, err
		}
		if resp.Resp == nil {
			return nil, errors.WithStack(tikverr.ErrBodyMissing)
		}
		commitResp := resp.Resp.(*kvrpcpb.CommitResponse)
		if keyErr := commitResp.GetError(); keyErr != nil {
			if rejected := keyErr.GetCommitTsExpired(); rejected != nil {
				logutil.Logger(bo.GetCtx()).Info("2PC commitTS rejected by TiKV, retry with a newer commitTS",
					zap.Uint64("txnStartTS", c.startTS),
					zap.Stringer("info", logutil.Hex(rejected)))

				// Do not retry for a txn which has a too large MinCommitTs
				// 3600000 << 18 = 943718400000
				if rejected.MinCommitTs-rejected.AttemptedCommitTs > 943718400000 {
					return nil, errors.Errorf("2PC MinCommitTS is too large, we got MinCommitTS: %d, and AttemptedCommitTS: %d",
						rejected.MinCommitTs, rejected.AttemptedCommitTs)
				}

				// Update commit ts and retry.
				commitTS, err1 := c.store.GetTimestampWithRetry(bo, c.txn.GetScope())
				if err1 != nil {
					logutil.Logger(bo.GetCtx()).Warn("2PC get commitTS failed",
						zap.Error(err1),
						zap.Uint64("txnStartTS", c.startTS))
					return nil, err1
				}

				c.mu.Lock()
				c.commitTS = commitTS
				c.mu.Unlock()
				// Update the commitTS of the request and retry.
				req.Commit().CommitVersion = commitTS
				continue
			}
			return nil, tikverr.ExtractKeyErr(keyErr)
		}
		return resp, nil
	}
}

func (a txnFileCommitAction) onPrimarySuccess(c *twoPhaseCommitter) {
	c.mu.Lock()
	c.mu.committed = true
	c.mu.Unlock()
}

func (a txnFileCommitAction) extractKeyError(resp *tikvrpc.Response) *kvrpcpb.KeyError {
	commitResp, _ := resp.Resp.(*kvrpcpb.CommitResponse)
	return commitResp.GetError()
}

type txnFileRollbackAction struct{}

func (a txnFileRollbackAction) executeBatch(c *twoPhaseCommitter, bo *retry.Backoffer, batch chunkBatch) (*tikvrpc.Response, error) {
	req := tikvrpc.NewRequest(tikvrpc.CmdBatchRollback, &kvrpcpb.BatchRollbackRequest{
		StartVersion: c.startTS,
		IsTxnFile:    true,
	}, kvrpcpb.Context{
		Priority:               c.priority,
		SyncLog:                c.syncLog,
		ResourceGroupTag:       c.resourceGroupTag,
		DiskFullOpt:            c.diskFullOpt,
		TxnSource:              c.txnSource,
		MaxExecutionDurationMs: uint64(client.ReadTimeoutShort.Milliseconds()),
		RequestSource:          c.txn.GetRequestSource(),
		ResourceControlContext: &kvrpcpb.ResourceControlContext{
			ResourceGroupName: c.resourceGroupName,
		},
	})
	sender := locate.NewRegionRequestSender(c.store.GetRegionCache(), c.store.GetTiKVClient())
	resp, _, err1 := sender.SendReq(bo, req, batch.region.VerID(), client.ReadTimeoutShort)
	if err1 != nil {
		return nil, err1
	}
	return resp, nil
}

func (a txnFileRollbackAction) onPrimarySuccess(_ *twoPhaseCommitter) {
}

func (a txnFileRollbackAction) extractKeyError(resp *tikvrpc.Response) *kvrpcpb.KeyError {
	rollbackResp, _ := resp.Resp.(*kvrpcpb.BatchRollbackResponse)
	return rollbackResp.GetError()
}

func (c *twoPhaseCommitter) executeTxnFile(ctx context.Context) (err error) {
	defer func() {
		// Always clean up all written keys if the txn does not commit.
		c.mu.RLock()
		committed := c.mu.committed
		undetermined := c.mu.undeterminedErr != nil
		c.mu.RUnlock()
		if !committed && !undetermined {
			err1 := c.executeTxnFileActionWithRetry(retry.NewBackofferWithVars(ctx, int(PrewriteMaxBackoff.Load()), c.txn.vars), c.txnFileCtx.slice, txnFileRollbackAction{})
			if err1 != nil {
				logutil.BgLogger().Error("executeTxnFileActionWithRetry failed", zap.Error(err1))
			}
			metrics.TwoPCTxnCounterError.Inc()
		} else {
			metrics.TwoPCTxnCounterOk.Inc()
		}
		c.txn.commitTS = c.commitTS
	}()

	bo := retry.NewBackofferWithVars(ctx, int(PrewriteMaxBackoff.Load()), c.txn.vars)
	if err = c.buildTxnFiles(bo, c.mutations); err != nil {
		return
	}
	if err = c.preSplitTxnFileRegions(bo); err != nil {
		return
	}
	err = c.executeTxnFileActionWithRetry(bo, c.txnFileCtx.slice, txnFilePrewriteAction{})
	if err != nil {
		return
	}
	c.commitTS, err = c.store.GetTimestampWithRetry(bo, c.txn.GetScope())
	if err != nil {
		return
	}
	err = c.executeTxnFileActionWithRetry(bo, c.txnFileCtx.slice, txnFileCommitAction{commitTS: c.commitTS})
	return
}

func (c *twoPhaseCommitter) executeTxnFileAction(bo *retry.Backoffer, chunkSlice txnChunkSlice, action txnFileAction) (txnChunkSlice, error) {
	var regionErrChunks txnChunkSlice
	batches, err := chunkSlice.groupToBatches(c.store.GetRegionCache(), bo)
	if err != nil {
		return regionErrChunks, err
	}
	firstBatch := batches[0]
	if firstBatch.region.Contains(c.primary()) {
		firstBatch.isPrimary = true
		resp, err := action.executeBatch(c, bo, firstBatch)
		if err != nil {
			return regionErrChunks, err
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return regionErrChunks, err
		}
		if regionErr != nil {
			return chunkSlice, nil
		}
		action.onPrimarySuccess(c)
		batches = batches[1:]
	}
	for _, batch := range batches {
		resp, err1 := action.executeBatch(c, bo, batch)
		if err1 != nil {
			return regionErrChunks, err1
		}
		if keyErr := action.extractKeyError(resp); keyErr != nil {
			if alreadyExist := keyErr.GetAlreadyExist(); alreadyExist != nil {
				e := &tikverr.ErrKeyExist{AlreadyExist: alreadyExist}
				return regionErrChunks, c.extractKeyExistsErr(e)
			}
			lock, err2 := txnlock.ExtractLockFromKeyErr(keyErr)
			if err2 != nil {
				return regionErrChunks, err2
			}
			if lock.TxnID > c.startTS {
				return regionErrChunks, tikverr.NewErrWriteConflictWithArgs(
					c.startTS,
					lock.TxnID,
					0,
					lock.Key,
					kvrpcpb.WriteConflict_Optimistic,
				)
			}

		}
		regionErr, err1 := resp.GetRegionError()
		if err1 != nil {
			return regionErrChunks, err1
		}
		if regionErr.GetEpochNotMatch() != nil {
			regionErrChunks.appendSlice(&batch.txnChunkSlice)
			continue
		}
	}
	return regionErrChunks, nil
}

func (c *twoPhaseCommitter) executeTxnFileActionWithRetry(bo *retry.Backoffer, chunkSlice txnChunkSlice, action txnFileAction) error {
	currentChunks := chunkSlice
	for {
		var regionErrChunks txnChunkSlice
		regionErrChunks, err := c.executeTxnFileAction(bo, currentChunks, action)
		if err != nil {
			return err
		}
		if regionErrChunks.len() == 0 {
			return nil
		}
		currentChunks = regionErrChunks
		err = bo.Backoff(retry.BoRegionMiss, errors.Errorf("txnFile region miss"))
		if err != nil {
			return err
		}
	}
}

func (c *twoPhaseCommitter) buildTxnFiles(bo *retry.Backoffer, mutations CommitterMutations) error {
	capacity := c.txn.Size() + c.txn.Len()*7 + 4
	if capacity > MaxTxnChunkSize {
		capacity = MaxTxnChunkSize
	}
	writerAddr := config.GetGlobalConfig().TiKVClient.TxnChunkWriterAddr
	buf := make([]byte, 0, capacity)
	chunkSmallest := mutations.GetKey(0)
	for i := 0; i < mutations.Len(); i++ {
		key := mutations.GetKey(i)
		op := mutations.GetOp(i)
		val := mutations.GetValue(i)
		entrySize := 2 + len(key) + 1 + 4 + len(val)
		if len(buf)+entrySize+4 > cap(buf) {
			chunkID, err := c.buildTxnFile(bo, writerAddr, buf)
			if err != nil {
				return err
			}
			ran := newTxnChunkRange(chunkSmallest, mutations.GetKey(i-1))
			c.txnFileCtx.slice.append(chunkID, ran)
			chunkSmallest = key
			buf = buf[:0]
		}
		buf = binary.LittleEndian.AppendUint16(buf, uint16(len(key)))
		buf = append(buf, key...)
		buf = append(buf, byte(op))
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(val)))
		buf = append(buf, val...)
	}
	if len(buf) > 0 {
		chunkID, err := c.buildTxnFile(bo, writerAddr, buf)
		if err != nil {
			return err
		}
		ran := newTxnChunkRange(chunkSmallest, mutations.GetKey(mutations.Len()-1))
		c.txnFileCtx.slice.append(chunkID, ran)
	}
	return nil
}

func (c *twoPhaseCommitter) buildTxnFile(bo *retry.Backoffer, writerAddr string, buf []byte) (uint64, error) {
	hash := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	hash.Write(buf)
	crc := hash.Sum32()
	buf = binary.LittleEndian.AppendUint32(buf, crc)
	logutil.BgLogger().Info("build txn file size", zap.Int("size", len(buf)))
	url := fmt.Sprintf("http://%s/txn_chunk", writerAddr)
	req, err := http.NewRequestWithContext(bo.GetCtx(), "POST", url, bytes.NewReader(buf))
	if err != nil {
		return 0, errors.WithStack(err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("http status %s", resp.Status)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(string(data), 10, 64)
}

func (c *twoPhaseCommitter) useTxnFile() bool {
	if c.txn.isPessimistic || c.txn.GetMemBuffer().Size() < 16*1024*1024 {
		return false
	}
	conf := config.GetGlobalConfig()
	return len(conf.TiKVClient.TxnChunkWriterAddr) > 0
}

func (c *twoPhaseCommitter) preSplitTxnFileRegions(bo *retry.Backoffer) error {
	for {
		batches, err := c.txnFileCtx.slice.groupToBatches(c.store.GetRegionCache(), bo)
		if err != nil {
			err = bo.Backoff(retry.BoRegionMiss, err)
			if err != nil {
				return err
			}
			continue
		}
		var splitKeys [][]byte
		for _, batch := range batches {
			if batch.len() > PreSplitRegionChunks {
				for i := PreSplitRegionChunks; i < batch.len(); i += PreSplitRegionChunks {
					splitKeys = append(splitKeys, batch.chunkRanges[i].smallest)
				}
			}
		}
		if len(splitKeys) == 0 {
			return nil
		}
		_, err = c.store.SplitRegions(bo.GetCtx(), splitKeys, false, nil)
		if err != nil {
			logutil.BgLogger().Warn("txn file pre-split region failed")
		}
		err = bo.Backoff(retry.BoRegionMiss, err)
		if err != nil {
			return err
		}
	}
}
