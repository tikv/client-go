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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/2pc.go
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

package transaction

import (
	"bytes"
	"context"
	"encoding/hex"
	"math"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/config"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/client"
	"github.com/tikv/client-go/v2/internal/latch"
	"github.com/tikv/client-go/v2/internal/locate"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/internal/retry"
	"github.com/tikv/client-go/v2/internal/unionstore"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
	"github.com/tikv/client-go/v2/util"
	atomicutil "go.uber.org/atomic"
	zap "go.uber.org/zap"
)

// If the duration of a single request exceeds the slowRequestThreshold, a warning log will be logged.
const slowRequestThreshold = time.Minute

type twoPhaseCommitAction interface {
	handleSingleBatch(*twoPhaseCommitter, *retry.Backoffer, batchMutations) error
	tiKVTxnRegionsNumHistogram() prometheus.Observer
	String() string
}

// Global variable set by config file.
var (
	ManagedLockTTL uint64 = 20000 // 20s
)

var (
	// PrewriteMaxBackoff is max sleep time of the `pre-write` command.
	PrewriteMaxBackoff = atomicutil.NewUint64(40000)
	// CommitMaxBackoff is max sleep time of the 'commit' command
	CommitMaxBackoff = uint64(40000)
)

type kvstore interface {
	// GetRegionCache gets the RegionCache.
	GetRegionCache() *locate.RegionCache
	// SplitRegions splits regions by splitKeys.
	SplitRegions(ctx context.Context, splitKeys [][]byte, scatter bool, tableID *int64) (regionIDs []uint64, err error)
	// WaitScatterRegionFinish implements SplittableStore interface.
	// backOff is the back off time of the wait scatter region.(Milliseconds)
	// if backOff <= 0, the default wait scatter back off time will be used.
	WaitScatterRegionFinish(ctx context.Context, regionID uint64, backOff int) error

	// GetTimestampWithRetry returns latest timestamp.
	GetTimestampWithRetry(bo *retry.Backoffer, scope string) (uint64, error)
	// GetOracle gets a timestamp oracle client.
	GetOracle() oracle.Oracle
	CurrentTimestamp(txnScope string) (uint64, error)
	// SendReq sends a request to TiKV.
	SendReq(bo *retry.Backoffer, req *tikvrpc.Request, regionID locate.RegionVerID, timeout time.Duration) (*tikvrpc.Response, error)
	// GetTiKVClient gets the client instance.
	GetTiKVClient() (client client.Client)
	GetLockResolver() *txnlock.LockResolver
	Ctx() context.Context
	WaitGroup() *sync.WaitGroup
	// TxnLatches returns txnLatches.
	TxnLatches() *latch.LatchesScheduler
	GetClusterID() uint64
	// IsClose checks whether the store is closed.
	IsClose() bool
	// Go run the function in a separate goroutine.
	Go(f func())
}

// twoPhaseCommitter executes a two-phase commit protocol.
type twoPhaseCommitter struct {
	store               kvstore
	txn                 *KVTxn
	startTS             uint64
	mutations           *memBufferMutations
	lockTTL             uint64
	commitTS            uint64
	priority            kvrpcpb.CommandPri
	sessionID           uint64 // sessionID is used for log.
	cleanWg             sync.WaitGroup
	detail              unsafe.Pointer
	txnSize             int
	hasNoNeedCommitKeys bool
	resourceGroupName   string

	primaryKey  []byte
	forUpdateTS uint64

	maxLockedWithConflictTS uint64

	mu struct {
		sync.RWMutex
		undeterminedErr error // undeterminedErr saves the rpc error we encounter when commit primary key.
		committed       bool
	}
	syncLog bool
	// For pessimistic transaction
	isPessimistic bool
	isFirstLock   bool
	// regionTxnSize stores the number of keys involved in each region
	regionTxnSize map[uint64]int
	// Used by pessimistic transaction and large transaction.
	ttlManager

	testingKnobs struct {
		acAfterCommitPrimary chan struct{}
		bkAfterCommitPrimary chan struct{}
		noFallBack           bool
	}

	useAsyncCommit    uint32
	minCommitTS       uint64
	maxCommitTS       uint64
	prewriteStarted   bool
	prewriteCancelled uint32
	useOnePC          uint32
	onePCCommitTS     uint64

	hasTriedAsyncCommit bool
	hasTriedOnePC       bool

	binlog BinlogExecutor

	resourceGroupTag    []byte
	resourceGroupTagger tikvrpc.ResourceGroupTagger // use this when resourceGroupTag is nil

	// allowed when tikv disk full happened.
	diskFullOpt kvrpcpb.DiskFullOpt

	// txnSource is used to record the source of the transaction.
	txnSource uint64

	// The total number of kv request after batch split.
	prewriteTotalReqNum int

	// assertion error happened when initializing mutations, could be false positive if pessimistic lock is lost
	stashedAssertionError error
}

type memBufferMutations struct {
	storage *unionstore.MemDB

	// The format to put to the UserData of the handles:
	// MSB									                                                                              LSB
	// [12 bits: Op][1 bit: NeedConstraintCheckInPrewrite][1 bit: assertNotExist][1 bit: assertExist][1 bit: isPessimisticLock]
	handles []unionstore.MemKeyHandle
}

func newMemBufferMutations(sizeHint int, storage *unionstore.MemDB) *memBufferMutations {
	return &memBufferMutations{
		handles: make([]unionstore.MemKeyHandle, 0, sizeHint),
		storage: storage,
	}
}

func (m *memBufferMutations) Len() int {
	return len(m.handles)
}

func (m *memBufferMutations) GetKey(i int) []byte {
	return m.storage.GetKeyByHandle(m.handles[i])
}

func (m *memBufferMutations) GetKeys() [][]byte {
	ret := make([][]byte, m.Len())
	for i := range ret {
		ret[i] = m.GetKey(i)
	}
	return ret
}

func (m *memBufferMutations) GetValue(i int) []byte {
	v, _ := m.storage.GetValueByHandle(m.handles[i])
	return v
}

func (m *memBufferMutations) GetOp(i int) kvrpcpb.Op {
	return kvrpcpb.Op(m.handles[i].UserData >> 4)
}

func (m *memBufferMutations) IsPessimisticLock(i int) bool {
	return m.handles[i].UserData&1 != 0
}

func (m *memBufferMutations) IsAssertExists(i int) bool {
	return m.handles[i].UserData&(1<<1) != 0
}

func (m *memBufferMutations) IsAssertNotExist(i int) bool {
	return m.handles[i].UserData&(1<<2) != 0
}

func (m *memBufferMutations) NeedConstraintCheckInPrewrite(i int) bool {
	return m.handles[i].UserData&(1<<3) != 0
}

func (m *memBufferMutations) Slice(from, to int) CommitterMutations {
	return &memBufferMutations{
		handles: m.handles[from:to],
		storage: m.storage,
	}
}

func (m *memBufferMutations) Push(op kvrpcpb.Op, isPessimisticLock, assertExist, assertNotExist, NeedConstraintCheckInPrewrite bool,
	handle unionstore.MemKeyHandle) {
	// See comments of `m.handles` field about the format of the user data `aux`.
	aux := uint16(op) << 4
	if isPessimisticLock {
		aux |= 1
	}
	if assertExist {
		aux |= 1 << 1
	}
	if assertNotExist {
		aux |= 1 << 2
	}
	if NeedConstraintCheckInPrewrite {
		aux |= 1 << 3
	}
	handle.UserData = aux
	m.handles = append(m.handles, handle)
}

// CommitterMutationFlags represents various bit flags of mutations.
type CommitterMutationFlags uint8

const (
	// MutationFlagIsPessimisticLock is the flag that marks a mutation needs to be pessimistic-locked.
	MutationFlagIsPessimisticLock CommitterMutationFlags = 1 << iota

	// MutationFlagIsAssertExists is the flag that marks a mutation needs to be asserted to be existed when prewriting.
	MutationFlagIsAssertExists

	// MutationFlagIsAssertNotExists is the flag that marks a mutation needs to be asserted to be not-existed when prewriting.
	MutationFlagIsAssertNotExists

	// MutationFlagNeedConstraintCheckInPrewrite is the flag that marks a mutation needs to be checked for conflicts in prewrite.
	MutationFlagNeedConstraintCheckInPrewrite
)

func makeMutationFlags(isPessimisticLock, assertExist, assertNotExist, NeedConstraintCheckInPrewrite bool) CommitterMutationFlags {
	var flags CommitterMutationFlags = 0
	if isPessimisticLock {
		flags |= MutationFlagIsPessimisticLock
	}
	if assertExist {
		flags |= MutationFlagIsAssertExists
	}
	if assertNotExist {
		flags |= MutationFlagIsAssertNotExists
	}
	if NeedConstraintCheckInPrewrite {
		flags |= MutationFlagNeedConstraintCheckInPrewrite
	}
	return flags
}

// CommitterMutations contains the mutations to be submitted.
type CommitterMutations interface {
	Len() int
	GetKey(i int) []byte
	GetKeys() [][]byte
	GetOp(i int) kvrpcpb.Op
	GetValue(i int) []byte
	IsPessimisticLock(i int) bool
	Slice(from, to int) CommitterMutations
	IsAssertExists(i int) bool
	IsAssertNotExist(i int) bool
	NeedConstraintCheckInPrewrite(i int) bool
}

// PlainMutations contains transaction operations.
type PlainMutations struct {
	ops    []kvrpcpb.Op
	keys   [][]byte
	values [][]byte
	flags  []CommitterMutationFlags
}

// NewPlainMutations creates a PlainMutations object with sizeHint reserved.
func NewPlainMutations(sizeHint int) PlainMutations {
	return PlainMutations{
		ops:    make([]kvrpcpb.Op, 0, sizeHint),
		keys:   make([][]byte, 0, sizeHint),
		values: make([][]byte, 0, sizeHint),
		flags:  make([]CommitterMutationFlags, 0, sizeHint),
	}
}

// Slice return a sub mutations in range [from, to).
func (c *PlainMutations) Slice(from, to int) CommitterMutations {
	var res PlainMutations
	res.keys = c.keys[from:to]
	if c.ops != nil {
		res.ops = c.ops[from:to]
	}
	if c.values != nil {
		res.values = c.values[from:to]
	}
	if c.flags != nil {
		res.flags = c.flags[from:to]
	}
	return &res
}

// Push another mutation into mutations.
func (c *PlainMutations) Push(op kvrpcpb.Op, key []byte, value []byte, isPessimisticLock, assertExist,
	assertNotExist, NeedConstraintCheckInPrewrite bool) {
	c.ops = append(c.ops, op)
	c.keys = append(c.keys, key)
	c.values = append(c.values, value)
	c.flags = append(c.flags, makeMutationFlags(isPessimisticLock, assertExist, assertNotExist, NeedConstraintCheckInPrewrite))
}

// Len returns the count of mutations.
func (c *PlainMutations) Len() int {
	return len(c.keys)
}

// GetKey returns the key at index.
func (c *PlainMutations) GetKey(i int) []byte {
	return c.keys[i]
}

// GetKeys returns the keys.
func (c *PlainMutations) GetKeys() [][]byte {
	return c.keys
}

// GetOps returns the key ops.
func (c *PlainMutations) GetOps() []kvrpcpb.Op {
	return c.ops
}

// GetValues returns the key values.
func (c *PlainMutations) GetValues() [][]byte {
	return c.values
}

// GetFlags returns the flags on the mutations.
func (c *PlainMutations) GetFlags() []CommitterMutationFlags {
	return c.flags
}

// IsAssertExists returns the key assertExist flag at index.
func (c *PlainMutations) IsAssertExists(i int) bool {
	return c.flags[i]&MutationFlagIsAssertExists != 0
}

// IsAssertNotExist returns the key assertNotExist flag at index.
func (c *PlainMutations) IsAssertNotExist(i int) bool {
	return c.flags[i]&MutationFlagIsAssertNotExists != 0
}

// NeedConstraintCheckInPrewrite returns the key NeedConstraintCheckInPrewrite flag at index.
func (c *PlainMutations) NeedConstraintCheckInPrewrite(i int) bool {
	return c.flags[i]&MutationFlagNeedConstraintCheckInPrewrite != 0
}

// GetOp returns the key op at index.
func (c *PlainMutations) GetOp(i int) kvrpcpb.Op {
	return c.ops[i]
}

// GetValue returns the key value at index.
func (c *PlainMutations) GetValue(i int) []byte {
	if len(c.values) <= i {
		return nil
	}
	return c.values[i]
}

// IsPessimisticLock returns the key pessimistic flag at index.
func (c *PlainMutations) IsPessimisticLock(i int) bool {
	return c.flags[i]&MutationFlagIsPessimisticLock != 0
}

// PlainMutation represents a single transaction operation.
type PlainMutation struct {
	KeyOp kvrpcpb.Op
	Key   []byte
	Value []byte
	Flags CommitterMutationFlags
}

// MergeMutations append input mutations into current mutations.
func (c *PlainMutations) MergeMutations(mutations PlainMutations) {
	c.ops = append(c.ops, mutations.ops...)
	c.keys = append(c.keys, mutations.keys...)
	c.values = append(c.values, mutations.values...)
	c.flags = append(c.flags, mutations.flags...)
}

// AppendMutation merges a single Mutation into the current mutations.
func (c *PlainMutations) AppendMutation(mutation PlainMutation) {
	c.ops = append(c.ops, mutation.KeyOp)
	c.keys = append(c.keys, mutation.Key)
	c.values = append(c.values, mutation.Value)
	c.flags = append(c.flags, mutation.Flags)
}

// newTwoPhaseCommitter creates a twoPhaseCommitter.
func newTwoPhaseCommitter(txn *KVTxn, sessionID uint64) (*twoPhaseCommitter, error) {
	return &twoPhaseCommitter{
		store:         txn.store,
		txn:           txn,
		startTS:       txn.StartTS(),
		sessionID:     sessionID,
		regionTxnSize: map[uint64]int{},
		isPessimistic: txn.IsPessimistic(),
		binlog:        txn.binlog,
		diskFullOpt:   kvrpcpb.DiskFullOpt_NotAllowedOnFull,
	}, nil
}

func (c *twoPhaseCommitter) extractKeyExistsErr(err *tikverr.ErrKeyExist) error {
	c.txn.GetMemBuffer().RLock()
	defer c.txn.GetMemBuffer().RUnlock()
	if !c.txn.us.HasPresumeKeyNotExists(err.GetKey()) {
		return errors.Errorf("session %d, existErr for key:%s should not be nil", c.sessionID, err.GetKey())
	}
	return errors.WithStack(err)
}

// KVFilter is a filter that filters out unnecessary KV pairs.
type KVFilter interface {
	// IsUnnecessaryKeyValue returns whether this KV pair should be committed.
	IsUnnecessaryKeyValue(key, value []byte, flags kv.KeyFlags) (bool, error)
}

func (c *twoPhaseCommitter) checkAssertionByPessimisticLockResults(ctx context.Context, key []byte, flags kv.KeyFlags, mustExist, mustNotExist bool) error {
	var assertionFailed *tikverr.ErrAssertionFailed
	if flags.HasLockedValueExists() && mustNotExist {
		assertionFailed = &tikverr.ErrAssertionFailed{
			AssertionFailed: &kvrpcpb.AssertionFailed{
				StartTs:          c.startTS,
				Key:              key,
				Assertion:        kvrpcpb.Assertion_NotExist,
				ExistingStartTs:  0,
				ExistingCommitTs: 0,
			},
		}
	} else if !flags.HasLockedValueExists() && mustExist {
		assertionFailed = &tikverr.ErrAssertionFailed{
			AssertionFailed: &kvrpcpb.AssertionFailed{
				StartTs:          c.startTS,
				Key:              key,
				Assertion:        kvrpcpb.Assertion_Exist,
				ExistingStartTs:  0,
				ExistingCommitTs: 0,
			},
		}
	}

	if assertionFailed != nil {
		return c.checkSchemaOnAssertionFail(ctx, assertionFailed)
	}

	return nil
}

func (c *twoPhaseCommitter) checkSchemaOnAssertionFail(ctx context.Context, assertionFailed *tikverr.ErrAssertionFailed) error {
	// If the schema has changed, it might be a false-positive. In this case we should return schema changed, which
	// is a usual case, instead of assertion failed.
	ts, err := c.store.GetTimestampWithRetry(retry.NewBackofferWithVars(ctx, TsoMaxBackoff, c.txn.vars), c.txn.GetScope())
	if err != nil {
		return err
	}
	err = c.checkSchemaValid(ctx, ts, c.txn.schemaVer)
	if err != nil {
		return err
	}

	return assertionFailed
}

func (c *twoPhaseCommitter) initKeysAndMutations(ctx context.Context) error {
	var size, putCnt, delCnt, lockCnt, checkCnt int

	txn := c.txn
	memBuf := txn.GetMemBuffer()
	sizeHint := txn.us.GetMemBuffer().Len()
	c.mutations = newMemBufferMutations(sizeHint, memBuf)
	c.isPessimistic = txn.IsPessimistic()
	filter := txn.kvFilter

	var err error
	var assertionError error
	for it := memBuf.IterWithFlags(nil, nil); it.Valid(); err = it.Next() {
		_ = err
		key := it.Key()
		flags := it.Flags()
		var value []byte
		var op kvrpcpb.Op

		if !it.HasValue() {
			if !flags.HasLocked() {
				continue
			}
			op = kvrpcpb.Op_Lock
			lockCnt++
		} else {
			value = it.Value()
			var isUnnecessaryKV bool
			if filter != nil {
				isUnnecessaryKV, err = filter.IsUnnecessaryKeyValue(key, value, flags)
				if err != nil {
					return err
				}
			}
			if len(value) > 0 {
				if isUnnecessaryKV {
					if !flags.HasLocked() {
						continue
					}
					// If the key was locked before, we should prewrite the lock even if
					// the KV needn't be committed according to the filter. Otherwise, we
					// were forgetting removing pessimistic locks added before.
					op = kvrpcpb.Op_Lock
					lockCnt++
				} else {
					op = kvrpcpb.Op_Put
					if flags.HasPresumeKeyNotExists() {
						op = kvrpcpb.Op_Insert
					}
					putCnt++
				}
			} else {
				if isUnnecessaryKV {
					continue
				}
				if !txn.IsPessimistic() && flags.HasPresumeKeyNotExists() {
					// delete-your-writes keys in optimistic txn need check not exists in prewrite-phase
					// due to `Op_CheckNotExists` doesn't prewrite lock, so mark those keys should not be used in commit-phase.
					op = kvrpcpb.Op_CheckNotExists
					checkCnt++
					memBuf.UpdateFlags(key, kv.SetPrewriteOnly)
				} else {
					if flags.HasNewlyInserted() {
						// The delete-your-write keys in pessimistic transactions, only lock needed keys and skip
						// other deletes for example the secondary index delete.
						// Here if `tidb_constraint_check_in_place` is enabled and the transaction is in optimistic mode,
						// the logic is same as the pessimistic mode.
						if flags.HasLocked() {
							op = kvrpcpb.Op_Lock
							lockCnt++
						} else {
							continue
						}
					} else {
						op = kvrpcpb.Op_Del
						delCnt++
					}
				}
			}
		}

		var isPessimistic bool
		if flags.HasLocked() {
			isPessimistic = c.isPessimistic
		}
		mustExist, mustNotExist, hasAssertUnknown := flags.HasAssertExist(), flags.HasAssertNotExist(), flags.HasAssertUnknown()
		if c.txn.assertionLevel == kvrpcpb.AssertionLevel_Off {
			mustExist, mustNotExist, hasAssertUnknown = false, false, false
		}
		c.mutations.Push(op, isPessimistic, mustExist, mustNotExist, flags.HasNeedConstraintCheckInPrewrite(), it.Handle())
		size += len(key) + len(value)

		if c.txn.assertionLevel != kvrpcpb.AssertionLevel_Off {
			// Check mutations for pessimistic-locked keys with the read results of pessimistic lock requests.
			// This can be disabled by failpoint.
			skipCheckFromLock := false
			if _, err := util.EvalFailpoint("assertionSkipCheckFromLock"); err == nil {
				skipCheckFromLock = true
			}
			if isPessimistic && !skipCheckFromLock {
				err1 := c.checkAssertionByPessimisticLockResults(ctx, key, flags, mustExist, mustNotExist)
				// Do not exit immediately here. To rollback the pessimistic locks (if any), we need to finish
				// collecting all the keys.
				// Keep only the first assertion error.

				// assertion errors is treated differently from other errors. If there is an assertion error,
				// it's probably cause by loss of pessimistic locks, so we can't directly return the assertion error.
				// Instead, we stash the error, forbid async commit and 1PC, then let the prewrite continue.
				// If the prewrite requests all succeed, the assertion error is returned, otherwise return the error
				// from the prewrite phase.
				if err1 != nil && assertionError == nil {
					assertionError = errors.WithStack(err1)
					c.stashedAssertionError = assertionError
					c.txn.enableAsyncCommit = false
					c.txn.enable1PC = false
				}
			}

			// Update metrics
			if mustExist {
				metrics.PrewriteAssertionUsageCounterExist.Inc()
			} else if mustNotExist {
				metrics.PrewriteAssertionUsageCounterNotExist.Inc()
			} else if hasAssertUnknown {
				metrics.PrewriteAssertionUsageCounterUnknown.Inc()
			} else {
				metrics.PrewriteAssertionUsageCounterNone.Inc()
			}
		}

		if len(c.primaryKey) == 0 && op != kvrpcpb.Op_CheckNotExists {
			c.primaryKey = key
		}
	}

	if c.mutations.Len() == 0 {
		return nil
	}
	c.txnSize = size

	const logEntryCount = 10000
	const logSize = 4 * 1024 * 1024 // 4MB
	if c.mutations.Len() > logEntryCount || size > logSize {
		logutil.BgLogger().Info("[BIG_TXN]",
			zap.Uint64("session", c.sessionID),
			zap.String("key sample", kv.StrKey(c.mutations.GetKey(0))),
			zap.Int("size", size),
			zap.Int("keys", c.mutations.Len()),
			zap.Int("puts", putCnt),
			zap.Int("dels", delCnt),
			zap.Int("locks", lockCnt),
			zap.Int("checks", checkCnt),
			zap.Uint64("txnStartTS", txn.startTS))
	}

	// Sanity check for startTS.
	if txn.StartTS() == math.MaxUint64 {
		err = errors.Errorf("try to commit with invalid txnStartTS: %d", txn.StartTS())
		logutil.BgLogger().Error("commit failed",
			zap.Uint64("session", c.sessionID),
			zap.Error(err))
		return err
	}

	commitDetail := &util.CommitDetails{
		WriteSize:   size,
		WriteKeys:   c.mutations.Len(),
		ResolveLock: util.ResolveLockDetail{},
	}
	metrics.TiKVTxnWriteKVCountHistogram.Observe(float64(commitDetail.WriteKeys))
	metrics.TiKVTxnWriteSizeHistogram.Observe(float64(commitDetail.WriteSize))
	c.hasNoNeedCommitKeys = checkCnt > 0
	c.lockTTL = txnLockTTL(txn.startTime, size)
	c.priority = txn.priority.ToPB()
	c.syncLog = txn.syncLog
	c.resourceGroupTag = txn.resourceGroupTag
	c.resourceGroupTagger = txn.resourceGroupTagger
	c.resourceGroupName = txn.resourceGroupName
	c.setDetail(commitDetail)

	return nil
}

func (c *twoPhaseCommitter) primary() []byte {
	if len(c.primaryKey) == 0 {
		if c.mutations != nil {
			return c.mutations.GetKey(0)
		}
		return nil
	}
	return c.primaryKey
}

// asyncSecondaries returns all keys that must be checked in the recovery phase of an async commit.
func (c *twoPhaseCommitter) asyncSecondaries() [][]byte {
	secondaries := make([][]byte, 0, c.mutations.Len())
	for i := 0; i < c.mutations.Len(); i++ {
		k := c.mutations.GetKey(i)
		if bytes.Equal(k, c.primary()) || c.mutations.GetOp(i) == kvrpcpb.Op_CheckNotExists {
			continue
		}
		secondaries = append(secondaries, k)
	}
	return secondaries
}

const bytesPerMiB = 1024 * 1024

// ttl = ttlFactor * sqrt(writeSizeInMiB)
var ttlFactor = 6000

// By default, locks after 3000ms is considered unusual (the client created the
// lock might be dead). Other client may cleanup this kind of lock.
// For locks created recently, we will do backoff and retry.
var defaultLockTTL uint64 = 3000

func txnLockTTL(startTime time.Time, txnSize int) uint64 {
	// Increase lockTTL for large transactions.
	// The formula is `ttl = ttlFactor * sqrt(sizeInMiB)`.
	// When writeSize is less than 256KB, the base ttl is defaultTTL (3s);
	// When writeSize is 1MiB, 4MiB, or 10MiB, ttl is 6s, 12s, 20s correspondingly;
	lockTTL := defaultLockTTL
	if txnSize >= int(kv.TxnCommitBatchSize.Load()) {
		sizeMiB := float64(txnSize) / bytesPerMiB
		lockTTL = uint64(float64(ttlFactor) * math.Sqrt(sizeMiB))
		if lockTTL < defaultLockTTL {
			lockTTL = defaultLockTTL
		}
		if lockTTL > ManagedLockTTL {
			lockTTL = ManagedLockTTL
		}
	}

	// Increase lockTTL by the transaction's read time.
	// When resolving a lock, we compare current ts and startTS+lockTTL to decide whether to clean up. If a txn
	// takes a long time to read, increasing its TTL will help to prevent it from been aborted soon after prewrite.
	elapsed := time.Since(startTime) / time.Millisecond
	return lockTTL + uint64(elapsed)
}

var preSplitDetectThreshold uint32 = 100000
var preSplitSizeThreshold uint32 = 32 << 20

// doActionOnMutations groups keys into primary batch and secondary batches, if primary batch exists in the key,
// it does action on primary batch first, then on secondary batches. If action is commit, secondary batches
// is done in background goroutine.
func (c *twoPhaseCommitter) doActionOnMutations(bo *retry.Backoffer, action twoPhaseCommitAction, mutations CommitterMutations) error {
	if mutations.Len() == 0 {
		return nil
	}
	groups, err := c.groupMutations(bo, mutations)
	if err != nil {
		return err
	}

	// This is redundant since `doActionOnGroupMutations` will still split groups into batches and
	// check the number of batches. However we don't want the check fail after any code changes.
	c.checkOnePCFallBack(action, len(groups))

	return c.doActionOnGroupMutations(bo, action, groups)
}

type groupedMutations struct {
	region    locate.RegionVerID
	mutations CommitterMutations
}

// groupSortedMutationsByRegion separates keys into groups by their belonging Regions.
func groupSortedMutationsByRegion(c *locate.RegionCache, bo *retry.Backoffer, m CommitterMutations) ([]groupedMutations, error) {
	var (
		groups  []groupedMutations
		lastLoc *locate.KeyLocation
	)
	lastUpperBound := 0
	for i := 0; i < m.Len(); i++ {
		if lastLoc == nil || !lastLoc.Contains(m.GetKey(i)) {
			if lastLoc != nil {
				groups = append(groups, groupedMutations{
					region:    lastLoc.Region,
					mutations: m.Slice(lastUpperBound, i),
				})
				lastUpperBound = i
			}
			var err error
			lastLoc, err = c.LocateKey(bo, m.GetKey(i))
			if err != nil {
				return nil, err
			}
		}
	}
	if lastLoc != nil {
		groups = append(groups, groupedMutations{
			region:    lastLoc.Region,
			mutations: m.Slice(lastUpperBound, m.Len()),
		})
	}
	return groups, nil
}

// groupMutations groups mutations by region, then checks for any large groups and in that case pre-splits the region.
func (c *twoPhaseCommitter) groupMutations(bo *retry.Backoffer, mutations CommitterMutations) ([]groupedMutations, error) {
	groups, err := groupSortedMutationsByRegion(c.store.GetRegionCache(), bo, mutations)
	if err != nil {
		return nil, err
	}

	// Pre-split regions to avoid too much write workload into a single region.
	// In the large transaction case, this operation is important to avoid TiKV 'server is busy' error.
	var didPreSplit bool
	preSplitDetectThresholdVal := atomic.LoadUint32(&preSplitDetectThreshold)
	for _, group := range groups {
		if uint32(group.mutations.Len()) >= preSplitDetectThresholdVal {
			logutil.BgLogger().Info("2PC detect large amount of mutations on a single region",
				zap.Uint64("region", group.region.GetID()),
				zap.Int("mutations count", group.mutations.Len()))
			if c.preSplitRegion(bo.GetCtx(), group) {
				didPreSplit = true
			}
		}
	}
	// Reload region cache again.
	if didPreSplit {
		groups, err = groupSortedMutationsByRegion(c.store.GetRegionCache(), bo, mutations)
		if err != nil {
			return nil, err
		}
	}

	return groups, nil
}

func (c *twoPhaseCommitter) preSplitRegion(ctx context.Context, group groupedMutations) bool {
	splitKeys := make([][]byte, 0, 4)

	preSplitSizeThresholdVal := atomic.LoadUint32(&preSplitSizeThreshold)
	regionSize := 0
	keysLength := group.mutations.Len()
	// The value length maybe zero for pessimistic lock keys
	for i := 0; i < keysLength; i++ {
		regionSize = regionSize + len(group.mutations.GetKey(i)) + len(group.mutations.GetValue(i))
		// The second condition is used for testing.
		if regionSize >= int(preSplitSizeThresholdVal) {
			regionSize = 0
			splitKeys = append(splitKeys, group.mutations.GetKey(i))
		}
	}
	if len(splitKeys) == 0 {
		return false
	}

	regionIDs, err := c.store.SplitRegions(ctx, splitKeys, true, nil)
	if err != nil {
		logutil.BgLogger().Warn("2PC split regions failed", zap.Uint64("regionID", group.region.GetID()),
			zap.Int("keys count", keysLength), zap.Error(err))
		return false
	}

	for _, regionID := range regionIDs {
		err := c.store.WaitScatterRegionFinish(ctx, regionID, 0)
		if err != nil {
			logutil.BgLogger().Warn("2PC wait scatter region failed", zap.Uint64("regionID", regionID), zap.Error(err))
		}
	}
	// Invalidate the old region cache information.
	c.store.GetRegionCache().InvalidateCachedRegion(group.region)
	return true
}

// CommitSecondaryMaxBackoff is max sleep time of the 'commit' command
const CommitSecondaryMaxBackoff = 41000

// doActionOnGroupedMutations splits groups into batches (there is one group per region, and potentially many batches per group, but all mutations
// in a batch will belong to the same region).
func (c *twoPhaseCommitter) doActionOnGroupMutations(bo *retry.Backoffer, action twoPhaseCommitAction, groups []groupedMutations) error {
	action.tiKVTxnRegionsNumHistogram().Observe(float64(len(groups)))

	var sizeFunc = c.keySize

	switch act := action.(type) {
	case actionPrewrite:
		// Do not update regionTxnSize on retries. They are not used when building a PrewriteRequest.
		if !act.retry {
			for _, group := range groups {
				c.regionTxnSize[group.region.GetID()] = group.mutations.Len()
			}
		}
		sizeFunc = c.keyValueSize
		atomic.AddInt32(&c.getDetail().PrewriteRegionNum, int32(len(groups)))
	case actionPessimisticLock:
		if act.LockCtx.Stats != nil {
			act.LockCtx.Stats.RegionNum = int32(len(groups))
		}
	}

	batchBuilder := newBatched(c.primary())
	for _, group := range groups {
		batchBuilder.appendBatchMutationsBySize(group.region, group.mutations, sizeFunc,
			int(kv.TxnCommitBatchSize.Load()))
	}
	firstIsPrimary := batchBuilder.setPrimary()

	actionCommit, actionIsCommit := action.(actionCommit)
	_, actionIsCleanup := action.(actionCleanup)
	_, actionIsPessimisticLock := action.(actionPessimisticLock)
	_, actionIsPrewrite := action.(actionPrewrite)

	c.checkOnePCFallBack(action, len(batchBuilder.allBatches()))

	var err error
	if val, err := util.EvalFailpoint("skipKeyReturnOK"); err == nil {
		valStr, ok := val.(string)
		if ok && c.sessionID > 0 {
			if firstIsPrimary && actionIsPessimisticLock {
				logutil.Logger(bo.GetCtx()).Warn("pessimisticLock failpoint", zap.String("valStr", valStr))
				switch valStr {
				case "pessimisticLockSkipPrimary":
					err = c.doActionOnBatches(bo, action, batchBuilder.allBatches())
					return err
				case "pessimisticLockSkipSecondary":
					err = c.doActionOnBatches(bo, action, batchBuilder.primaryBatch())
					return err
				}
			}
		}
	}
	if _, err := util.EvalFailpoint("pessimisticRollbackDoNth"); err == nil {
		_, actionIsPessimisticRollback := action.(actionPessimisticRollback)
		if actionIsPessimisticRollback && c.sessionID > 0 {
			logutil.Logger(bo.GetCtx()).Warn("pessimisticRollbackDoNth failpoint")
			return nil
		}
	}

	if actionIsPrewrite && c.prewriteTotalReqNum == 0 && len(batchBuilder.allBatches()) > 0 {
		c.prewriteTotalReqNum = len(batchBuilder.allBatches())
	}

	if firstIsPrimary &&
		((actionIsCommit && !c.isAsyncCommit()) || actionIsCleanup || actionIsPessimisticLock) {
		// primary should be committed(not async commit)/cleanup/pessimistically locked first
		err = c.doActionOnBatches(bo, action, batchBuilder.primaryBatch())
		if err != nil {
			return err
		}
		if actionIsCommit && c.testingKnobs.bkAfterCommitPrimary != nil && c.testingKnobs.acAfterCommitPrimary != nil {
			c.testingKnobs.acAfterCommitPrimary <- struct{}{}
			<-c.testingKnobs.bkAfterCommitPrimary
		}
		batchBuilder.forgetPrimary()
	}
	util.EvalFailpoint("afterPrimaryBatch")

	// Already spawned a goroutine for async commit transaction.
	if actionIsCommit && !actionCommit.retry && !c.isAsyncCommit() {
		secondaryBo := retry.NewBackofferWithVars(c.store.Ctx(), CommitSecondaryMaxBackoff, c.txn.vars)
		if c.store.IsClose() {
			logutil.Logger(bo.GetCtx()).Warn("the store is closed",
				zap.Uint64("startTS", c.startTS), zap.Uint64("commitTS", c.commitTS),
				zap.Uint64("sessionID", c.sessionID))
			return nil
		}
		c.store.WaitGroup().Add(1)
		c.store.Go(func() {
			defer c.store.WaitGroup().Done()
			if c.sessionID > 0 {
				if v, err := util.EvalFailpoint("beforeCommitSecondaries"); err == nil {
					if s, ok := v.(string); !ok {
						logutil.Logger(bo.GetCtx()).Info("[failpoint] sleep 2s before commit secondary keys",
							zap.Uint64("sessionID", c.sessionID), zap.Uint64("txnStartTS", c.startTS), zap.Uint64("txnCommitTS", c.commitTS))
						time.Sleep(2 * time.Second)
					} else if s == "skip" {
						logutil.Logger(bo.GetCtx()).Info("[failpoint] injected skip committing secondaries",
							zap.Uint64("sessionID", c.sessionID), zap.Uint64("txnStartTS", c.startTS), zap.Uint64("txnCommitTS", c.commitTS))
						return
					}
				}
			}

			e := c.doActionOnBatches(secondaryBo, action, batchBuilder.allBatches())
			if e != nil {
				logutil.BgLogger().Debug("2PC async doActionOnBatches",
					zap.Uint64("session", c.sessionID),
					zap.Stringer("action type", action),
					zap.Error(e))
				metrics.SecondaryLockCleanupFailureCounterCommit.Inc()
			}
		})

	} else {
		err = c.doActionOnBatches(bo, action, batchBuilder.allBatches())
	}
	return err
}

// doActionOnBatches does action to batches in parallel.
func (c *twoPhaseCommitter) doActionOnBatches(bo *retry.Backoffer, action twoPhaseCommitAction, batches []batchMutations) error {
	if len(batches) == 0 {
		return nil
	}

	noNeedFork := len(batches) == 1
	if !noNeedFork {
		if ac, ok := action.(actionCommit); ok && ac.retry {
			noNeedFork = true
		}
	}
	if noNeedFork {
		for _, b := range batches {
			e := action.handleSingleBatch(c, bo, b)
			if e != nil {
				logutil.BgLogger().Debug("2PC doActionOnBatches failed",
					zap.Uint64("session", c.sessionID),
					zap.Stringer("action type", action),
					zap.Error(e),
					zap.Uint64("txnStartTS", c.startTS))
				return e
			}
		}
		return nil
	}
	rateLim := len(batches)
	// Set rateLim here for the large transaction.
	// If the rate limit is too high, tikv will report service is busy.
	// If the rate limit is too low, we can't full utilize the tikv's throughput.
	// TODO: Find a self-adaptive way to control the rate limit here.
	if rateLim > config.GetGlobalConfig().CommitterConcurrency {
		rateLim = config.GetGlobalConfig().CommitterConcurrency
	}
	batchExecutor := newBatchExecutor(rateLim, c, action, bo)
	return batchExecutor.process(batches)
}

func (c *twoPhaseCommitter) keyValueSize(key, value []byte) int {
	return len(key) + len(value)
}

func (c *twoPhaseCommitter) keySize(key, value []byte) int {
	return len(key)
}

func (c *twoPhaseCommitter) SetDiskFullOpt(level kvrpcpb.DiskFullOpt) {
	c.diskFullOpt = level
}

func (c *twoPhaseCommitter) SetTxnSource(txnSource uint64) {
	c.txnSource = txnSource
}

type ttlManagerState uint32

const (
	stateUninitialized ttlManagerState = iota
	stateRunning
	stateClosed
)

type ttlManager struct {
	state   ttlManagerState
	ch      chan struct{}
	lockCtx *kv.LockCtx
}

func (tm *ttlManager) run(c *twoPhaseCommitter, lockCtx *kv.LockCtx) {
	if _, err := util.EvalFailpoint("doNotKeepAlive"); err == nil {
		return
	}

	// Run only once.
	if !atomic.CompareAndSwapUint32((*uint32)(&tm.state), uint32(stateUninitialized), uint32(stateRunning)) {
		return
	}
	tm.ch = make(chan struct{})
	tm.lockCtx = lockCtx

	go keepAlive(c, tm.ch, c.primary(), lockCtx)
}

func (tm *ttlManager) close() {
	if !atomic.CompareAndSwapUint32((*uint32)(&tm.state), uint32(stateRunning), uint32(stateClosed)) {
		return
	}
	close(tm.ch)
}

func (tm *ttlManager) reset() {
	if !atomic.CompareAndSwapUint32((*uint32)(&tm.state), uint32(stateRunning), uint32(stateUninitialized)) {
		return
	}
	close(tm.ch)
}

const keepAliveMaxBackoff = 20000
const pessimisticLockMaxBackoff = 20000
const maxConsecutiveFailure = 10

func keepAlive(c *twoPhaseCommitter, closeCh chan struct{}, primaryKey []byte, lockCtx *kv.LockCtx) {
	// Ticker is set to 1/2 of the ManagedLockTTL.
	ticker := time.NewTicker(time.Duration(atomic.LoadUint64(&ManagedLockTTL)) * time.Millisecond / 2)
	defer ticker.Stop()
	keepFail := 0
	for {
		select {
		case <-closeCh:
			return
		case <-ticker.C:
			// If kill signal is received, the ttlManager should exit.
			if lockCtx != nil && lockCtx.Killed != nil && atomic.LoadUint32(lockCtx.Killed) != 0 {
				return
			}
			bo := retry.NewBackofferWithVars(context.Background(), keepAliveMaxBackoff, c.txn.vars)
			now, err := c.store.GetTimestampWithRetry(bo, c.txn.GetScope())
			if err != nil {
				logutil.Logger(bo.GetCtx()).Warn("keepAlive get tso fail",
					zap.Error(err))
				return
			}

			uptime := uint64(oracle.ExtractPhysical(now) - oracle.ExtractPhysical(c.startTS))
			if uptime > config.GetGlobalConfig().MaxTxnTTL {
				// Checks maximum lifetime for the ttlManager, so when something goes wrong
				// the key will not be locked forever.
				logutil.Logger(bo.GetCtx()).Info("ttlManager live up to its lifetime",
					zap.Uint64("txnStartTS", c.startTS),
					zap.Uint64("uptime", uptime),
					zap.Uint64("maxTxnTTL", config.GetGlobalConfig().MaxTxnTTL))
				metrics.TiKVTTLLifeTimeReachCounter.Inc()
				// the pessimistic locks may expire if the ttl manager has timed out, set `LockExpired` flag
				// so that this transaction could only commit or rollback with no more statement executions
				if c.isPessimistic && lockCtx != nil && lockCtx.LockExpired != nil {
					atomic.StoreUint32(lockCtx.LockExpired, 1)
				}
				return
			}

			newTTL := uptime + atomic.LoadUint64(&ManagedLockTTL)
			logutil.Logger(bo.GetCtx()).Info("send TxnHeartBeat",
				zap.Uint64("startTS", c.startTS), zap.Uint64("newTTL", newTTL))
			startTime := time.Now()
			_, stopHeartBeat, err := sendTxnHeartBeat(bo, c.store, primaryKey, c.startTS, newTTL)
			if err != nil {
				keepFail++
				metrics.TxnHeartBeatHistogramError.Observe(time.Since(startTime).Seconds())
				logutil.Logger(bo.GetCtx()).Debug("send TxnHeartBeat failed",
					zap.Error(err),
					zap.Uint64("txnStartTS", c.startTS))
				if stopHeartBeat || keepFail > maxConsecutiveFailure {
					logutil.Logger(bo.GetCtx()).Warn("stop TxnHeartBeat",
						zap.Error(err),
						zap.Int("consecutiveFailure", keepFail),
						zap.Uint64("txnStartTS", c.startTS))
					return
				}
				continue
			}
			keepFail = 0
			metrics.TxnHeartBeatHistogramOK.Observe(time.Since(startTime).Seconds())
		}
	}
}

func sendTxnHeartBeat(bo *retry.Backoffer, store kvstore, primary []byte, startTS, ttl uint64) (newTTL uint64, stopHeartBeat bool, err error) {
	req := tikvrpc.NewRequest(tikvrpc.CmdTxnHeartBeat, &kvrpcpb.TxnHeartBeatRequest{
		PrimaryLock:   primary,
		StartVersion:  startTS,
		AdviseLockTtl: ttl,
	})
	for {
		loc, err := store.GetRegionCache().LocateKey(bo, primary)
		if err != nil {
			return 0, false, err
		}
		req.MaxExecutionDurationMs = uint64(client.MaxWriteExecutionTime.Milliseconds())
		resp, err := store.SendReq(bo, req, loc.Region, client.ReadTimeoutShort)
		if err != nil {
			return 0, false, err
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return 0, false, err
		}
		if regionErr != nil {
			// For other region error and the fake region error, backoff because
			// there's something wrong.
			// For the real EpochNotMatch error, don't backoff.
			if regionErr.GetEpochNotMatch() == nil || locate.IsFakeRegionError(regionErr) {
				err = bo.Backoff(retry.BoRegionMiss, errors.New(regionErr.String()))
				if err != nil {
					return 0, false, err
				}
			}
			continue
		}
		if resp.Resp == nil {
			return 0, false, errors.WithStack(tikverr.ErrBodyMissing)
		}
		cmdResp := resp.Resp.(*kvrpcpb.TxnHeartBeatResponse)
		if keyErr := cmdResp.GetError(); keyErr != nil {
			return 0, true, errors.Errorf("txn %d heartbeat fail, primary key = %v, err = %s", startTS, hex.EncodeToString(primary), tikverr.ExtractKeyErr(keyErr))
		}
		return cmdResp.GetLockTtl(), false, nil
	}
}

// checkAsyncCommit checks if async commit protocol is available for current transaction commit, true is returned if possible.
func (c *twoPhaseCommitter) checkAsyncCommit() bool {
	// Disable async commit in local transactions
	if c.txn.GetScope() != oracle.GlobalTxnScope {
		return false
	}

	// Don't use async commit when commitTSUpperBoundCheck is set.
	// For TiDB, this is used by cached table.
	if c.txn.commitTSUpperBoundCheck != nil {
		return false
	}

	asyncCommitCfg := config.GetGlobalConfig().TiKVClient.AsyncCommit
	// TODO the keys limit need more tests, this value makes the unit test pass by now.
	// Async commit is not compatible with Binlog because of the non unique timestamp issue.
	if c.txn.enableAsyncCommit &&
		uint(c.mutations.Len()) <= asyncCommitCfg.KeysLimit &&
		!c.shouldWriteBinlog() {
		totalKeySize := uint64(0)
		for i := 0; i < c.mutations.Len(); i++ {
			totalKeySize += uint64(len(c.mutations.GetKey(i)))
			if totalKeySize > asyncCommitCfg.TotalKeySizeLimit {
				return false
			}
		}
		return true
	}
	return false
}

// checkOnePC checks if 1PC protocol is available for current transaction.
func (c *twoPhaseCommitter) checkOnePC() bool {
	// Disable 1PC in local transactions
	if c.txn.GetScope() != oracle.GlobalTxnScope {
		return false
	}
	// Disable 1PC for transaction when commitTSUpperBoundCheck is set.
	if c.txn.commitTSUpperBoundCheck != nil {
		return false
	}

	return !c.shouldWriteBinlog() && c.txn.enable1PC
}

func (c *twoPhaseCommitter) needLinearizability() bool {
	return !c.txn.causalConsistency
}

func (c *twoPhaseCommitter) isAsyncCommit() bool {
	return atomic.LoadUint32(&c.useAsyncCommit) > 0
}

func (c *twoPhaseCommitter) setAsyncCommit(val bool) {
	if val {
		atomic.StoreUint32(&c.useAsyncCommit, 1)
	} else {
		atomic.StoreUint32(&c.useAsyncCommit, 0)
	}
}

func (c *twoPhaseCommitter) isOnePC() bool {
	return atomic.LoadUint32(&c.useOnePC) > 0
}

func (c *twoPhaseCommitter) setOnePC(val bool) {
	if val {
		atomic.StoreUint32(&c.useOnePC, 1)
	} else {
		atomic.StoreUint32(&c.useOnePC, 0)
	}
}

func (c *twoPhaseCommitter) checkOnePCFallBack(action twoPhaseCommitAction, batchCount int) {
	if _, ok := action.(actionPrewrite); ok {
		if batchCount > 1 {
			c.setOnePC(false)
		}
	}
}

const (
	cleanupMaxBackoff = 20000
	// TsoMaxBackoff is the max sleep time to get tso.
	TsoMaxBackoff = 15000
)

func (c *twoPhaseCommitter) cleanup(ctx context.Context) {
	if c.store.IsClose() {
		logutil.Logger(ctx).Warn("twoPhaseCommitter fail to cleanup because the store exited",
			zap.Uint64("txnStartTS", c.startTS), zap.Bool("isPessimistic", c.isPessimistic),
			zap.Bool("isOnePC", c.isOnePC()))
		return
	}
	c.cleanWg.Add(1)
	c.store.WaitGroup().Add(1)
	go func() {
		defer c.store.WaitGroup().Done()
		if _, err := util.EvalFailpoint("commitFailedSkipCleanup"); err == nil {
			logutil.Logger(ctx).Info("[failpoint] injected skip cleanup secondaries on failure",
				zap.Uint64("txnStartTS", c.startTS))
			c.cleanWg.Done()
			return
		}

		cleanupKeysCtx := context.WithValue(c.store.Ctx(), retry.TxnStartKey, ctx.Value(retry.TxnStartKey))
		var err error
		if !c.isOnePC() {
			err = c.cleanupMutations(retry.NewBackofferWithVars(cleanupKeysCtx, cleanupMaxBackoff, c.txn.vars), c.mutations)
		} else if c.isPessimistic {
			err = c.pessimisticRollbackMutations(retry.NewBackofferWithVars(cleanupKeysCtx, cleanupMaxBackoff, c.txn.vars), c.mutations)
		}

		if err != nil {
			metrics.SecondaryLockCleanupFailureCounterRollback.Inc()
			logutil.Logger(ctx).Info("2PC cleanup failed", zap.Error(err), zap.Uint64("txnStartTS", c.startTS),
				zap.Bool("isPessimistic", c.isPessimistic), zap.Bool("isOnePC", c.isOnePC()))
		} else {
			logutil.Logger(ctx).Debug("2PC clean up done",
				zap.Uint64("txnStartTS", c.startTS), zap.Bool("isPessimistic", c.isPessimistic),
				zap.Bool("isOnePC", c.isOnePC()))
		}
		c.cleanWg.Done()
	}()
}

// execute executes the two-phase commit protocol.
func (c *twoPhaseCommitter) execute(ctx context.Context) (err error) {
	var binlogSkipped bool
	defer func() {
		if c.isOnePC() {
			// The error means the 1PC transaction failed.
			if err != nil {
				if c.getUndeterminedErr() == nil {
					c.cleanup(ctx)
				}
				metrics.OnePCTxnCounterError.Inc()
			} else {
				metrics.OnePCTxnCounterOk.Inc()
			}
		} else if c.isAsyncCommit() {
			// The error means the async commit should not succeed.
			if err != nil {
				if c.getUndeterminedErr() == nil {
					c.cleanup(ctx)
				}
				metrics.AsyncCommitTxnCounterError.Inc()
			} else {
				metrics.AsyncCommitTxnCounterOk.Inc()
			}
		} else {
			// Always clean up all written keys if the txn does not commit.
			c.mu.RLock()
			committed := c.mu.committed
			undetermined := c.mu.undeterminedErr != nil
			c.mu.RUnlock()
			if !committed && !undetermined {
				c.cleanup(ctx)
				metrics.TwoPCTxnCounterError.Inc()
			} else {
				metrics.TwoPCTxnCounterOk.Inc()
			}
			c.txn.commitTS = c.commitTS
			if binlogSkipped {
				c.binlog.Skip()
				return
			}
			if !c.shouldWriteBinlog() {
				return
			}
			if err != nil {
				c.binlog.Commit(ctx, 0)
			} else {
				c.binlog.Commit(ctx, int64(c.commitTS))
			}
		}
	}()

	commitDetail := c.getDetail()
	commitTSMayBeCalculated := false
	// Check async commit is available or not.
	if c.checkAsyncCommit() {
		commitTSMayBeCalculated = true
		c.setAsyncCommit(true)
		c.hasTriedAsyncCommit = true
	}
	// Check if 1PC is enabled.
	if c.checkOnePC() {
		commitTSMayBeCalculated = true
		c.setOnePC(true)
		c.hasTriedOnePC = true
	}

	// if lazy uniqueness check is enabled in TiDB (@@constraint_check_in_place_pessimistic=0), for_update_ts might be
	// zero for a pessimistic transaction. We set it to the start_ts to force the PrewritePessimistic path in TiKV.
	// TODO: can we simply set for_update_ts = start_ts for all pessimistic transactions whose for_update_ts=0?
	if c.forUpdateTS == 0 {
		for i := 0; i < c.mutations.Len(); i++ {
			if c.mutations.NeedConstraintCheckInPrewrite(i) {
				c.forUpdateTS = c.startTS
				break
			}
		}
	}

	// TODO(youjiali1995): It's better to use different maxSleep for different operations
	// and distinguish permanent errors from temporary errors, for example:
	//   - If all PDs are down, all requests to PD will fail due to network error.
	//     The maxSleep should't be very long in this case.
	//   - If the region isn't found in PD, it's possible the reason is write-stall.
	//     The maxSleep can be long in this case.
	bo := retry.NewBackofferWithVars(ctx, int(PrewriteMaxBackoff.Load()), c.txn.vars)

	// If we want to use async commit or 1PC and also want linearizability across
	// all nodes, we have to make sure the commit TS of this transaction is greater
	// than the snapshot TS of all existent readers. So we get a new timestamp
	// from PD and plus one as our MinCommitTS.
	if commitTSMayBeCalculated && c.needLinearizability() {
		util.EvalFailpoint("getMinCommitTSFromTSO")
		start := time.Now()
		latestTS, err := c.store.GetTimestampWithRetry(bo, c.txn.GetScope())
		// If we fail to get a timestamp from PD, we just propagate the failure
		// instead of falling back to the normal 2PC because a normal 2PC will
		// also be likely to fail due to the same timestamp issue.
		if err != nil {
			return err
		}
		commitDetail.GetLatestTsTime = time.Since(start)
		// Plus 1 to avoid producing the same commit TS with previously committed transactions
		c.minCommitTS = latestTS + 1
	}
	// Calculate maxCommitTS if necessary
	if commitTSMayBeCalculated {
		if err = c.calculateMaxCommitTS(ctx); err != nil {
			return err
		}
	}

	if c.sessionID > 0 {
		util.EvalFailpoint("beforePrewrite")
	}

	c.prewriteStarted = true
	var binlogChan <-chan BinlogWriteResult
	if c.shouldWriteBinlog() {
		binlogChan = c.binlog.Prewrite(ctx, c.primary())
	}

	start := time.Now()

	err = c.prewriteMutations(bo, c.mutations)

	if err != nil {
		if assertionFailed, ok := errors.Cause(err).(*tikverr.ErrAssertionFailed); ok {
			err = c.checkSchemaOnAssertionFail(ctx, assertionFailed)
		}

		// TODO: Now we return an undetermined error as long as one of the prewrite
		// RPCs fails. However, if there are multiple errors and some of the errors
		// are not RPC failures, we can return the actual error instead of undetermined.
		if undeterminedErr := c.getUndeterminedErr(); undeterminedErr != nil {
			logutil.Logger(ctx).Error("Async commit/1PC result undetermined",
				zap.Error(err),
				zap.NamedError("rpcErr", undeterminedErr),
				zap.Uint64("txnStartTS", c.startTS))
			return errors.WithStack(tikverr.ErrResultUndetermined)
		}
	}

	commitDetail.PrewriteTime = time.Since(start)
	commitDetail.PrewriteReqNum = c.prewriteTotalReqNum
	if bo.GetTotalSleep() > 0 {
		boSleep := int64(bo.GetTotalSleep()) * int64(time.Millisecond)
		commitDetail.Mu.Lock()
		if boSleep > commitDetail.Mu.CommitBackoffTime {
			commitDetail.Mu.CommitBackoffTime = boSleep
			commitDetail.Mu.PrewriteBackoffTypes = bo.GetTypes()
		}
		commitDetail.Mu.Unlock()
	}

	if binlogChan != nil {
		startWaitBinlog := time.Now()
		binlogWriteResult := <-binlogChan
		commitDetail.WaitPrewriteBinlogTime = time.Since(startWaitBinlog)
		if binlogWriteResult != nil {
			binlogSkipped = binlogWriteResult.Skipped()
			binlogErr := binlogWriteResult.GetError()
			if binlogErr != nil {
				return binlogErr
			}
		}
	}
	if err != nil {
		logutil.Logger(ctx).Debug("2PC failed on prewrite",
			zap.Error(err),
			zap.Uint64("txnStartTS", c.startTS))
		return err
	}

	// return assertion error found in TiDB after prewrite succeeds to prevent false positive. Note this is only visible
	// when async commit or 1PC is disabled.
	if c.stashedAssertionError != nil {
		if c.isAsyncCommit() || c.isOnePC() {
			// should be unreachable
			panic("tidb-side assertion error should forbids async commit or 1PC")
		}
		return c.stashedAssertionError
	}

	// strip check_not_exists keys that no need to commit.
	c.stripNoNeedCommitKeys()

	var commitTS uint64

	if c.isOnePC() {
		if c.onePCCommitTS == 0 {
			return errors.Errorf("session %d invalid onePCCommitTS for 1PC protocol after prewrite, startTS=%v", c.sessionID, c.startTS)
		}
		c.commitTS = c.onePCCommitTS
		c.txn.commitTS = c.commitTS
		logutil.Logger(ctx).Debug("1PC protocol is used to commit this txn",
			zap.Uint64("startTS", c.startTS), zap.Uint64("commitTS", c.commitTS),
			zap.Uint64("session", c.sessionID))
		return nil
	}

	if c.onePCCommitTS != 0 {
		logutil.Logger(ctx).Fatal("non 1PC transaction committed in 1PC",
			zap.Uint64("session", c.sessionID), zap.Uint64("startTS", c.startTS))
	}

	if c.isAsyncCommit() {
		if c.minCommitTS == 0 {
			return errors.Errorf("session %d invalid minCommitTS for async commit protocol after prewrite, startTS=%v", c.sessionID, c.startTS)
		}
		commitTS = c.minCommitTS
	} else {
		start = time.Now()
		logutil.Event(ctx, "start get commit ts")
		commitTS, err = c.store.GetTimestampWithRetry(retry.NewBackofferWithVars(ctx, TsoMaxBackoff, c.txn.vars), c.txn.GetScope())
		if err != nil {
			logutil.Logger(ctx).Warn("2PC get commitTS failed",
				zap.Error(err),
				zap.Uint64("txnStartTS", c.startTS))
			return err
		}
		commitDetail.GetCommitTsTime = time.Since(start)
		logutil.Event(ctx, "finish get commit ts")
		logutil.SetTag(ctx, "commitTs", commitTS)
	}

	if !c.isAsyncCommit() {
		err = c.checkSchemaValid(ctx, commitTS, c.txn.schemaVer)
		if err != nil {
			return err
		}
	}
	atomic.StoreUint64(&c.commitTS, commitTS)

	if c.store.GetOracle().IsExpired(c.startTS, MaxTxnTimeUse, &oracle.Option{TxnScope: oracle.GlobalTxnScope}) {
		err = errors.Errorf("session %d txn takes too much time, txnStartTS: %d, comm: %d",
			c.sessionID, c.startTS, c.commitTS)
		return err
	}
	if c.txn.commitTSUpperBoundCheck != nil {
		if !c.txn.commitTSUpperBoundCheck(commitTS) {
			err = errors.Errorf("session %d check commit ts upper bound fail, txnStartTS: %d, comm: %d",
				c.sessionID, c.startTS, c.commitTS)
			return err
		}
	}

	if c.sessionID > 0 {
		if val, err := util.EvalFailpoint("beforeCommit"); err == nil {
			// Pass multiple instructions in one string, delimited by commas, to trigger multiple behaviors, like
			// `return("delay,fail")`. Then they will be executed sequentially at once.
			if v, ok := val.(string); ok {
				for _, action := range strings.Split(v, ",") {
					// Async commit transactions cannot return error here, since it's already successful.
					if action == "fail" && !c.isAsyncCommit() {
						logutil.Logger(ctx).Info("[failpoint] injected failure before commit", zap.Uint64("txnStartTS", c.startTS))
						return errors.New("injected failure before commit")
					} else if action == "delay" {
						duration := time.Duration(rand.Int63n(int64(time.Second) * 5))
						logutil.Logger(ctx).Info("[failpoint] injected delay before commit",
							zap.Uint64("txnStartTS", c.startTS), zap.Duration("duration", duration))
						time.Sleep(duration)
					}
				}
			}
		}
	}

	if c.isAsyncCommit() {
		// For async commit protocol, the commit is considered success here.
		c.txn.commitTS = c.commitTS
		logutil.Logger(ctx).Debug("2PC will use async commit protocol to commit this txn",
			zap.Uint64("startTS", c.startTS), zap.Uint64("commitTS", c.commitTS),
			zap.Uint64("sessionID", c.sessionID))
		if c.store.IsClose() {
			logutil.Logger(ctx).Warn("2PC will use async commit protocol to commit this txn but the store is closed",
				zap.Uint64("startTS", c.startTS), zap.Uint64("commitTS", c.commitTS),
				zap.Uint64("sessionID", c.sessionID))
			return nil
		}
		c.store.WaitGroup().Add(1)
		go func() {
			defer c.store.WaitGroup().Done()
			if _, err := util.EvalFailpoint("asyncCommitDoNothing"); err == nil {
				return
			}
			commitBo := retry.NewBackofferWithVars(c.store.Ctx(), CommitSecondaryMaxBackoff, c.txn.vars)
			err := c.commitMutations(commitBo, c.mutations)
			if err != nil {
				logutil.Logger(ctx).Warn("2PC async commit failed", zap.Uint64("sessionID", c.sessionID),
					zap.Uint64("startTS", c.startTS), zap.Uint64("commitTS", c.commitTS), zap.Error(err))
			}
		}()
		return nil
	}
	return c.commitTxn(ctx, commitDetail)
}

func (c *twoPhaseCommitter) commitTxn(ctx context.Context, commitDetail *util.CommitDetails) error {
	c.txn.GetMemBuffer().DiscardValues()
	start := time.Now()

	// Use the VeryLongMaxBackoff to commit the primary key.
	commitBo := retry.NewBackofferWithVars(ctx, int(CommitMaxBackoff), c.txn.vars)
	err := c.commitMutations(commitBo, c.mutations)
	commitDetail.CommitTime = time.Since(start)
	if commitBo.GetTotalSleep() > 0 {
		commitDetail.Mu.Lock()
		commitDetail.Mu.CommitBackoffTime += int64(commitBo.GetTotalSleep()) * int64(time.Millisecond)
		commitDetail.Mu.CommitBackoffTypes = append(commitDetail.Mu.CommitBackoffTypes, commitBo.GetTypes()...)
		commitDetail.Mu.Unlock()
	}
	if err != nil {
		if undeterminedErr := c.getUndeterminedErr(); undeterminedErr != nil {
			logutil.Logger(ctx).Error("2PC commit result undetermined",
				zap.Error(err),
				zap.NamedError("rpcErr", undeterminedErr),
				zap.Uint64("txnStartTS", c.startTS))
			err = errors.WithStack(tikverr.ErrResultUndetermined)
		}
		if !c.mu.committed {
			logutil.Logger(ctx).Debug("2PC failed on commit",
				zap.Error(err),
				zap.Uint64("txnStartTS", c.startTS))
			return err
		}
		logutil.Logger(ctx).Debug("got some exceptions, but 2PC was still successful",
			zap.Error(err),
			zap.Uint64("txnStartTS", c.startTS))
	}
	return nil
}

func (c *twoPhaseCommitter) stripNoNeedCommitKeys() {
	if !c.hasNoNeedCommitKeys {
		return
	}
	m := c.mutations
	var newIdx int
	for oldIdx := range m.handles {
		key := m.GetKey(oldIdx)
		flags, err := c.txn.GetMemBuffer().GetFlags(key)
		if err == nil && flags.HasPrewriteOnly() {
			continue
		}
		m.handles[newIdx] = m.handles[oldIdx]
		newIdx++
	}
	c.mutations.handles = c.mutations.handles[:newIdx]
}

// SchemaVer is the infoSchema which will return the schema version.
type SchemaVer interface {
	// SchemaMetaVersion returns the meta schema version.
	SchemaMetaVersion() int64
}

// SchemaLeaseChecker is used to validate schema version is not changed during transaction execution.
type SchemaLeaseChecker interface {
	// CheckBySchemaVer checks if the schema has changed for the transaction related tables between the startSchemaVer
	// and the schema version at txnTS, all the related schema changes will be returned.
	CheckBySchemaVer(txnTS uint64, startSchemaVer SchemaVer) (*RelatedSchemaChange, error)
}

// RelatedSchemaChange contains information about schema diff between two schema versions.
type RelatedSchemaChange struct {
	PhyTblIDS        []int64
	ActionTypes      []uint64
	LatestInfoSchema SchemaVer
}

// checkSchemaValid checks if the schema has changed.
func (c *twoPhaseCommitter) checkSchemaValid(ctx context.Context, checkTS uint64, startInfoSchema SchemaVer) error {
	if _, err := util.EvalFailpoint("failCheckSchemaValid"); err == nil {
		logutil.Logger(ctx).Info("[failpoint] injected fail schema check",
			zap.Uint64("txnStartTS", c.startTS))
		return errors.Errorf("mock check schema valid failure")
	}
	if c.txn.schemaLeaseChecker == nil {
		if c.sessionID > 0 {
			logutil.Logger(ctx).Warn("schemaLeaseChecker is not set for this transaction",
				zap.Uint64("sessionID", c.sessionID),
				zap.Uint64("startTS", c.startTS),
				zap.Uint64("checkTS", checkTS))
		}
		return nil
	}
	_, err := c.txn.schemaLeaseChecker.CheckBySchemaVer(checkTS, startInfoSchema)
	return errors.WithStack(err)
}

func (c *twoPhaseCommitter) calculateMaxCommitTS(ctx context.Context) error {
	currentTS := oracle.ComposeTS(int64(time.Since(c.txn.startTime)/time.Millisecond), 0) + c.startTS
	err := c.checkSchemaValid(ctx, currentTS, c.txn.schemaVer)
	if err != nil {
		logutil.Logger(ctx).Info("Schema changed for async commit txn",
			zap.Error(err),
			zap.Uint64("startTS", c.startTS))
		return err
	}

	safeWindow := config.GetGlobalConfig().TiKVClient.AsyncCommit.SafeWindow
	maxCommitTS := oracle.ComposeTS(int64(safeWindow/time.Millisecond), 0) + currentTS
	logutil.BgLogger().Debug("calculate MaxCommitTS",
		zap.Time("startTime", c.txn.startTime),
		zap.Duration("safeWindow", safeWindow),
		zap.Uint64("startTS", c.startTS),
		zap.Uint64("maxCommitTS", maxCommitTS))

	c.maxCommitTS = maxCommitTS
	return nil
}

func (c *twoPhaseCommitter) shouldWriteBinlog() bool {
	return c.binlog != nil
}

type batchMutations struct {
	region    locate.RegionVerID
	mutations CommitterMutations
	isPrimary bool
}

func (b *batchMutations) relocate(bo *retry.Backoffer, c *locate.RegionCache) (bool, error) {
	begin, end := b.mutations.GetKey(0), b.mutations.GetKey(b.mutations.Len()-1)
	loc, err := c.LocateKey(bo, begin)
	if err != nil {
		return false, err
	}
	if !loc.Contains(end) {
		return false, nil
	}
	b.region = loc.Region
	return true, nil
}

type batched struct {
	batches    []batchMutations
	primaryIdx int
	primaryKey []byte
}

func newBatched(primaryKey []byte) *batched {
	return &batched{
		primaryIdx: -1,
		primaryKey: primaryKey,
	}
}

// appendBatchMutationsBySize appends mutations to b. It may split the keys to make
// sure each batch's size does not exceed the limit.
func (b *batched) appendBatchMutationsBySize(region locate.RegionVerID, mutations CommitterMutations, sizeFn func(k, v []byte) int, limit int) {
	if _, err := util.EvalFailpoint("twoPCRequestBatchSizeLimit"); err == nil {
		limit = 1
	}

	var start, end int
	for start = 0; start < mutations.Len(); start = end {
		isPrimary := false
		var size int
		for end = start; end < mutations.Len() && size < limit; end++ {
			var k, v []byte
			k = mutations.GetKey(end)
			v = mutations.GetValue(end)
			size += sizeFn(k, v)
			if b.primaryIdx < 0 && bytes.Equal(k, b.primaryKey) {
				b.primaryIdx = len(b.batches)
				isPrimary = true
			}
		}
		b.batches = append(b.batches, batchMutations{
			region:    region,
			mutations: mutations.Slice(start, end),
			isPrimary: isPrimary,
		})
	}
}

func (b *batched) setPrimary() bool {
	// If the batches include the primary key, put it to the first
	if b.primaryIdx >= 0 {
		if len(b.batches) > 0 {
			b.batches[b.primaryIdx].isPrimary = true
			b.batches[0], b.batches[b.primaryIdx] = b.batches[b.primaryIdx], b.batches[0]
			b.primaryIdx = 0
		}
		return true
	}

	return false
}

func (b *batched) allBatches() []batchMutations {
	return b.batches
}

// primaryBatch returns the batch containing the primary key.
// Precondition: `b.setPrimary() == true`
func (b *batched) primaryBatch() []batchMutations {
	return b.batches[:1]
}

func (b *batched) forgetPrimary() {
	if len(b.batches) == 0 {
		return
	}
	b.batches = b.batches[1:]
}

// batchExecutor is txn controller providing rate control like utils
type batchExecutor struct {
	rateLim           int                  // concurrent worker numbers
	rateLimiter       *util.RateLimit      // rate limiter for concurrency control, maybe more strategies
	committer         *twoPhaseCommitter   // here maybe more different type committer in the future
	action            twoPhaseCommitAction // the work action type
	backoffer         *retry.Backoffer     // Backoffer
	tokenWaitDuration time.Duration        // get token wait time
}

// newBatchExecutor create processor to handle concurrent batch works(prewrite/commit etc)
func newBatchExecutor(rateLimit int, committer *twoPhaseCommitter,
	action twoPhaseCommitAction, backoffer *retry.Backoffer) *batchExecutor {
	return &batchExecutor{rateLimit, nil, committer,
		action, backoffer, 0}
}

// initUtils do initialize batchExecutor related policies like rateLimit util
func (batchExe *batchExecutor) initUtils() error {
	// init rateLimiter by injected rate limit number
	batchExe.rateLimiter = util.NewRateLimit(batchExe.rateLim)
	return nil
}

// startWork concurrently do the work for each batch considering rate limit
func (batchExe *batchExecutor) startWorker(exitCh chan struct{}, ch chan error, batches []batchMutations) {
	for idx, batch1 := range batches {
		waitStart := time.Now()
		if exit := batchExe.rateLimiter.GetToken(exitCh); !exit {
			batchExe.tokenWaitDuration += time.Since(waitStart)
			batch := batch1
			go func() {
				defer batchExe.rateLimiter.PutToken()
				var singleBatchBackoffer *retry.Backoffer
				if _, ok := batchExe.action.(actionCommit); ok {
					// Because the secondary batches of the commit actions are implemented to be
					// committed asynchronously in background goroutines, we should not
					// fork a child context and call cancel() while the foreground goroutine exits.
					// Otherwise the background goroutines will be canceled execeptionally.
					// Here we makes a new clone of the original backoffer for this goroutine
					// exclusively to avoid the data race when using the same backoffer
					// in concurrent goroutines.
					singleBatchBackoffer = batchExe.backoffer.Clone()
				} else {
					var singleBatchCancel context.CancelFunc
					singleBatchBackoffer, singleBatchCancel = batchExe.backoffer.Fork()
					defer singleBatchCancel()
				}
				ch <- batchExe.action.handleSingleBatch(batchExe.committer, singleBatchBackoffer, batch)
				commitDetail := batchExe.committer.getDetail()
				// For prewrite, we record the max backoff time
				if _, ok := batchExe.action.(actionPrewrite); ok {
					commitDetail.Mu.Lock()
					boSleep := int64(singleBatchBackoffer.GetTotalSleep()) * int64(time.Millisecond)
					if boSleep > commitDetail.Mu.CommitBackoffTime {
						commitDetail.Mu.CommitBackoffTime = boSleep
						commitDetail.Mu.PrewriteBackoffTypes = singleBatchBackoffer.GetTypes()
					}
					commitDetail.Mu.Unlock()
				}
				// Backoff time in the 2nd phase of a non-async-commit txn is added
				// in the commitTxn method, so we don't add it here.
			}()
		} else {
			logutil.Logger(batchExe.backoffer.GetCtx()).Info("break startWorker",
				zap.Stringer("action", batchExe.action), zap.Int("batch size", len(batches)),
				zap.Int("index", idx))
			break
		}
	}
}

// process will start worker routine and collect results
func (batchExe *batchExecutor) process(batches []batchMutations) error {
	var err error
	err = batchExe.initUtils()
	if err != nil {
		logutil.Logger(batchExe.backoffer.GetCtx()).Error("batchExecutor initUtils failed", zap.Error(err))
		return err
	}

	// For prewrite, stop sending other requests after receiving first error.
	// However, AssertionFailed error is less prior to other kinds of errors. If we meet an AssertionFailed error,
	// we hold it to see if there's other error, and return it if there are no other kinds of errors.
	// This is because when there are transaction conflicts in pessimistic transaction, it's possible that the
	// non-pessimistic-locked keys may report false-positive assertion failure.
	// See also: https://github.com/tikv/tikv/issues/12113
	var cancel context.CancelFunc
	if _, ok := batchExe.action.(actionPrewrite); ok {
		batchExe.backoffer, cancel = batchExe.backoffer.Fork()
		defer cancel()
	}
	var assertionFailedErr error = nil
	// concurrently do the work for each batch.
	ch := make(chan error, len(batches))
	exitCh := make(chan struct{})
	go batchExe.startWorker(exitCh, ch, batches)
	// check results
	for i := 0; i < len(batches); i++ {
		if e := <-ch; e != nil {
			logutil.Logger(batchExe.backoffer.GetCtx()).Debug("2PC doActionOnBatch failed",
				zap.Uint64("session", batchExe.committer.sessionID),
				zap.Stringer("action type", batchExe.action),
				zap.Error(e),
				zap.Uint64("txnStartTS", batchExe.committer.startTS))
			if _, isAssertionFailed := errors.Cause(e).(*tikverr.ErrAssertionFailed); isAssertionFailed {
				if assertionFailedErr == nil {
					assertionFailedErr = e
				}
			} else {
				// Cancel other requests and return the first error.
				if cancel != nil {
					logutil.Logger(batchExe.backoffer.GetCtx()).Debug("2PC doActionOnBatch to cancel other actions",
						zap.Uint64("session", batchExe.committer.sessionID),
						zap.Stringer("action type", batchExe.action),
						zap.Uint64("txnStartTS", batchExe.committer.startTS))
					atomic.StoreUint32(&batchExe.committer.prewriteCancelled, 1)
					cancel()
				}
				if err == nil {
					err = e
				}
			}
		}
	}
	close(exitCh)
	if batchExe.tokenWaitDuration > 0 {
		metrics.TiKVTokenWaitDuration.Observe(float64(batchExe.tokenWaitDuration.Nanoseconds()))
	}
	if err != nil {
		if assertionFailedErr != nil {
			logutil.Logger(batchExe.backoffer.GetCtx()).Debug("2PC doActionOnBatch met assertion failed error but ignored due to other kinds of error",
				zap.Uint64("session", batchExe.committer.sessionID),
				zap.Stringer("actoin type", batchExe.action),
				zap.Uint64("txnStartTS", batchExe.committer.startTS),
				zap.Uint64("forUpdateTS", batchExe.committer.forUpdateTS),
				zap.NamedError("assertionFailed", assertionFailedErr),
				zap.Error(err))
		}
		return err
	}
	return assertionFailedErr
}

func (c *twoPhaseCommitter) setDetail(d *util.CommitDetails) {
	atomic.StorePointer(&c.detail, unsafe.Pointer(d))
}

func (c *twoPhaseCommitter) getDetail() *util.CommitDetails {
	return (*util.CommitDetails)(atomic.LoadPointer(&c.detail))
}

func (c *twoPhaseCommitter) setUndeterminedErr(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.undeterminedErr = err
}

func (c *twoPhaseCommitter) getUndeterminedErr() error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.mu.undeterminedErr
}

func (c *twoPhaseCommitter) mutationsOfKeys(keys [][]byte) CommitterMutations {
	var res PlainMutations
	for i := 0; i < c.mutations.Len(); i++ {
		for _, key := range keys {
			if bytes.Equal(c.mutations.GetKey(i), key) {
				res.Push(c.mutations.GetOp(i), c.mutations.GetKey(i), c.mutations.GetValue(i), c.mutations.IsPessimisticLock(i),
					c.mutations.IsAssertExists(i), c.mutations.IsAssertNotExist(i), c.mutations.NeedConstraintCheckInPrewrite(i))
				break
			}
		}
	}
	return &res
}
