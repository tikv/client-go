// Copyright 2024 TiKV Authors
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

package unionstore

import (
	"context"
	stderrors "errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

// PipelinedMemDB is a Store which contains
//   - a mutable buffer for read and write
//   - an immutable onflushing buffer for read
//   - like MemDB, PipelinedMemDB also CANNOT be used concurrently
type PipelinedMemDB struct {
	// Like MemDB, this RWMutex only used to ensure memdbSnapGetter.Get will not race with
	// concurrent memdb.Set, memdb.SetWithFlags, memdb.Delete and memdb.UpdateFlags.
	sync.RWMutex
	onFlushing              atomic.Bool
	errCh                   chan error
	flushFunc               FlushFunc
	bufferBatchGetter       BufferBatchGetter
	memDB                   *MemDB
	flushingMemDB           *MemDB // the flushingMemDB is not wrapped by a mutex, because there is no data race in it.
	len, size               int    // len and size records the total flushed and onflushing memdb.
	generation              uint64
	entryLimit, bufferLimit uint64
	flushOption             flushOption
	// prefetchCache is used to cache the result of BatchGet, it's invalidated when Flush.
	// the values are wrapped by util.Option.
	//   None -> not found
	//   Some([...]) -> put
	//   Some([]) -> delete
	batchGetCache map[string]util.Option[[]byte]
	memChangeHook func(uint64)

	// metrics
	flushWaitDuration time.Duration
}

const (
	// MinFlushKeys is the minimum number of keys to trigger flush.
	// small batch can lead to poor performance and resource waste in random write workload.
	// 10K batch size is large enough to get good performance with random write workloads in tests.
	MinFlushKeys = 10000
	// MinFlushMemSize is the minimum size of MemDB to trigger flush.
	MinFlushMemSize uint64 = 16 * 1024 * 1024 // 16MB
	// ForceFlushMemSizeThreshold is the threshold to force flush MemDB, which controls the max memory consumption of PipelinedMemDB.
	ForceFlushMemSizeThreshold uint64 = 128 * 1024 * 1024 // 128MB
)

type flushOption struct {
	MinFlushKeys               uint64
	MinFlushMemSize            uint64
	ForceFlushMemSizeThreshold uint64
}

func newFlushOption() flushOption {
	opt := flushOption{
		MinFlushKeys:               MinFlushKeys,
		MinFlushMemSize:            MinFlushMemSize,
		ForceFlushMemSizeThreshold: ForceFlushMemSizeThreshold,
	}
	if val, err := util.EvalFailpoint("pipelinedMemDBMinFlushKeys"); err == nil && val != nil {
		opt.MinFlushKeys = uint64(val.(int))
	}
	if val, err := util.EvalFailpoint("pipelinedMemDBMinFlushSize"); err == nil && val != nil {
		opt.MinFlushMemSize = uint64(val.(int))
	}
	if val, err := util.EvalFailpoint("pipelinedMemDBForceFlushSizeThreshold"); err == nil && val != nil {
		opt.ForceFlushMemSizeThreshold = uint64(val.(int))
	}
	return opt
}

type FlushFunc func(uint64, *MemDB) error
type BufferBatchGetter func(ctx context.Context, keys [][]byte) (map[string][]byte, error)

func NewPipelinedMemDB(bufferBatchGetter BufferBatchGetter, flushFunc FlushFunc) *PipelinedMemDB {
	memdb := newMemDB()
	memdb.setSkipMutex(true)
	flushOpt := newFlushOption()
	return &PipelinedMemDB{
		memDB:             memdb,
		errCh:             make(chan error, 1),
		flushFunc:         flushFunc,
		bufferBatchGetter: bufferBatchGetter,
		generation:        0,
		// keep entryLimit and bufferLimit same with the memdb's default values.
		entryLimit:  memdb.entrySizeLimit,
		bufferLimit: memdb.bufferSizeLimit,
		flushOption: flushOpt,
	}
}

// Dirty returns whether the pipelined buffer is mutated.
func (p *PipelinedMemDB) Dirty() bool {
	return p.memDB.Dirty() || p.len > 0
}

// GetMemDB implements MemBuffer interface.
func (p *PipelinedMemDB) GetMemDB() *MemDB {
	panic("GetMemDB should not be invoked for PipelinedMemDB")
}

func (p *PipelinedMemDB) get(ctx context.Context, k []byte, skipRemoteBuffer bool) ([]byte, error) {
	v, err := p.memDB.Get(k)
	if err == nil {
		return v, nil
	}
	if !tikverr.IsErrNotFound(err) {
		return nil, err
	}
	if p.flushingMemDB != nil {
		v, err = p.flushingMemDB.Get(k)
		if err == nil {
			return v, nil
		}
		if !tikverr.IsErrNotFound(err) {
			return nil, err
		}
	}
	if skipRemoteBuffer {
		return nil, tikverr.ErrNotExist
	}
	if p.batchGetCache != nil {
		v, ok := p.batchGetCache[string(k)]
		if ok {
			inner := v.Inner()
			if inner == nil {
				return nil, tikverr.ErrNotExist
			}
			return *inner, nil
		}
	}
	// read remote buffer
	var (
		dataMap map[string][]byte
		ok      bool
	)
	dataMap, err = p.bufferBatchGetter(ctx, [][]byte{k})
	if err != nil {
		return nil, err
	}
	v, ok = dataMap[string(k)]
	if !ok {
		return nil, tikverr.ErrNotExist
	}
	return v, nil
}

// Get the value by given key, it returns tikverr.ErrNotExist if not exist.
// The priority of the value is MemBuffer > flushingMemDB > flushed memdbs.
func (p *PipelinedMemDB) Get(ctx context.Context, k []byte) ([]byte, error) {
	return p.get(ctx, k, false)
}

// GetLocal implements the MemBuffer interface.
// It only checks the mutable memdb and the immutable memdb.
// It does not check mutations that have been flushed to TiKV.
func (p *PipelinedMemDB) GetLocal(ctx context.Context, key []byte) ([]byte, error) {
	return p.get(ctx, key, true)
}

func (p *PipelinedMemDB) GetFlags(k []byte) (kv.KeyFlags, error) {
	f, err := p.memDB.GetFlags(k)
	if p.flushingMemDB != nil && tikverr.IsErrNotFound(err) {
		f, err = p.flushingMemDB.GetFlags(k)
	}
	if err != nil {
		return 0, err
	}
	return f, nil
}

func (p *PipelinedMemDB) BatchGet(ctx context.Context, keys [][]byte) (map[string][]byte, error) {
	m := make(map[string][]byte, len(keys))
	if p.batchGetCache == nil {
		p.batchGetCache = make(map[string]util.Option[[]byte], len(keys))
	}
	shrinkKeys := make([][]byte, 0, len(keys))
	for _, k := range keys {
		v, err := p.GetLocal(ctx, k)
		if err != nil {
			if tikverr.IsErrNotFound(err) {
				shrinkKeys = append(shrinkKeys, k)
				continue
			}
			return nil, err
		}
		m[string(k)] = v
		p.batchGetCache[string(k)] = util.Some(v)
	}
	storageValues, err := p.bufferBatchGetter(ctx, shrinkKeys)
	if err != nil {
		return nil, err
	}
	for _, k := range shrinkKeys {
		v, ok := storageValues[string(k)]
		if ok {
			// the protobuf cast empty byte slice to nil, we need to cast it back when receiving values from storage.
			if v == nil {
				v = []byte{}
			}
			m[string(k)] = v
			p.batchGetCache[string(k)] = util.Some(v)
		} else {
			p.batchGetCache[string(k)] = util.None[[]byte]()
		}
	}
	return m, nil
}

func (p *PipelinedMemDB) UpdateFlags(k []byte, ops ...kv.FlagsOp) {
	p.memDB.UpdateFlags(k, ops...)
}

// Set sets the value for key k in the MemBuffer.
func (p *PipelinedMemDB) Set(key, value []byte) error {
	p.Lock()
	defer p.Unlock()
	err := p.memDB.Set(key, value)
	p.onMemChange()
	return err
}

// SetWithFlags sets the value for key k in the MemBuffer with flags.
func (p *PipelinedMemDB) SetWithFlags(key, value []byte, ops ...kv.FlagsOp) error {
	p.Lock()
	defer p.Unlock()
	err := p.memDB.SetWithFlags(key, value, ops...)
	p.onMemChange()
	return err
}

// Delete deletes the key k in the MemBuffer.
func (p *PipelinedMemDB) Delete(key []byte) error {
	p.Lock()
	defer p.Unlock()
	err := p.memDB.Delete(key)
	p.onMemChange()
	return err
}

// DeleteWithFlags deletes the key k in the MemBuffer with flags.
func (p *PipelinedMemDB) DeleteWithFlags(key []byte, ops ...kv.FlagsOp) error {
	p.Lock()
	defer p.Unlock()
	err := p.memDB.DeleteWithFlags(key, ops...)
	p.onMemChange()
	return err
}

// Flush is called during execution of a transaction, it does flush when there are enough keys and the ongoing flushingMemDB is done.
// The first returned value indicates whether the flush is triggered.
// The second returned value is the error if there is a failure, txn should abort when there is an error.
// When the mutable memdb is too large, it blocks until the ongoing flush is done.
func (p *PipelinedMemDB) Flush(force bool) (bool, error) {
	if p.flushFunc == nil {
		return false, errors.New("flushFunc is not provided")
	}

	// invalidate the batch get cache whether the flush is really triggered.
	p.batchGetCache = nil

	if len(p.memDB.stages) > 0 {
		return false, errors.New("there are stages unreleased when Flush is called")
	}

	if !force && !p.needFlush() {
		return false, nil
	}
	if p.flushingMemDB != nil {
		if err := <-p.errCh; err != nil {
			if err != nil {
				err = p.handleAlreadyExistErr(err)
			}
			p.flushingMemDB = nil
			return false, err
		}
	}
	p.onFlushing.Store(true)
	p.flushingMemDB = p.memDB
	p.len += p.flushingMemDB.Len()
	p.size += p.flushingMemDB.Size()
	p.memDB = newMemDB()
	p.memDB.SetEntrySizeLimit(p.entryLimit, p.bufferLimit)
	p.memDB.setSkipMutex(true)
	p.generation++
	go func(generation uint64) {
		util.EvalFailpoint("beforePipelinedFlush")
		metrics.TiKVPipelinedFlushLenHistogram.Observe(float64(p.flushingMemDB.Len()))
		metrics.TiKVPipelinedFlushSizeHistogram.Observe(float64(p.flushingMemDB.Size()))
		flushStart := time.Now()
		err := p.flushFunc(generation, p.flushingMemDB)
		metrics.TiKVPipelinedFlushDuration.Observe(time.Since(flushStart).Seconds())
		p.onFlushing.Store(false)
		// Send the error to errCh after onFlushing status is set to false.
		// this guarantees the onFlushing.Store(true) in another goroutine's Flush happens after onFlushing.Store(false) in this function.
		p.errCh <- err
	}(p.generation)
	p.onMemChange()
	return true, nil
}

func (p *PipelinedMemDB) needFlush() bool {
	size := p.memDB.Mem()
	// mem size < MinFlushMemSize, do not flush.
	// MinFlushMemSize <= mem size < ForceFlushMemSizeThreshold && keys < MinFlushKeys, do not flush.
	// MinFlushMemSize <= mem size < ForceFlushMemSizeThreshold && keys >= MinFlushKeys, flush.
	// mem size >= ForceFlushMemSizeThreshold, flush.

	/*
	              Keys
	               ^
	               |       |
	               |       |
	               |       |
	               |       |    Flush
	               |       |
	   MinKey(10k) |       +------------+
	               |       |            |
	               |  No   |  No Flush  |   Flush
	               | Flush |            |
	               +-----------------------------------------> Size
	               0   MinSize(16MB)   Force(128MB)
	*/
	if size < p.flushOption.MinFlushMemSize ||
		(uint64(p.memDB.Len()) < p.flushOption.MinFlushKeys &&
			size < p.flushOption.ForceFlushMemSizeThreshold) {
		return false
	}
	if p.onFlushing.Load() && size < p.flushOption.ForceFlushMemSizeThreshold {
		return false
	}
	return true
}

// FlushWait will wait for all flushing tasks are done and return the error if there is a failure.
func (p *PipelinedMemDB) FlushWait() error {
	if p.flushingMemDB != nil {
		now := time.Now()
		err := <-p.errCh
		if err != nil {
			err = p.handleAlreadyExistErr(err)
		}
		// cleanup the flushingMemDB so the next call of FlushWait will not wait for the error channel.
		p.flushingMemDB = nil
		p.flushWaitDuration += time.Since(now)
		return err
	}
	return nil
}

func (p *PipelinedMemDB) handleAlreadyExistErr(err error) error {
	var existErr *tikverr.ErrKeyExist
	if stderrors.As(err, &existErr) {
		v, err2 := p.flushingMemDB.Get(existErr.GetKey())
		if err2 != nil {
			// TODO: log more info like start_ts, also for other logs
			logutil.BgLogger().Warn(
				"[pipelined-dml] Getting value from flushingMemDB when"+
					" AlreadyExist error occurs failed", zap.Error(err2),
				zap.Uint64("generation", p.generation),
			)
		} else {
			existErr.Value = v
		}
		return existErr
	}
	return err
}

// Iter implements the Retriever interface.
func (p *PipelinedMemDB) Iter([]byte, []byte) (Iterator, error) {
	return nil, errors.New("pipelined memdb does not support Iter")
}

// IterReverse implements the Retriever interface.
func (p *PipelinedMemDB) IterReverse([]byte, []byte) (Iterator, error) {
	return nil, errors.New("pipelined memdb does not support IterReverse")
}

// SetEntrySizeLimit sets the size limit for each entry and total buffer.
func (p *PipelinedMemDB) SetEntrySizeLimit(entryLimit, bufferLimit uint64) {
	p.entryLimit, p.bufferLimit = entryLimit, bufferLimit
	p.memDB.SetEntrySizeLimit(entryLimit, bufferLimit)
}

func (p *PipelinedMemDB) Len() int {
	return p.memDB.Len() + p.len
}

func (p *PipelinedMemDB) Size() int {
	size := p.size
	if p.memDB != nil {
		size += p.memDB.Size()
	}
	return size
}

func (p *PipelinedMemDB) OnFlushing() bool {
	return p.onFlushing.Load()
}

// SetMemoryFootprintChangeHook sets the hook for memory footprint change.
func (p *PipelinedMemDB) SetMemoryFootprintChangeHook(hook func(uint64)) {
	p.memChangeHook = hook
}

func (p *PipelinedMemDB) onMemChange() {
	if p.memChangeHook != nil {
		p.memChangeHook(p.Mem())
	}
}

// Mem returns the memory usage of MemBuffer.
func (p *PipelinedMemDB) Mem() uint64 {
	var mem uint64
	if p.memDB != nil {
		mem += p.memDB.Mem()
	}
	if p.flushingMemDB != nil {
		mem += p.flushingMemDB.Mem()
	}
	return mem
}

type errIterator struct {
	err error
}

func (e *errIterator) Valid() bool   { return true }
func (e *errIterator) Next() error   { return e.err }
func (e *errIterator) Key() []byte   { return nil }
func (e *errIterator) Value() []byte { return nil }
func (e *errIterator) Close()        {}

// SnapshotIter implements MemBuffer interface, returns an iterator which outputs error.
func (p *PipelinedMemDB) SnapshotIter(k, upperBound []byte) Iterator {
	return &errIterator{err: errors.New("SnapshotIter is not supported for PipelinedMemDB")}
}

// SnapshotIterReverse implements MemBuffer interface, returns an iterator which outputs error.
func (p *PipelinedMemDB) SnapshotIterReverse(k, lowerBound []byte) Iterator {
	return &errIterator{err: errors.New("SnapshotIter is not supported for PipelinedMemDB")}
}

// The following methods are not implemented for PipelinedMemDB and DOES NOT return error because of the interface limitation.
// It panics when the following methods are called, the application should not use those methods when PipelinedMemDB is enabled.

// RemoveFromBuffer implements MemBuffer interface.
func (p *PipelinedMemDB) RemoveFromBuffer(key []byte) {
	panic("RemoveFromBuffer is not supported for PipelinedMemDB")
}

// InspectStage implements MemBuffer interface.
func (p *PipelinedMemDB) InspectStage(int, func([]byte, kv.KeyFlags, []byte)) {
	panic("InspectStage is not supported for PipelinedMemDB")
}

// SnapshotGetter implements MemBuffer interface.
func (p *PipelinedMemDB) SnapshotGetter() Getter {
	panic("SnapshotGetter is not supported for PipelinedMemDB")
}

// NOTE about the Staging()/Cleanup()/Release() methods:
// Its correctness is guaranteed by that no stage can exist when a Flush() is called.
// We guarantee in TiDB side that every stage lives inside a flush batch,
// which means the modifications all goes to the mutable memdb.
// Then the staging of the whole PipelinedMemDB can be directly implemented by its mutable memdb.
//
// Checkpoint()/RevertToCheckpoint() is not supported for PipelinedMemDB.

// Staging implements MemBuffer interface.
func (p *PipelinedMemDB) Staging() int {
	return p.memDB.Staging()
}

// Cleanup implements MemBuffer interface.
func (p *PipelinedMemDB) Cleanup(h int) {
	p.memDB.Cleanup(h)
}

// Release implements MemBuffer interface.
func (p *PipelinedMemDB) Release(h int) {
	p.memDB.Release(h)
}

// Checkpoint implements MemBuffer interface.
func (p *PipelinedMemDB) Checkpoint() *MemDBCheckpoint {
	panic("Checkpoint is not supported for PipelinedMemDB")
}

// RevertToCheckpoint implements MemBuffer interface.
func (p *PipelinedMemDB) RevertToCheckpoint(*MemDBCheckpoint) {
	panic("RevertToCheckpoint is not supported for PipelinedMemDB")
}

// GetFlushMetrics implements MemBuffer interface.
func (p *PipelinedMemDB) GetFlushMetrics() FlushMetrics {
	return FlushMetrics{
		WaitDuration: p.flushWaitDuration,
	}
}

// MemHookSet implements the MemBuffer interface.
func (p *PipelinedMemDB) MemHookSet() bool {
	return p.memChangeHook != nil
}
