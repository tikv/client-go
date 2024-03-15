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
}

const (
	// MinFlushKeys is the minimum number of keys to trigger flush.
	// small batch can lead to poor performance and resource waste in random write workload.
	// 10K batch size is large enough to get good performance with random write workloads in tests.
	MinFlushKeys = 10000
	// MinFlushSize is the minimum size of MemDB to trigger flush.
	MinFlushSize = 16 * 1024 * 1024 // 16MB
	// ForceFlushSizeThreshold is the threshold to force flush MemDB, which controls the max memory consumption of PipelinedMemDB.
	ForceFlushSizeThreshold = 128 * 1024 * 1024 // 128MB
)

type flushOption struct {
	MinFlushKeys            int
	MinFlushSize            int
	ForceFlushSizeThreshold int
}

func newFlushOption() flushOption {
	opt := flushOption{
		MinFlushKeys:            MinFlushKeys,
		MinFlushSize:            MinFlushSize,
		ForceFlushSizeThreshold: ForceFlushSizeThreshold,
	}
	if val, err := util.EvalFailpoint("pipelinedMemDBMinFlushKeys"); err == nil && val != nil {
		opt.MinFlushKeys = val.(int)
	}
	if val, err := util.EvalFailpoint("pipelinedMemDBMinFlushSize"); err == nil && val != nil {
		opt.MinFlushSize = val.(int)
	}
	if val, err := util.EvalFailpoint("pipelinedMemDBForceFlushSizeThreshold"); err == nil && val != nil {
		opt.ForceFlushSizeThreshold = val.(int)
	}
	return opt
}

type pipelinedMemDBSkipRemoteBuffer struct{}

// TODO: skip remote buffer by context is too obscure, add a new method to read local buffer.
var pipelinedMemDBSkipRemoteBufferKey = pipelinedMemDBSkipRemoteBuffer{}

// WithPipelinedMemDBSkipRemoteBuffer is used to skip reading remote buffer for saving RPC.
func WithPipelinedMemDBSkipRemoteBuffer(ctx context.Context) context.Context {
	return context.WithValue(ctx, pipelinedMemDBSkipRemoteBufferKey, struct{}{})
}

type FlushFunc func(uint64, *MemDB) error
type BufferBatchGetter func(ctx context.Context, keys [][]byte) (map[string][]byte, error)

func NewPipelinedMemDB(bufferBatchGetter BufferBatchGetter, flushFunc FlushFunc) *PipelinedMemDB {
	memdb := newMemDB()
	memdb.setSkipMutex(true)
	flushOptoin := newFlushOption()
	return &PipelinedMemDB{
		memDB:             memdb,
		errCh:             make(chan error, 1),
		flushFunc:         flushFunc,
		bufferBatchGetter: bufferBatchGetter,
		generation:        0,
		// keep entryLimit and bufferLimit same with the memdb's default values.
		entryLimit:  memdb.entrySizeLimit,
		bufferLimit: memdb.bufferSizeLimit,
		flushOption: flushOptoin,
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

// Get the value by given key, it returns tikverr.ErrNotExist if not exist.
// The priority of the value is MemBuffer > flushingMemDB > flushed memdbs.
func (p *PipelinedMemDB) Get(ctx context.Context, k []byte) ([]byte, error) {
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
	if ctx.Value(pipelinedMemDBSkipRemoteBufferKey) != nil {
		return nil, tikverr.ErrNotExist
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

func (p *PipelinedMemDB) UpdateFlags(k []byte, ops ...kv.FlagsOp) {
	p.memDB.UpdateFlags(k, ops...)
}

// Set sets the value for key k in the MemBuffer.
func (p *PipelinedMemDB) Set(key, value []byte) error {
	p.Lock()
	defer p.Unlock()
	return p.memDB.Set(key, value)
}

// SetWithFlags sets the value for key k in the MemBuffer with flags.
func (p *PipelinedMemDB) SetWithFlags(key, value []byte, ops ...kv.FlagsOp) error {
	p.Lock()
	defer p.Unlock()
	return p.memDB.SetWithFlags(key, value, ops...)
}

// Delete deletes the key k in the MemBuffer.
func (p *PipelinedMemDB) Delete(key []byte) error {
	p.Lock()
	defer p.Unlock()
	return p.memDB.Delete(key)
}

// DeleteWithFlags deletes the key k in the MemBuffer with flags.
func (p *PipelinedMemDB) DeleteWithFlags(key []byte, ops ...kv.FlagsOp) error {
	p.Lock()
	defer p.Unlock()
	return p.memDB.DeleteWithFlags(key, ops...)
}

// Flush is called during execution of a transaction, it does flush when there are enough keys and the ongoing flushingMemDB is done.
// The first returned value indicates whether the flush is triggered.
// The second returned value is the error if there is a failure, txn should abort when there is an error.
// When the mutable memdb is too large, it blocks until the ongoing flush is done.
func (p *PipelinedMemDB) Flush(force bool) (bool, error) {
	if p.flushFunc == nil {
		return false, errors.New("flushFunc is not provided")
	}

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
	return true, nil
}

func (p *PipelinedMemDB) needFlush() bool {
	size := p.memDB.Size()
	// size < MinFlushSize, do not flush.
	// MinFlushSize <= size < ForceFlushSizeThreshold && keys < MinFlushKeys, do not flush.
	// MinFlushSize <= size < ForceFlushSizeThreshold && keys >= MinFlushKeys, flush.
	// size >= ForceFlushSizeThreshold, flush.
	if size < p.flushOption.MinFlushSize || (p.memDB.Len() < p.flushOption.MinFlushKeys && size < p.flushOption.ForceFlushSizeThreshold) {
		return false
	}
	if p.onFlushing.Load() && size < p.flushOption.ForceFlushSizeThreshold {
		return false
	}
	return true
}

// FlushWait will wait for all flushing tasks are done and return the error if there is a failure.
func (p *PipelinedMemDB) FlushWait() error {
	if p.flushingMemDB != nil {
		err := <-p.errCh
		if err != nil {
			err = p.handleAlreadyExistErr(err)
		}
		// cleanup the flushingMemDB so the next call of FlushWait will not wait for the error channel.
		p.flushingMemDB = nil
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
	return p.memDB.Size() + p.size
}

func (p *PipelinedMemDB) OnFlushing() bool {
	return p.onFlushing.Load()
}

// SetMemoryFootprintChangeHook sets the hook for memory footprint change.
// TODO: implement this.
func (p *PipelinedMemDB) SetMemoryFootprintChangeHook(hook func(uint64)) {}

// Mem returns the memory usage of MemBuffer.
// TODO: implement this.
func (p *PipelinedMemDB) Mem() uint64 {
	return 0
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
