package unionstore

import (
	"context"

	tikverr "github.com/tikv/client-go/v2/error"
	art "github.com/tikv/client-go/v2/internal/unionstore/art"
)

var _ MemBuffer = &ArenaArt{}

type ArenaArt struct {
	*art.Art
}

func NewArenaArt() *ArenaArt {
	return &ArenaArt{
		Art: art.New(),
	}
}

func (a *ArenaArt) setSkipMutex(skip bool) {
	a.Art.SetSkipMutex(skip)
}

func (a *ArenaArt) Get(_ context.Context, k []byte) ([]byte, error) {
	return a.Art.Get(k)
}

// GetLocal gets the value from the buffer in local memory.
// It makes nonsense for MemDB, but makes a difference for pipelined DML.
func (a *ArenaArt) GetLocal(_ context.Context, key []byte) ([]byte, error) {
	return a.Art.Get(key)
}

// BatchGet gets the values for given keys from the MemBuffer and cache the result if there are remote buffer.
func (a *ArenaArt) BatchGet(_ context.Context, keys [][]byte) (map[string][]byte, error) {
	if !a.Art.Dirty() {
		return map[string][]byte{}, nil
	}
	m := make(map[string][]byte, len(keys))
	for _, k := range keys {
		v, err := a.Art.Get(k)
		if err != nil {
			if tikverr.IsErrNotFound(err) {
				continue
			}
			return nil, err
		}
		m[string(k)] = v
	}
	return m, nil
}

func (a *ArenaArt) RemoveFromBuffer(key []byte) {}

// Iter implements the Retriever interface.
func (a *ArenaArt) Iter(start []byte, end []byte) (Iterator, error) {
	it, err := a.Art.Iter(start, end)
	if err != nil {
		return nil, err
	}
	return it, err
}

// IterReverse implements the Retriever interface.
func (a *ArenaArt) IterReverse(end, start []byte) (Iterator, error) {
	it, err := a.Art.IterReverse(end, start)
	if err != nil {
		return nil, err
	}
	return it, err
}

// SnapshotIter returns an Iterator for a snapshot of MemBuffer.
func (a *ArenaArt) SnapshotIter(lower, upper []byte) Iterator {
	return a.Art.SnapshotIter(lower, upper)
}

// SnapshotIterReverse returns a reversed Iterator for a snapshot of MemBuffer.
func (a *ArenaArt) SnapshotIterReverse(upper, lower []byte) Iterator {
	return a.Art.SnapshotIterReverse(upper, lower)
}

func (a *ArenaArt) SnapshotGetter() Getter {
	return a.Art.SnapshotGetter()
}

func (a *ArenaArt) Dirty() bool {
	return a.Art.Dirty()
}

// Checkpoint returns the checkpoint of the MemBuffer.
func (a *ArenaArt) Checkpoint() *MemDBCheckpoint {
	blockSize, blocks, offsetInBlock := a.Art.Checkpoint()
	return &MemDBCheckpoint{
		blockSize:     blockSize,
		blocks:        blocks,
		offsetInBlock: offsetInBlock,
	}
}

// RevertToCheckpoint reverts the MemBuffer to the specified checkpoint.
func (a *ArenaArt) RevertToCheckpoint(cp *MemDBCheckpoint) {
	a.Art.RevertToCheckpoint(cp.blockSize, cp.blocks, cp.offsetInBlock)
}

// GetMemDB returns the MemDB binding to this MemBuffer.
// This method can also be used for bypassing the wrapper of MemDB.
func (a *ArenaArt) GetMemDB() *MemDB {
	return nil
}

// Flush flushes the pipelined memdb when the keys or sizes reach the threshold.
// If force is true, it will flush the memdb without size limitation.
// it returns true when the memdb is flushed, and returns error when there are any failures.
func (a *ArenaArt) Flush(force bool) (bool, error) {
	return false, nil
}

// FlushWait waits for the flushing task done and return error.
func (a *ArenaArt) FlushWait() error {
	return nil
}

// GetFlushMetrics returns the metrics related to flushing
func (a *ArenaArt) GetFlushMetrics() FlushMetrics {
	return FlushMetrics{}
}

func (a *ArenaArt) stages() []art.ARTCheckpoint {
	return a.Art.Stages()
}
