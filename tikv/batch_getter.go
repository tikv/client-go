package tikv

import (
	"context"

	"github.com/pingcap/errors"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/unionstore"
)

// BufferBatchGetter is the type for BatchGet with MemBuffer.
type BufferBatchGetter struct {
	buffer   *unionstore.MemDB
	snapshot *KVSnapshot
}

// NewBufferBatchGetter creates a new BufferBatchGetter.
func NewBufferBatchGetter(buffer *unionstore.MemDB, snapshot *KVSnapshot) *BufferBatchGetter {
	return &BufferBatchGetter{buffer: buffer, snapshot: snapshot}
}

func (b *BufferBatchGetter) BatchGet(ctx context.Context, keys [][]byte) (map[string][]byte, error) {
	if b.buffer.Len() == 0 {
		return b.snapshot.BatchGet(ctx, keys)
	}
	bufferValues := make([][]byte, len(keys))
	shrinkKeys := make([][]byte, 0, len(keys))
	for i, key := range keys {
		val, err := b.buffer.Get(key)
		if err == nil {
			bufferValues[i] = val
			continue
		}
		if !tikverr.IsErrNotFound(err) {
			return nil, errors.Trace(err)
		}
		shrinkKeys = append(shrinkKeys, key)
	}
	storageValues, err := b.snapshot.BatchGet(ctx, shrinkKeys)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for i, key := range keys {
		if len(bufferValues[i]) == 0 {
			continue
		}
		storageValues[string(key)] = bufferValues[i]
	}
	return storageValues, nil
}
