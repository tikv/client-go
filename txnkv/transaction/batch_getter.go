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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/tests/snapshot_test.go
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
	"context"

	"github.com/tikv/client-go/v2/internal/unionstore"
)

// BatchBufferGetter is the interface for BatchGet.
type BatchBufferGetter interface {
	Len() int
	unionstore.Getter
	BatchGetter
}

// BatchGetter is the interface for BatchGet.
type BatchGetter interface {
	// BatchGet gets a batch of values.
	BatchGet(ctx context.Context, keys [][]byte) (map[string][]byte, error)
}

// BufferBatchGetter is the type for BatchGet with MemBuffer.
type BufferBatchGetter struct {
	buffer   BatchBufferGetter
	snapshot BatchGetter
}

// NewBufferBatchGetter creates a new BufferBatchGetter.
func NewBufferBatchGetter(buffer BatchBufferGetter, snapshot BatchGetter) *BufferBatchGetter {
	return &BufferBatchGetter{buffer: buffer, snapshot: snapshot}
}

// BatchGet gets a batch of values.
func (b *BufferBatchGetter) BatchGet(ctx context.Context, keys [][]byte) (map[string][]byte, error) {
	bufferValues, err := b.buffer.BatchGet(ctx, keys)
	if err != nil {
		return nil, err
	}
	if len(bufferValues) == 0 {
		return b.snapshot.BatchGet(ctx, keys)
	}
	shrinkKeys := make([][]byte, 0, len(keys)-len(bufferValues))
	for _, key := range keys {
		val, ok := bufferValues[string(key)]
		if !ok {
			shrinkKeys = append(shrinkKeys, key)
			continue
		}
		// the deleted key should be removed from the result, and also no need to snapshot read it again.
		if len(val) == 0 {
			delete(bufferValues, string(key))
		}
	}
	storageValues, err := b.snapshot.BatchGet(ctx, shrinkKeys)
	if err != nil {
		return nil, err
	}
	for key, val := range storageValues {
		bufferValues[key] = val
	}
	return bufferValues, nil
}

// BufferSnapshotBatchGetter is the type for BatchGet with MemBuffer.
type BufferSnapshotBatchGetter struct {
	buffer   BatchSnapshotBufferGetter
	snapshot BatchGetter
}

// BatchSnapshotBufferGetter is the interface for BatchGet.
type BatchSnapshotBufferGetter interface {
	unionstore.Getter
	BatchGetter
}

// NewBufferSnapshotBatchGetter creates a new BufferBatchGetter.
func NewBufferSnapshotBatchGetter(buffer BatchSnapshotBufferGetter, snapshot BatchGetter) *BufferSnapshotBatchGetter {
	return &BufferSnapshotBatchGetter{buffer: buffer, snapshot: snapshot}
}

// BatchGet gets a batch of values.
func (b *BufferSnapshotBatchGetter) BatchGet(ctx context.Context, keys [][]byte) (map[string][]byte, error) {
	bufferValues, err := b.buffer.BatchGet(ctx, keys)
	if err != nil {
		return nil, err
	}
	if len(bufferValues) == 0 {
		return b.snapshot.BatchGet(ctx, keys)
	}
	shrinkKeys := make([][]byte, 0, len(keys)-len(bufferValues))
	for _, key := range keys {
		val, ok := bufferValues[string(key)]
		if !ok {
			shrinkKeys = append(shrinkKeys, key)
			continue
		}
		// the deleted key should be removed from the result, and also no need to snapshot read it again.
		if len(val) == 0 {
			delete(bufferValues, string(key))
		}
	}
	storageValues, err := b.snapshot.BatchGet(ctx, shrinkKeys)
	if err != nil {
		return nil, err
	}
	for key, val := range storageValues {
		bufferValues[key] = val
	}
	return bufferValues, nil
}
