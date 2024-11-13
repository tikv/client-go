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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/unionstore/memdb_bench_test.go
//

// Copyright 2020 PingCAP, Inc.
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
	"encoding/binary"
	"math/rand"
	"slices"
	"testing"
)

const (
	keySize   = 16
	valueSize = 128
)

func BenchmarkLargeIndex(b *testing.B) {
	fn := func(b *testing.B, p MemBuffer) {
		buf := make([][valueSize]byte, b.N)
		for i := range buf {
			binary.LittleEndian.PutUint32(buf[i][:], uint32(i))
		}
		b.ResetTimer()

		for i := range buf {
			p.Set(buf[i][:], buf[i][:])
		}
	}

	b.Run("RBT", func(b *testing.B) { fn(b, newRbtDBWithContext()) })
	b.Run("ART", func(b *testing.B) { fn(b, newArtDBWithContext()) })
}

func BenchmarkPut(b *testing.B) {
	fn := func(b *testing.B, p MemBuffer) {
		buf := make([][valueSize]byte, b.N)
		for i := range buf {
			binary.BigEndian.PutUint32(buf[i][:], uint32(i))
		}
		b.ResetTimer()
		for i := range buf {
			p.Set(buf[i][:keySize], buf[i][:])
		}
	}

	b.Run("RBT", func(b *testing.B) { fn(b, newRbtDBWithContext()) })
	b.Run("ART", func(b *testing.B) { fn(b, newArtDBWithContext()) })
}

func BenchmarkPutRandom(b *testing.B) {
	fn := func(b *testing.B, p MemBuffer) {
		buf := make([][valueSize]byte, b.N)
		for i := range buf {
			binary.LittleEndian.PutUint32(buf[i][:], uint32(rand.Int()))
		}
		b.ResetTimer()
		for i := range buf {
			p.Set(buf[i][:keySize], buf[i][:])
		}
	}

	b.Run("RBT", func(b *testing.B) { fn(b, newRbtDBWithContext()) })
	b.Run("ART", func(b *testing.B) { fn(b, newArtDBWithContext()) })
}

func BenchmarkGet(b *testing.B) {
	fn := func(b *testing.B, p MemBuffer) {
		buf := make([][valueSize]byte, b.N)
		for i := range buf {
			binary.BigEndian.PutUint32(buf[i][:], uint32(i))
		}

		for i := range buf {
			p.Set(buf[i][:keySize], buf[i][:])
		}

		ctx := context.Background()
		b.ResetTimer()
		for i := range buf {
			p.Get(ctx, buf[i][:keySize])
		}
	}

	b.Run("RBT", func(b *testing.B) { fn(b, newRbtDBWithContext()) })
	b.Run("ART", func(b *testing.B) { fn(b, newArtDBWithContext()) })
}

func BenchmarkGetRandom(b *testing.B) {
	fn := func(b *testing.B, p MemBuffer) {
		buf := make([][valueSize]byte, b.N)
		for i := range buf {
			binary.LittleEndian.PutUint32(buf[i][:], uint32(rand.Int()))
		}

		for i := range buf {
			p.Set(buf[i][:keySize], buf[i][:])
		}

		ctx := context.Background()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			p.Get(ctx, buf[i][:keySize])
		}
	}

	b.Run("RBT", func(b *testing.B) { fn(b, newRbtDBWithContext()) })
	b.Run("ART", func(b *testing.B) { fn(b, newArtDBWithContext()) })
}

var opCnt = 100000

func BenchmarkMemDbBufferSequential(b *testing.B) {
	fn := func(b *testing.B, buffer MemBuffer) {
		data := make([][]byte, opCnt)
		for i := 0; i < opCnt; i++ {
			data[i] = encodeInt(i)
		}
		benchmarkSetGet(b, buffer, data)
		b.ReportAllocs()
	}

	b.Run("RBT", func(b *testing.B) { fn(b, newRbtDBWithContext()) })
	b.Run("ART", func(b *testing.B) { fn(b, newArtDBWithContext()) })
}

func BenchmarkMemDbBufferRandom(b *testing.B) {
	fn := func(b *testing.B, buffer MemBuffer) {
		data := make([][]byte, opCnt)
		for i := 0; i < opCnt; i++ {
			data[i] = encodeInt(i)
		}
		shuffle(data)
		benchmarkSetGet(b, buffer, data)
		b.ReportAllocs()
	}

	b.Run("RBT", func(b *testing.B) { fn(b, newRbtDBWithContext()) })
	b.Run("ART", func(b *testing.B) { fn(b, newArtDBWithContext()) })
}

func BenchmarkMemDbIter(b *testing.B) {
	fn := func(b *testing.B, buffer MemBuffer) {
		benchIterator(b, buffer)
		b.ReportAllocs()
	}

	b.Run("RBT", func(b *testing.B) { fn(b, newRbtDBWithContext()) })
	b.Run("ART", func(b *testing.B) { fn(b, newArtDBWithContext()) })
}

func BenchmarkMemDbCreation(b *testing.B) {
	fn := func(b *testing.B, createFn func() MemBuffer) {
		for i := 0; i < b.N; i++ {
			createFn()
		}
		b.ReportAllocs()
	}

	b.Run("RBT", func(b *testing.B) { fn(b, func() MemBuffer { return newRbtDBWithContext() }) })
	b.Run("ART", func(b *testing.B) { fn(b, func() MemBuffer { return newArtDBWithContext() }) })
}

func shuffle(slc [][]byte) {
	N := len(slc)
	for i := 0; i < N; i++ {
		// choose index uniformly in [i, N-1]
		r := i + rand.Intn(N-i)
		slc[r], slc[i] = slc[i], slc[r]
	}
}
func benchmarkSetGet(b *testing.B, buffer MemBuffer, data [][]byte) {
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, k := range data {
			buffer.Set(k, k)
		}
		for _, k := range data {
			buffer.Get(ctx, k)
		}
	}
}

func benchIterator(b *testing.B, buffer MemBuffer) {
	for k := 0; k < opCnt; k++ {
		buffer.Set(encodeInt(k), encodeInt(k))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter, err := buffer.Iter(nil, nil)
		if err != nil {
			b.Error(err)
		}
		for iter.Valid() {
			iter.Next()
		}
		iter.Close()
	}
}

func BenchmarkMemBufferCache(b *testing.B) {
	fn := func(b *testing.B, buffer MemBuffer) {
		buf := make([][keySize]byte, b.N)
		for i := range buf {
			binary.LittleEndian.PutUint32(buf[i][:], uint32(i))
			buffer.Set(buf[i][:], buf[i][:])
		}
		ctx := context.Background()
		b.ResetTimer()
		for i := range buf {
			buffer.Get(ctx, buf[i][:])
			for j := 0; j < 10; j++ {
				// the cache hit get will be fast
				buffer.Get(ctx, buf[i][:])
			}
		}
	}
	b.Run("RBT", func(b *testing.B) { fn(b, newRbtDBWithContext()) })
	b.Run("ART", func(b *testing.B) { fn(b, newArtDBWithContext()) })
}

func BenchmarkMemBufferSetGetLongKey(b *testing.B) {
	fn := func(b *testing.B, buffer MemBuffer) {
		keys := make([][]byte, b.N)
		for i := 0; i < b.N; i++ {
			keys[i] = make([]byte, 1024)
			binary.BigEndian.PutUint64(keys[i], uint64(i))
			slices.Reverse(keys[i])
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			buffer.Set(keys[i], keys[i])
		}
		for i := 0; i < b.N; i++ {
			buffer.Get(context.Background(), keys[i])
		}
	}
	b.Run("RBT", func(b *testing.B) { fn(b, newRbtDBWithContext()) })
	b.Run("ART", func(b *testing.B) { fn(b, newArtDBWithContext()) })
}
