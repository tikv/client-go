package unionstore

import (
	"bytes"
	"context"
	"fmt"
	"github.com/tikv/client-go/v2/internal/unionstore/art"
	"github.com/tikv/client-go/v2/kv"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func randNumKey(rd rand.Source) []byte {
	return []byte(fmt.Sprintf("%010d", rd.Int63()))
}

func randTiDBKey(rd rand.Source) []byte {
	keys := [][]byte{
		{116, 128, 0, 0, 0, 0, 0, 0, 4, 95, 114, 128, 0, 0, 0, 0, 0, 0, 1},                                                                                                                                                                                                                                                         // row
		{116, 128, 0, 0, 0, 0, 0, 0, 4, 95, 105, 128, 0, 0, 0, 0, 0, 0, 1, 1, 37, 0, 0, 0, 0, 0, 0, 0, 248, 1, 114, 111, 111, 116, 0, 0, 0, 0, 251},                                                                                                                                                                                // index
		{116, 128, 0, 0, 0, 0, 0, 0, 16, 95, 105, 128, 0, 0, 0, 0, 0, 0, 1, 1, 97, 117, 116, 111, 95, 105, 110, 99, 255, 114, 101, 109, 101, 110, 116, 95, 111, 255, 102, 102, 115, 101, 116, 0, 0, 0, 252},                                                                                                                        // index
		{116, 128, 0, 0, 0, 0, 0, 0, 16, 95, 105, 128, 0, 0, 0, 0, 0, 0, 1, 1, 97, 117, 116, 104, 101, 110, 116, 105, 255, 99, 97, 116, 105, 111, 110, 95, 108, 255, 100, 97, 112, 95, 115, 97, 115, 108, 255, 95, 116, 108, 115, 0, 0, 0, 0, 251},                                                                                 // index
		{116, 128, 0, 0, 0, 0, 0, 0, 16, 95, 105, 128, 0, 0, 0, 0, 0, 0, 1, 1, 97, 117, 116, 104, 101, 110, 116, 105, 255, 99, 97, 116, 105, 111, 110, 95, 108, 255, 100, 97, 112, 95, 115, 97, 115, 108, 255, 95, 99, 97, 95, 112, 97, 116, 104, 255, 0, 0, 0, 0, 0, 0, 0, 0, 247},                                                // index
		{116, 128, 0, 0, 0, 0, 0, 0, 16, 95, 105, 128, 0, 0, 0, 0, 0, 0, 1, 1, 97, 108, 108, 111, 119, 95, 97, 117, 255, 116, 111, 95, 114, 97, 110, 100, 111, 255, 109, 95, 101, 120, 112, 108, 105, 99, 255, 105, 116, 95, 105, 110, 115, 101, 114, 255, 116, 0, 0, 0, 0, 0, 0, 0, 248},                                          // index
		{116, 128, 0, 0, 0, 0, 0, 0, 16, 95, 105, 128, 0, 0, 0, 0, 0, 0, 1, 1, 97, 117, 116, 104, 101, 110, 116, 105, 255, 99, 97, 116, 105, 111, 110, 95, 108, 255, 100, 97, 112, 95, 115, 97, 115, 108, 255, 95, 97, 117, 116, 104, 95, 109, 101, 255, 116, 104, 111, 100, 95, 110, 97, 109, 255, 101, 0, 0, 0, 0, 0, 0, 0, 248}, // index
	}
	key := keys[int(rd.Int63())%len(keys)]
	cpKey := make([]byte, len(key))
	copy(cpKey, key)
	randomBits := rd.Int63()%3 + 1
	for i := 0; i < int(randomBits); i++ {
		cpKey[len(cpKey)-i-1] = byte(rd.Int63() % 256)
	}
	return cpKey
}

func testRandomComplex(t *testing.T, seed int64, randKeyFn func(source rand.Source) []byte) {
	rd := rand.NewSource(seed)
	memdb := newMemDB()
	artdb := NewArenaArt()
	ctx := context.Background()
	var (
		memdbStages []int
		artdbStages []int
	)
	for i := 0; i < 5000; i++ {
		key := randKeyFn(rd)
		stage := rd.Int63()%2 == 0
		if stage {
			memdbStages = append(memdbStages, memdb.Staging())
			artdbStages = append(artdbStages, artdb.Staging())
		}
		hasFlag := rd.Int63()%2 == 0
		if hasFlag {
			flagBits := rd.Int63() % 20
			flagOp := kv.FlagsOp(1 << uint(flagBits))
			assert.Nil(t, memdb.SetWithFlags(key, key, flagOp))
			assert.Nil(t, artdb.SetWithFlags(key, key, flagOp))
			assert.Nil(t, memdb.Set(key, key))
			assert.Nil(t, artdb.Set(key, key))
		}
		assert.Nil(t, memdb.Set(key, key))
		assert.Nil(t, artdb.Set(key, key))

		release := rd.Int63() % 4
		switch release {
		case 0, 1:
			// do nothing, to keep the possibility same with staging(50%)
		case 2:
			if len(memdbStages) > 0 {
				idx := len(memdbStages) - 1
				memdb.Release(memdbStages[idx])
				artdb.Release(artdbStages[idx])
				memdbStages = memdbStages[:idx]
				artdbStages = artdbStages[:idx]
			}
		case 3:
			if len(memdbStages) > 0 {
				idx := len(memdbStages) - 1
				memdb.Cleanup(memdbStages[idx])
				artdb.Cleanup(artdbStages[idx])
				memdbStages = memdbStages[:idx]
				artdbStages = artdbStages[:idx]
			}
		}

		assert.Equal(t, memdb.Dirty(), artdb.Dirty(), i)
		assert.Equal(t, memdb.Len(), artdb.Len(), i)

		val1, err1 := memdb.Get(key)
		val2, err2 := artdb.Get(ctx, key)
		assert.Equal(t, err1, err2)
		assert.Equal(t, val1, val2)
		flag1, err1 := memdb.GetFlags(key)
		flag2, err2 := artdb.GetFlags(key)
		assert.Equal(t, err1, err2)
		assert.Equal(t, flag1, flag2)
		start, end := randKeyFn(rd), randKeyFn(rd)
		if bytes.Compare(start, end) > 0 {
			start, end = end, start
		}
		var memdbIter, artdbIter Iterator
		reverse := rd.Int63()%2 != 0
		upperBound := rd.Int63()%2 == 0
		lowerBound := rd.Int63()%2 == 0
		if !upperBound {
			end = nil
		}
		if !lowerBound {
			start = nil
		}
		var err error
		if reverse {
			memdbIter, err = memdb.IterReverse(end, start)
			assert.Nil(t, err)
			artdbIter, err = artdb.IterReverse(end, start)
			assert.Nil(t, err)
		} else {
			memdbIter, err = memdb.Iter(start, end)
			assert.Nil(t, err)
			artdbIter, err = artdb.Iter(start, end)
			assert.Nil(t, err)
		}
		cnt := 0
		msg := fmt.Sprintf("i: %d", i)
		for memdbIter.Valid() {
			msg = fmt.Sprintf("i: %d, cnt: %d", i, cnt)
			assert.True(t, artdbIter.Valid(), msg)
			memdbKey, artdbKey := memdbIter.Key(), artdbIter.Key()
			assert.Equal(t, memdbKey, artdbKey, msg)
			assert.Equal(t, memdbIter.Value(), artdbIter.Value())
			assert.Nil(t, memdbIter.Next())
			assert.Nil(t, artdbIter.Next())
			cnt++
		}
		assert.False(t, artdbIter.Valid(), msg)

		var (
			memdbFlagIter *MemdbIterator
			artdbFlagIter *art.ArtIterator
		)
		if reverse {
			memdbFlagIter = memdb.IterReverseWithFlags(end)
			artdbFlagIter = artdb.IterReverseWithFlags(end)
		} else {
			memdbFlagIter = memdb.IterWithFlags(start, end)
			artdbFlagIter = artdb.IterWithFlags(start, end)
		}
		cnt = 0
		var (
			memdbHandles []MemKeyHandle
			artdbHandles []art.ArtMemKeyHandle
		)
		for memdbFlagIter.Valid() {
			msg = fmt.Sprintf("i: %d, cnt: %d", i, cnt)
			assert.True(t, artdbFlagIter.Valid(), msg)
			memdbKey, artdbKey := memdbFlagIter.Key(), artdbFlagIter.Key()
			assert.Equal(t, memdbKey, artdbKey, msg)
			assert.Equal(t, memdbFlagIter.HasValue(), artdbFlagIter.HasValue())
			if memdbFlagIter.HasValue() {
				assert.Equal(t, memdbFlagIter.Value(), artdbFlagIter.Value())
			}
			assert.Equal(t, memdbFlagIter.Flags(), artdbFlagIter.Flags())
			memdbHandles = append(memdbHandles, memdbFlagIter.Handle())
			artdbHandles = append(artdbHandles, artdbFlagIter.Handle())
			assert.Nil(t, memdbFlagIter.Next())
			assert.Nil(t, artdbFlagIter.Next())
			cnt++
		}
		for j := 0; j < len(memdbHandles); j++ {
			memdbKey, artdbKey := memdb.GetKeyByHandle(memdbHandles[j]), artdb.GetKeyByHandle(artdbHandles[j])
			assert.Equal(t, memdbKey, artdbKey, i)
			memdbValue, memdbExist := memdb.GetValueByHandle(memdbHandles[j])
			artdbValue, artdbExist := artdb.GetValueByHandle(artdbHandles[j])
			assert.Equal(t, memdbExist, artdbExist)
			assert.Equal(t, memdbValue, artdbValue)
		}
		assert.False(t, artdbFlagIter.Valid(), msg)
	}
}

func TestRandomNumKeyComplex(t *testing.T) {
	for i := 0; i < 20; i++ {
		t.Run(fmt.Sprintf("seed %d", i), func(t *testing.T) {
			t.Parallel()
			testRandomComplex(t, int64(i), randNumKey)
		})
	}
}

func TestRandomTiDBKeyComplex(t *testing.T) {
	for i := 0; i < 20; i++ {
		t.Run(fmt.Sprintf("seed %d", i), func(t *testing.T) {
			t.Parallel()
			testRandomComplex(t, int64(i), randTiDBKey)
		})
	}
}
