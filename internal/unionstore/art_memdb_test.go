package unionstore

import (
	"bytes"
	"sort"
	"testing"

	"encoding/binary"
	"github.com/stretchr/testify/require"
)

func fillArtDB(cnt int) *ArtMemDB {
	db := newArtMemDB()
	deriveAndFillArt(0, cnt, 0, db)
	return db
}

func deriveAndFillArt(start, end, valueBase int, db *ArtMemDB) {
	var kbuf, vbuf [4]byte
	for i := start; i < end; i++ {
		binary.BigEndian.PutUint32(kbuf[:], uint32(i))
		binary.BigEndian.PutUint32(vbuf[:], uint32(i+valueBase))
		db.Set(kbuf[:], vbuf[:])
	}
}

func TestArtGetSet(t *testing.T) {
	require := require.New(t)

	const cnt = 10000
	p := fillDB(cnt)

	var buf [4]byte
	for i := 0; i < cnt; i++ {
		binary.BigEndian.PutUint32(buf[:], uint32(i))
		v, err := p.Get(buf[:])
		require.Nil(err)
		require.Equal(v, buf[:])
	}
}

func TestArtIterator(t *testing.T) {
	p := fillArtDB(10000)
	keys := make([][]byte, 0, 10000)
	values := make([][]byte, 0, 10000)
	for it := p.IterWithFlags(nil, nil); it.Valid(); it.Next() {
		require.True(t, it.HasValue())
		keys = append(keys, it.Key())
		values = append(values, it.Value())
	}

	require.Len(t, keys, 10000)
	require.Len(t, values, 10000)

	expectKeyValues := make([][2][]byte, 0, 10000)
	for i := 0; i < 10000; i++ {
		var kbuf, vbuf [4]byte
		binary.BigEndian.PutUint32(kbuf[:], uint32(i))
		binary.BigEndian.PutUint32(vbuf[:], uint32(i+0))
		expectKeyValues = append(expectKeyValues, [2][]byte{kbuf[:], vbuf[:]})
	}
	sort.Slice(expectKeyValues, func(i, j int) bool {
		return bytes.Compare(expectKeyValues[i][0], expectKeyValues[j][0]) <= 0
	})
	for i := 0; i < 10000; i++ {
		require.Equal(t, expectKeyValues[i][0], keys[i])
		require.Equal(t, expectKeyValues[i][1], values[i])
	}
	lower, upper := p.Bounds()
	require.Equal(t, lower, keys[0])
	require.Equal(t, upper, keys[len(keys)-1])
}
