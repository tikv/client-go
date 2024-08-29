package art

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/kv"
)

func TestSimple(t *testing.T) {
	tree := New()
	for i := 0; i < 256; i++ {
		key := []byte{byte(i)}
		_, err := tree.Get(key)
		assert.Equal(t, err, tikverr.ErrNotExist)
		err = tree.Set(key, key)
		assert.Nil(t, err)
		val, err := tree.Get(key)
		assert.Nil(t, err, i)
		assert.Equal(t, val, key, i)
	}
}

func TestSubNode(t *testing.T) {
	tree := New()
	assert.Nil(t, tree.Set([]byte("a"), []byte("a")))
	assert.Nil(t, tree.Set([]byte("aa"), []byte("aa")))
	assert.Nil(t, tree.Set([]byte("aaa"), []byte("aaa")))
	v, err := tree.Get([]byte("a"))
	assert.Nil(t, err)
	assert.Equal(t, v, []byte("a"))
	v, err = tree.Get([]byte("aa"))
	assert.Nil(t, err)
	assert.Equal(t, v, []byte("aa"))
	v, err = tree.Get([]byte("aaa"))
	assert.Nil(t, err)
	assert.Equal(t, v, []byte("aaa"))
}

func BenchmarkReadAfterWriteArt(b *testing.B) {
	buf := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		key := []byte{byte(i)}
		buf[i] = key
	}
	tree := New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Set(buf[i], buf[i])
		v, _ := tree.Get(buf[i])
		assert.Equal(b, v, buf[i])
	}
}

func encodeInt(n int) []byte {
	return []byte(fmt.Sprintf("%010d", n))
}

func TestBenchKey(t *testing.T) {
	buffer := New()
	cnt := 100000
	for k := 0; k < cnt; k++ {
		key, value := encodeInt(k), encodeInt(k)
		buffer.Set(key, value)
	}
	for k := 0; k < cnt; k++ {
		v, err := buffer.Get(encodeInt(k))
		assert.Nil(t, err, k)
		assert.Equal(t, v, encodeInt(k))
	}
}

func TestLeafWithCommonPrefix(t *testing.T) {
	tree := New()
	tree.Set([]byte{1, 1, 1}, []byte{1, 1, 1})
	tree.Set([]byte{1, 1, 2}, []byte{1, 1, 2})
	v, err := tree.Get([]byte{1, 1, 1})
	assert.Nil(t, err)
	assert.Equal(t, v, []byte{1, 1, 1})
	v, err = tree.Get([]byte{1, 1, 2})
	assert.Nil(t, err)
	assert.Equal(t, v, []byte{1, 1, 2})
}

func TestUpdateInplace(t *testing.T) {
	tree := New()
	key := []byte{0}
	for i := 0; i < 256; i++ {
		val := make([]byte, 4096)
		tree.Set(key, val)
		assert.Equal(t, tree.allocator.vlogAllocator.Blocks(), 1)
	}
}

func TestFlag(t *testing.T) {
	tree := New()
	tree.Set([]byte{0}, []byte{0}, kv.SetPresumeKeyNotExists)
	flags, err := tree.GetFlags([]byte{0})
	assert.Nil(t, err)
	assert.True(t, flags.HasPresumeKeyNotExists())
	tree.Set([]byte{1}, []byte{1}, kv.SetKeyLocked)
	flags, err = tree.GetFlags([]byte{1})
	assert.Nil(t, err)
	assert.True(t, flags.HasLocked())
	// iterate can also see the flags
	//it, err := tree.Iter(nil, nil)
	//assert.Nil(t, err)
	//assert.True(t, it.Valid())
	//assert.Equal(t, it.Key(), []byte{0})
	//assert.Equal(t, it.Value(), []byte{0})
	//assert.True(t, it.Flags().HasPresumeKeyNotExists())
	//assert.False(t, it.Flags().HasLocked())
	//assert.Nil(t, it.Next())
	//assert.True(t, it.Valid())
	//assert.Equal(t, it.Key(), []byte{1})
	//assert.Equal(t, it.Value(), []byte{1})
	//assert.True(t, it.Flags().HasLocked())
	//assert.False(t, it.Flags().HasPresumeKeyNotExists())
	//assert.Nil(t, it.Next())
	//assert.False(t, it.Valid())
}

func TestLongPrefix1(t *testing.T) {
	key1 := []byte{109, 68, 66, 115, 0, 0, 0, 0, 0, 250, 0, 0, 0, 0, 0, 0, 0, 104, 68, 66, 58, 49, 0, 0, 0, 0, 251}
	key2 := []byte{109, 68, 66, 115, 0, 0, 0, 0, 0, 250, 0, 0, 0, 0, 0, 0, 0, 105, 68, 66, 58, 49, 0, 0, 0, 0, 251}
	buffer := New()
	assert.Nil(t, buffer.Set(key1, []byte{1}))
	assert.Nil(t, buffer.Set(key2, []byte{2}))
	val, err := buffer.Get(key1)
	assert.Nil(t, err)
	assert.Equal(t, val, []byte{1})
	val, err = buffer.Get(key2)
	assert.Nil(t, err)
	assert.Equal(t, val, []byte{2})
	assert.Nil(t, buffer.Set(key2, []byte{3}))
	val, err = buffer.Get(key2)
	assert.Nil(t, err)
	assert.Equal(t, val, []byte{3})
}

func TestLongPrefix2(t *testing.T) {
	tree := New()
	key1 := []byte{0, 97, 0, 0, 0, 0, 0, 0, 0, 248, 0, 0, 0, 0, 0, 0, 0, 108, 127, 255, 255, 255, 255, 255, 255, 255}
	key2 := []byte{0, 97, 0, 0, 0, 0, 0, 0, 0, 248, 0, 0, 0, 0, 0, 0, 0, 108, 127, 255, 255, 255, 255, 255, 255, 254}
	key3 := []byte{0, 97, 0, 0, 0, 0, 0, 0, 0, 248, 0, 0, 0, 0, 0, 0, 0, 108, 127, 255, 255, 255, 255, 255, 255, 253}
	key4 := []byte{0, 97, 0, 0, 0, 0, 0, 0, 0, 248, 0, 0, 0, 0, 0, 0, 0, 76}
	key5 := []byte{0, 97, 0, 0, 0, 0, 0, 0, 0, 248, 0, 0, 0, 0, 0, 0, 0, 108, 127, 255, 255, 255, 255, 255, 255, 252}
	assert.Nil(t, tree.Set(key1, key1))
	assert.Nil(t, tree.Set(key2, key2))
	assert.Nil(t, tree.Set(key3, key3))
	assert.Nil(t, tree.Set(key4, key4))
	assert.Nil(t, tree.Set(key5, key5))
	assert.Nil(t, tree.Set(key4, key4))
	val, err := tree.Get(key1)
	assert.Nil(t, err)
	assert.Equal(t, val, key1)
	val, err = tree.Get(key2)
	assert.Nil(t, err)
	assert.Equal(t, val, key2)
	val, err = tree.Get(key3)
	assert.Nil(t, err)
	assert.Equal(t, val, key3)
	val, err = tree.Get(key4)
	assert.Nil(t, err)
	assert.Equal(t, val, key4)
	val, err = tree.Get(key5)
	assert.Nil(t, err)
	assert.Equal(t, val, key5)
}

func TestFlagOnlyKey(t *testing.T) {
	tree := New()
	tree.Set([]byte{0}, nil, kv.SetAssertNone)
	flags, err := tree.GetFlags([]byte{0})
	assert.Nil(t, err)
	assert.False(t, flags.HasAssertionFlags())
	_, err = tree.Get([]byte{0})
	assert.Error(t, err)
}

func TestSearchOptimisticMismatch(t *testing.T) {
	tree := New()
	prefix := make([]byte, 22)
	tree.Set(append(prefix, []byte{1}...), prefix)
	tree.Set(append(prefix, []byte{2}...), prefix)
	// the search key is matched within maxPrefixLen, but the full key is not matched.
	_, err := tree.Get(append(make([]byte, 21), []byte{1, 1}...))
	assert.NotNil(t, err)
}
