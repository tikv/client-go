package tikv

import (
	"context"

	. "github.com/pingcap/check"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/unionstore"
)

var _ = Suite(&testBatchGetterSuite{})

type testBatchGetterSuite struct{}

func (s *testBatchGetterSuite) TestBufferBatchGetter(c *C) {
	snap := newMockStore()
	ka := []byte("a")
	kb := []byte("b")
	kc := []byte("c")
	kd := []byte("d")
	snap.Set(ka, ka)
	snap.Set(kb, kb)
	snap.Set(kc, kc)
	snap.Set(kd, kd)

	buffer := unionstore.NewUnionStore(snap)
	buffer.GetMemBuffer().Set(ka, []byte("a1"))
	buffer.GetMemBuffer().Delete(kb)

	batchGetter := NewBufferBatchGetter(buffer.GetMemBuffer(), snap)
	result, err := batchGetter.BatchGet(context.Background(), [][]byte{ka, kb, kc, kd})
	c.Assert(err, IsNil)
	c.Assert(len(result), Equals, 3)
	c.Assert(string(result[string(ka)]), Equals, "a1")
	c.Assert(string(result[string(kc)]), Equals, "c")
	c.Assert(string(result[string(kd)]), Equals, "d")
}

type mockBatchGetterStore struct {
	index [][]byte
	value [][]byte
}

func newMockStore() *mockBatchGetterStore {
	return &mockBatchGetterStore{
		index: make([][]byte, 0),
		value: make([][]byte, 0),
	}
}

func (s *mockBatchGetterStore) Len() int {
	return len(s.index)
}

func (s *mockBatchGetterStore) Get(ctx context.Context, k []byte) ([]byte, error) {
	for i, key := range s.index {
		if kv.CmpKey(key, k) == 0 {
			return s.value[i], nil
		}
	}
	return nil, tikverr.ErrNotExist
}

func (s *mockBatchGetterStore) BatchGet(ctx context.Context, keys [][]byte) (map[string][]byte, error) {
	m := make(map[string][]byte)
	for _, k := range keys {
		v, err := s.Get(ctx, k)
		if err == nil {
			m[string(k)] = v
			continue
		}
		if tikverr.IsErrNotFound(err) {
			continue
		}
		return m, err
	}
	return m, nil
}

func (s *mockBatchGetterStore) Set(k []byte, v []byte) error {
	for i, key := range s.index {
		if kv.CmpKey(key, k) == 0 {
			s.value[i] = v
			return nil
		}
	}
	s.index = append(s.index, k)
	s.value = append(s.value, v)
	return nil
}

func (s *mockBatchGetterStore) Delete(k []byte) error {
	s.Set(k, []byte{})
	return nil
}

func (s *mockBatchGetterStore) Iter(k []byte, upperBound []byte) (unionstore.Iterator, error) {
	panic("unimplemented")
}

func (s *mockBatchGetterStore) IterReverse(k []byte) (unionstore.Iterator, error) {
	panic("unimplemented")
}
