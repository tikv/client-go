package unionstore

import (
	art "github.com/plar/go-adaptive-radix-tree"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/kv"
	"math"
	"sync"
)

type ArtMemDB struct {
	sync.RWMutex
	len, size       int
	stages          []int // donot support stages by now
	entrySizeLimit  uint64
	bufferSizeLimit uint64
	vlog            memdbVlog
	tree            art.Tree
}

type FlagValue struct {
	addr  memdbArenaAddr
	flags kv.KeyFlags
}

func newArtMemDB() *ArtMemDB {
	artTree := art.New()
	artTree.Size()
	m := &ArtMemDB{
		tree:            artTree,
		entrySizeLimit:  math.MaxUint64,
		bufferSizeLimit: math.MaxUint64,
	}
	return m
}

func (a *ArtMemDB) Bounds() ([]byte, []byte) {
	lower, _ := a.tree.MinimumKey()
	upper, _ := a.tree.MaximumKey()
	return lower, upper
}

func (a *ArtMemDB) setValue(value []byte, flags kv.KeyFlags) memdbArenaAddr {
	size := memdbVlogHdrSize + len(value) + 2
	addr, mem := a.vlog.alloc(size, false)
	endian.PutUint16(mem, uint16(flags))
	copy(mem[2:], value)
	hdr := memdbVlogHdr{
		valueLen: uint32(len(value)) + 2,
	}
	hdr.store(mem[len(value)+2:])
	addr.off += uint32(size)
	return addr
}

func (a *ArtMemDB) getValue(addr memdbArenaAddr) []byte {
	lenOff := addr.off - memdbVlogHdrSize
	block := a.vlog.blocks[addr.idx].buf
	valueLen := endian.Uint32(block[lenOff:])
	if valueLen == 0 {
		return tombstone
	}
	valueOff := lenOff - valueLen
	return block[valueOff:lenOff:lenOff][2:]
}

func (a *ArtMemDB) getFlag(addr memdbArenaAddr) kv.KeyFlags {
	lenOff := addr.off - memdbVlogHdrSize
	block := a.vlog.blocks[addr.idx].buf
	valueLen := endian.Uint32(block[lenOff:])
	if valueLen == 0 {
		return 0
	}
	valueOff := lenOff - valueLen
	flag := endian.Uint16(block[valueOff:lenOff:lenOff][:2])
	return kv.KeyFlags(flag)
}

func (a *ArtMemDB) appendValue(x memdbNodeAddr) []byte {
	if x.vptr.isNull() {
		return nil
	}
	return a.vlog.getValue(x.vptr)
}

func (a *ArtMemDB) Set(key []byte, value []byte) error {
	a.Lock()
	defer a.Unlock()
	addr := a.setValue(value, 0)
	old, updated := a.tree.Insert(key, addr)
	a.len++
	a.size += len(key) + len(value)
	if old != nil && updated {
		a.len--
		a.size -= len(key) + len(a.getValue(old.(memdbArenaAddr)))
	}
	return nil
}

func (a *ArtMemDB) SetWithFlags(key []byte, value []byte, ops ...kv.FlagsOp) error {
	a.Lock()
	defer a.Unlock()
	addr := a.setValue(value, kv.ApplyFlagsOps(0, ops...))
	old, updated := a.tree.Insert(key, addr)
	a.len++
	a.size += len(key) + len(value)
	if old != nil && updated {
		a.len--
		a.size -= len(key) + len(a.getValue(old.(memdbArenaAddr)))
	}
	return nil
}

func (a *ArtMemDB) Delete(key []byte) error {
	a.Lock()
	defer a.Unlock()
	addr := a.setValue(tombstone, 0)
	old, updated := a.tree.Insert(key, addr)
	a.len++
	a.size += len(key)
	if old != nil && updated {
		a.len--
		a.size -= len(key) + len(a.getValue(old.(memdbArenaAddr)))
	}
	return nil
}

func (a *ArtMemDB) DeleteWithFlags(key []byte, ops ...kv.FlagsOp) error {
	a.Lock()
	defer a.Unlock()
	var flags kv.KeyFlags
	val, found := a.tree.Search(key)
	if found {
		flags = a.getFlag(val.(memdbArenaAddr))
	}
	addr := a.setValue(tombstone, kv.ApplyFlagsOps(flags, ops...))
	old, updated := a.tree.Insert(key, addr)
	a.len++
	a.size += len(key)
	if old != nil && updated {
		a.len--
		a.size -= len(key) + len(a.getValue(old.(memdbArenaAddr)))
	}
	return nil
}

func (a *ArtMemDB) UpdateFlags(key []byte, ops ...kv.FlagsOp) {
	a.Lock()
	defer a.Unlock()
	_, found := a.tree.Search(key)
	if !found {
		return
	}
	//flagVal := val.(*)
	//flagVal.flags = kv.ApplyFlagsOps(flagVal.flags, ops...)
}

func (a *ArtMemDB) Get(key []byte) ([]byte, error) {
	flagVal, found := a.tree.Search(key)
	if !found {
		return nil, tikverr.ErrNotExist
	}
	return a.getValue(flagVal.(memdbArenaAddr)), nil
}

func (a *ArtMemDB) GetFlags(key []byte) (kv.KeyFlags, error) {
	flagVal, found := a.tree.Search(key)
	if !found {
		return 0, tikverr.ErrNotExist
	}
	return a.getFlag(flagVal.(memdbArenaAddr)), nil
}

func (a *ArtMemDB) Dirty() bool {
	return a.tree.Size() > 0
}

func (a *ArtMemDB) Len() int {
	return a.len
}

func (a *ArtMemDB) Size() int {
	return a.size
}

func (a *ArtMemDB) Mem() uint64 {
	return uint64(a.size)
}

func (a *ArtMemDB) Staging() int {
	return 0
}

func (a *ArtMemDB) Release(int) {}

func (a *ArtMemDB) Cleanup(int) {}

var _ Iterator = &ArtMemDBIterator{}

type ArtMemDBIterator struct {
	artTree  *ArtMemDB
	from, to []byte
	inner    art.Iterator
	cur      art.Node
	valid    bool
}

func (a *ArtMemDB) Iter(k []byte, upperBound []byte) (Iterator, error) {
	return a.IterWithFlags(k, upperBound), nil
}

func (a *ArtMemDB) IterWithFlags(k []byte, upperBound []byte) *ArtMemDBIterator {
	inner := a.tree.Iterator(art.TraverseAll)
	iterator := &ArtMemDBIterator{artTree: a, from: k, to: upperBound, inner: inner, cur: nil, valid: true}
	iterator.Next()
	return iterator
}

func (a *ArtMemDBIterator) Valid() bool {
	return a.valid
}

func (a *ArtMemDBIterator) Next() error {
	var err error
	for {
		a.cur, err = a.inner.Next()
		if err != nil {
			a.valid = false
			return nil
		}
		currKey := []byte(a.cur.Key())
		if currKey == nil {
			continue
		}
		if a.to != nil && kv.CmpKey(currKey, a.to) > 0 {
			return nil
		}
		if a.from != nil && kv.CmpKey(currKey, a.from) < 0 {
			continue
		}
		return nil
	}
}

func (a *ArtMemDBIterator) Flags() kv.KeyFlags {
	keyFlags := a.cur.Value().(*FlagValue)
	return keyFlags.flags
}

func (a *ArtMemDBIterator) HasValue() bool {
	val := a.cur.Value()
	if val == nil {
		return false
	}
	return val.(memdbArenaAddr) != memdbArenaAddr{}
}

func (a *ArtMemDBIterator) Key() []byte {
	return a.cur.Key()
}

func (a *ArtMemDBIterator) Value() []byte {
	return a.artTree.getValue(a.cur.Value().(memdbArenaAddr))
}

func (a *ArtMemDBIterator) Close() {}
