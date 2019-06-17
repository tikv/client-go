// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package proxy

import (
	"context"
	"sync"
	"unsafe"

	"github.com/pkg/errors"
	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/key"
	"github.com/tikv/client-go/txnkv"
	"github.com/tikv/client-go/txnkv/kv"
)

// TxnKVProxy implements proxy to use txnkv API.
// It is safe to copy by value or access concurrently.
type TxnKVProxy struct {
	clients   *sync.Map
	txns      *sync.Map
	iterators *sync.Map
}

// NewTxn creates a TxnKVProxy instance.
func NewTxn() TxnKVProxy {
	return TxnKVProxy{
		clients:   &sync.Map{},
		txns:      &sync.Map{},
		iterators: &sync.Map{},
	}
}

// New creates a new client and returns the client's UUID.
func (p TxnKVProxy) New(ctx context.Context, pdAddrs []string, conf config.Config) (UUID, error) {
	client, err := txnkv.NewClient(ctx, pdAddrs, conf)
	if err != nil {
		return "", err
	}
	return insertWithRetry(p.clients, client), nil
}

// Close releases a txnkv client.
func (p TxnKVProxy) Close(ctx context.Context) error {
	id := uuidFromContext(ctx)
	client, ok := p.clients.Load(id)
	if !ok {
		return errors.WithStack(ErrClientNotFound)
	}
	if err := client.(*txnkv.Client).Close(); err != nil {
		return err
	}
	p.clients.Delete(id)
	return nil
}

// Begin starts a new transaction and returns its UUID.
func (p TxnKVProxy) Begin(ctx context.Context) (UUID, error) {
	client, ok := p.clients.Load(uuidFromContext(ctx))
	if !ok {
		return "", errors.WithStack(ErrClientNotFound)
	}
	txn, err := client.(*txnkv.Client).Begin()
	if err != nil {
		return "", err
	}
	return insertWithRetry(p.txns, txn), nil
}

// BeginWithTS starts a new transaction with given ts and returns its UUID.
func (p TxnKVProxy) BeginWithTS(ctx context.Context, ts uint64) (UUID, error) {
	client, ok := p.clients.Load(uuidFromContext(ctx))
	if !ok {
		return "", errors.WithStack(ErrClientNotFound)
	}
	return insertWithRetry(p.txns, client.(*txnkv.Client).BeginWithTS(ts)), nil
}

// GetTS returns a latest timestamp.
func (p TxnKVProxy) GetTS(ctx context.Context) (uint64, error) {
	client, ok := p.clients.Load(uuidFromContext(ctx))
	if !ok {
		return 0, errors.WithStack(ErrClientNotFound)
	}
	return client.(*txnkv.Client).GetTS()
}

// TxnGet queries value for the given key from TiKV server.
func (p TxnKVProxy) TxnGet(ctx context.Context, key []byte) ([]byte, error) {
	txn, ok := p.txns.Load(uuidFromContext(ctx))
	if !ok {
		return nil, errors.WithStack(ErrTxnNotFound)
	}
	return txn.(*txnkv.Transaction).Get(key)
}

// TxnBatchGet gets a batch of values from TiKV server.
func (p TxnKVProxy) TxnBatchGet(ctx context.Context, keys [][]byte) (map[string][]byte, error) {
	txn, ok := p.txns.Load(uuidFromContext(ctx))
	if !ok {
		return nil, errors.WithStack(ErrTxnNotFound)
	}
	ks := *(*[]key.Key)(unsafe.Pointer(&keys))
	return txn.(*txnkv.Transaction).BatchGet(ks)
}

// TxnSet sets the value for key k as v into TiKV server.
func (p TxnKVProxy) TxnSet(ctx context.Context, k []byte, v []byte) error {
	txn, ok := p.txns.Load(uuidFromContext(ctx))
	if !ok {
		return errors.WithStack(ErrTxnNotFound)
	}
	return txn.(*txnkv.Transaction).Set(k, v)
}

// TxnIter creates an Iterator positioned on the first entry that key <= entry's
// key and returns the Iterator's UUID.
func (p TxnKVProxy) TxnIter(ctx context.Context, key []byte, upperBound []byte) (UUID, error) {
	txn, ok := p.txns.Load(uuidFromContext(ctx))
	if !ok {
		return "", errors.WithStack(ErrTxnNotFound)
	}
	iter, err := txn.(*txnkv.Transaction).Iter(key, upperBound)
	if err != nil {
		return "", err
	}
	return insertWithRetry(p.iterators, iter), nil
}

// TxnIterReverse creates a reversed Iterator positioned on the first entry
// which key is less than key and returns the Iterator's UUID.
func (p TxnKVProxy) TxnIterReverse(ctx context.Context, key []byte) (UUID, error) {
	txn, ok := p.txns.Load(uuidFromContext(ctx))
	if !ok {
		return "", errors.WithStack(ErrTxnNotFound)
	}
	iter, err := txn.(*txnkv.Transaction).IterReverse(key)
	if err != nil {
		return "", err
	}
	return insertWithRetry(p.iterators, iter), nil
}

// TxnIsReadOnly returns if there are pending key-value to commit in the transaction.
func (p TxnKVProxy) TxnIsReadOnly(ctx context.Context) (bool, error) {
	txn, ok := p.txns.Load(uuidFromContext(ctx))
	if !ok {
		return false, errors.WithStack(ErrTxnNotFound)
	}
	return txn.(*txnkv.Transaction).IsReadOnly(), nil
}

// TxnDelete removes the entry for key from TiKV server.
func (p TxnKVProxy) TxnDelete(ctx context.Context, key []byte) error {
	txn, ok := p.txns.Load(uuidFromContext(ctx))
	if !ok {
		return errors.WithStack(ErrTxnNotFound)
	}
	return txn.(*txnkv.Transaction).Delete(key)
}

// TxnCommit commits the transaction operations to TiKV server.
func (p TxnKVProxy) TxnCommit(ctx context.Context) error {
	id := uuidFromContext(ctx)
	txn, ok := p.txns.Load(id)
	if !ok {
		return errors.WithStack(ErrTxnNotFound)
	}
	defer p.txns.Delete(id)
	return txn.(*txnkv.Transaction).Commit(context.Background())
}

// TxnRollback undoes the transaction operations to TiKV server.
func (p TxnKVProxy) TxnRollback(ctx context.Context) error {
	id := uuidFromContext(ctx)
	txn, ok := p.txns.Load(id)
	if !ok {
		return errors.WithStack(ErrTxnNotFound)
	}
	defer p.txns.Delete(id)
	return txn.(*txnkv.Transaction).Rollback()
}

// TxnLockKeys tries to lock the entries with the keys in TiKV server.
func (p TxnKVProxy) TxnLockKeys(ctx context.Context, keys [][]byte) error {
	txn, ok := p.txns.Load(uuidFromContext(ctx))
	if !ok {
		return errors.WithStack(ErrTxnNotFound)
	}
	ks := *(*[]key.Key)(unsafe.Pointer(&keys))
	return txn.(*txnkv.Transaction).LockKeys(ks...)
}

// TxnValid returns if the transaction is valid.
func (p TxnKVProxy) TxnValid(ctx context.Context) (bool, error) {
	txn, ok := p.txns.Load(uuidFromContext(ctx))
	if !ok {
		return false, errors.WithStack(ErrTxnNotFound)
	}
	return txn.(*txnkv.Transaction).Valid(), nil
}

// TxnLen returns the count of key-value pairs in the transaction's memory buffer.
func (p TxnKVProxy) TxnLen(ctx context.Context) (int, error) {
	txn, ok := p.txns.Load(uuidFromContext(ctx))
	if !ok {
		return 0, errors.WithStack(ErrTxnNotFound)
	}
	return txn.(*txnkv.Transaction).Len(), nil
}

// TxnSize returns the length (in bytes) of the transaction's memory buffer.
func (p TxnKVProxy) TxnSize(ctx context.Context) (int, error) {
	txn, ok := p.txns.Load(uuidFromContext(ctx))
	if !ok {
		return 0, errors.WithStack(ErrTxnNotFound)
	}
	return txn.(*txnkv.Transaction).Size(), nil
}

// IterValid returns if the iterator is valid to use.
func (p TxnKVProxy) IterValid(ctx context.Context) (bool, error) {
	iter, ok := p.iterators.Load(uuidFromContext(ctx))
	if !ok {
		return false, errors.WithStack(ErrIterNotFound)
	}
	return iter.(kv.Iterator).Valid(), nil
}

// IterKey returns the key which the iterator points to.
func (p TxnKVProxy) IterKey(ctx context.Context) ([]byte, error) {
	iter, ok := p.iterators.Load(uuidFromContext(ctx))
	if !ok {
		return nil, errors.WithStack(ErrIterNotFound)
	}
	return iter.(kv.Iterator).Key(), nil
}

// IterValue returns the value which the iterator points to.
func (p TxnKVProxy) IterValue(ctx context.Context) ([]byte, error) {
	iter, ok := p.iterators.Load(uuidFromContext(ctx))
	if !ok {
		return nil, errors.WithStack(ErrIterNotFound)
	}
	return iter.(kv.Iterator).Value(), nil
}

// IterNext moves the iterator to next entry.
func (p TxnKVProxy) IterNext(ctx context.Context) error {
	iter, ok := p.iterators.Load(uuidFromContext(ctx))
	if !ok {
		return errors.WithStack(ErrIterNotFound)
	}
	return iter.(kv.Iterator).Next()
}

// IterClose releases an iterator.
func (p TxnKVProxy) IterClose(ctx context.Context) error {
	id := uuidFromContext(ctx)
	iter, ok := p.iterators.Load(id)
	if !ok {
		return errors.WithStack(ErrIterNotFound)
	}
	iter.(kv.Iterator).Close()
	p.iterators.Delete(id)
	return nil
}
