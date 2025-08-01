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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/safepoint.go
//

// Copyright 2017 PingCAP, Inc.
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

package tikv

import (
	"context"
	"crypto/tls"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/internal/logutil"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/namespace"
	"go.uber.org/zap"
)

// Safe point constants.
const (
	// This is almost the same as 'tikv_gc_safe_point' in the table 'mysql.tidb',
	// save this to pd instead of tikv, because we can't use interface of table
	// if the safepoint on tidb is expired.
	// Deprecated: All use this is no longer recommended and should be replaced by using the txn safe point.
	GcSavedSafePoint = "/tidb/store/gcworker/saved_safe_point"

	GcStateCacheInterval                = time.Second * 100
	gcCPUTimeInaccuracyBound            = time.Second * 10
	pollTxnSafePointInterval            = time.Second * 10
	pollTxnSafePointQuickRepeatInterval = time.Second
)

// SafePointKV is used for a seamingless integration for mockTest and runtime.
type SafePointKV interface {
	Put(k string, v string) error
	Get(k string) (string, error)
	GetWithPrefix(k string) ([]*mvccpb.KeyValue, error)
	Close() error

	extractConnectionInfo() (endpoints []string, tlsConfig *tls.Config)
}

// MockSafePointKV implements SafePointKV at mock test
type MockSafePointKV struct {
	store    map[string]string
	mockLock sync.RWMutex
}

// NewMockSafePointKV creates an instance of MockSafePointKV
func NewMockSafePointKV(opts ...SafePointKVOpt) *MockSafePointKV {
	return &MockSafePointKV{
		store: make(map[string]string),
	}
}

// Put implements the Put method for SafePointKV
func (w *MockSafePointKV) Put(k string, v string) error {
	w.mockLock.Lock()
	defer w.mockLock.Unlock()
	w.store[k] = v
	return nil
}

// Get implements the Get method for SafePointKV
func (w *MockSafePointKV) Get(k string) (string, error) {
	w.mockLock.RLock()
	defer w.mockLock.RUnlock()
	elem := w.store[k]
	return elem, nil
}

// GetWithPrefix implements the Get method for SafePointKV
func (w *MockSafePointKV) GetWithPrefix(prefix string) ([]*mvccpb.KeyValue, error) {
	w.mockLock.RLock()
	defer w.mockLock.RUnlock()
	kvs := make([]*mvccpb.KeyValue, 0, len(w.store))
	for k, v := range w.store {
		if strings.HasPrefix(k, prefix) {
			kvs = append(kvs, &mvccpb.KeyValue{Key: []byte(k), Value: []byte(v)})
		}
	}
	return kvs, nil
}

// Close implements the Close method for SafePointKV
func (w *MockSafePointKV) Close() error {
	return nil
}

func (w *MockSafePointKV) extractConnectionInfo() (endpoints []string, tlsConfig *tls.Config) {
	return nil, nil
}

// option represents safePoint kv configuration.
type option struct {
	prefix string
}

// SafePointKVOpt is used to set safePoint kv option.
type SafePointKVOpt func(*option)

func WithPrefix(prefix string) SafePointKVOpt {
	return func(opt *option) {
		opt.prefix = prefix
	}
}

// EtcdSafePointKV implements SafePointKV at runtime
type EtcdSafePointKV struct {
	addrs     []string
	tlsConfig *tls.Config
	cli       *clientv3.Client
}

// NewEtcdSafePointKV creates an instance of EtcdSafePointKV
func NewEtcdSafePointKV(addrs []string, tlsConfig *tls.Config, opts ...SafePointKVOpt) (*EtcdSafePointKV, error) {
	// Apply options.
	opt := &option{}
	for _, o := range opts {
		o(opt)
	}
	etcdCli, err := createEtcdKV(addrs, tlsConfig)
	if err != nil {
		return nil, err
	}
	// If a prefix is specified, wrap the etcd client with the target namespace.
	if opt.prefix != "" {
		etcdCli.KV = namespace.NewKV(etcdCli.KV, opt.prefix)
		etcdCli.Watcher = namespace.NewWatcher(etcdCli.Watcher, opt.prefix)
		etcdCli.Lease = namespace.NewLease(etcdCli.Lease, opt.prefix)
	}
	return &EtcdSafePointKV{
		addrs:     addrs,
		tlsConfig: tlsConfig,
		cli:       etcdCli,
	}, nil
}

// Put implements the Put method for SafePointKV
func (w *EtcdSafePointKV) Put(k string, v string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	_, err := w.cli.Put(ctx, k, v)
	cancel()
	return errors.WithStack(err)
}

// Get implements the Get method for SafePointKV
func (w *EtcdSafePointKV) Get(k string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	resp, err := w.cli.Get(ctx, k)
	cancel()
	if err != nil {
		return "", errors.WithStack(err)
	}
	if len(resp.Kvs) > 0 {
		return string(resp.Kvs[0].Value), nil
	}
	return "", nil
}

// GetWithPrefix implements the GetWithPrefix for SafePointKV
func (w *EtcdSafePointKV) GetWithPrefix(k string) ([]*mvccpb.KeyValue, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	resp, err := w.cli.Get(ctx, k, clientv3.WithPrefix())
	cancel()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return resp.Kvs, nil
}

// Close implements the Close for SafePointKV
func (w *EtcdSafePointKV) Close() error {
	return errors.WithStack(w.cli.Close())
}

func (w *EtcdSafePointKV) extractConnectionInfo() (endpoints []string, tlsConfig *tls.Config) {
	return w.addrs, w.tlsConfig
}

// Deprecated: Do not use
func saveSafePoint(kv SafePointKV, t uint64) error {
	s := strconv.FormatUint(t, 10)
	err := kv.Put(GcSavedSafePoint, s)
	if err != nil {
		logutil.BgLogger().Error("save safepoint failed", zap.Error(err))
		return err
	}
	return nil
}

// Deprecated: Do not use
func loadSafePoint(kv SafePointKV) (uint64, error) {
	str, err := kv.Get(GcSavedSafePoint)

	if err != nil {
		return 0, err
	}

	if str == "" {
		return 0, nil
	}

	t, err := strconv.ParseUint(str, 10, 64)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return t, nil
}
