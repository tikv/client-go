// Copyright 2025 TiKV Authors
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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/kv.go
package tikv

import (
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/tikv/client-go/v2/config"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/namespace"
)

var etcdMutex sync.Mutex
var etcdClients map[string]*clientv3.Client = make(map[string]*clientv3.Client)

func getSharedEtcdClient(addrs []string) (*clientv3.Client, error) {
	etcdMutex.Lock()
	defer etcdMutex.Unlock()

	key := addrsToKey(addrs)
	if cli, ok := etcdClients[key]; ok {
		return cli, nil
	}

	cfg := config.GetGlobalConfig()
	tlsConfig, err := cfg.Security.ToTLSConfig()
	if err != nil {
		return nil, err
	}
	cli, err := clientv3.New(
		clientv3.Config{
			Endpoints:        addrs,
			AutoSyncInterval: 30 * time.Second,
			DialTimeout:      5 * time.Second,
			TLS:              tlsConfig,
		},
	)
	if err != nil {
		return nil, err
	}
	etcdClients[key] = cli
	return cli, nil
}

// GetEtcdClient returns an etcd client for the given addresses. If a client for these addresses already exists,
// it returns the existing client. Otherwise, it creates a new client and stores it in the etcdClients map.
func GetEtcdClient(addrs []string) (EtcdClient, error) {
	cli, err := getSharedEtcdClient(addrs)
	if err != nil {
		return nil, err
	}
	return newWrappedEtcdClient(cli), nil
}

// GetEtcdClientWithPrefix returns an etcd client with the specified prefix for the given addresses.
// If a client for these addresses already exists, it returns the existing client.
// Otherwise, it creates a new client and stores it in the etcdClients map.
func GetEtcdClientWithPrefix(addrs []string, prefix string) (EtcdClient, error) {
	if prefix == "" {
		return GetEtcdClient(addrs)
	}
	cli, err := getSharedEtcdClient(addrs)
	if err != nil {
		return nil, err
	}
	return newWrappedEtcdClientWithKeyPrefix(cli, prefix), nil
}

func addrsToKey(addrs []string) string {
	sort.Strings(addrs)
	return strings.Join(addrs, "-")
}

// EtcdClient wraps clientv3.Client.
type EtcdClient interface {
	clientv3.Cluster
	clientv3.KV
	clientv3.Lease
	clientv3.Watcher
	clientv3.Auth
	clientv3.Maintenance
}

type wrappedEtcdClient struct {
	inner *clientv3.Client

	clientv3.Cluster
	clientv3.Auth
	clientv3.Maintenance

	clientv3.KV
	clientv3.Lease
	clientv3.Watcher
}

func newWrappedEtcdClient(inner *clientv3.Client) EtcdClient {
	return &wrappedEtcdClient{
		inner:   inner,
		Cluster: inner.Cluster,
		KV:      inner.KV,
		Lease:   inner.Lease,

		Watcher:     inner.Watcher,
		Auth:        inner.Auth,
		Maintenance: inner.Maintenance,
	}
}

func newWrappedEtcdClientWithKeyPrefix(inner *clientv3.Client, prefix string) EtcdClient {
	return &wrappedEtcdClient{
		inner:       inner,
		Cluster:     inner.Cluster,
		Auth:        inner.Auth,
		Maintenance: inner.Maintenance,

		KV:      namespace.NewKV(inner.KV, prefix),
		Lease:   namespace.NewLease(inner.Lease, prefix),
		Watcher: namespace.NewWatcher(inner.Watcher, prefix),
	}
}

func (c *wrappedEtcdClient) Close() error {
	return c.inner.Close()
}
