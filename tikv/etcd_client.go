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
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/tikv/client-go/v2/config"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/namespace"
	zap "go.uber.org/zap"
	"google.golang.org/grpc"
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
	etcdLogCfg := zap.NewProductionConfig()
	cli, err := clientv3.New(
		clientv3.Config{
			LogConfig:        &etcdLogCfg,
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

	Ctx() context.Context
	ActiveConnection() *grpc.ClientConn
	Endpoints() []string
	Close() error
}

type wrappedEtcdClient struct {
	inner  *clientv3.Client
	prefix string
	ctx    context.Context
	cancel context.CancelFunc

	clientv3.Cluster
	clientv3.Auth
	clientv3.Maintenance

	clientv3.KV
	clientv3.Lease
	clientv3.Watcher
}

func newWrappedEtcdClient(inner *clientv3.Client) EtcdClient {
	ctx, cancel := context.WithCancel(inner.Ctx())
	return &wrappedEtcdClient{
		inner:       inner,
		ctx:         ctx,
		cancel:      cancel,
		Cluster:     inner.Cluster,
		KV:          inner.KV,
		Lease:       wrapLeaseClient(inner.Lease),
		Watcher:     wrapWatchClient(inner.Watcher),
		Auth:        inner.Auth,
		Maintenance: inner.Maintenance,
	}
}

func newWrappedEtcdClientWithKeyPrefix(inner *clientv3.Client, prefix string) EtcdClient {
	ctx, cancel := context.WithCancel(inner.Ctx())
	return &wrappedEtcdClient{
		inner:  inner,
		prefix: prefix,
		ctx:    ctx,
		cancel: cancel,

		Cluster:     inner.Cluster,
		Auth:        inner.Auth,
		Maintenance: inner.Maintenance,

		KV:      namespace.NewKV(inner.KV, prefix),
		Lease:   wrapLeaseClient(namespace.NewLease(inner.Lease, prefix)),
		Watcher: wrapWatchClient(namespace.NewWatcher(inner.Watcher, prefix)),
	}
}

func (c *wrappedEtcdClient) Ctx() context.Context {
	return c.ctx
}

func (c *wrappedEtcdClient) Endpoints() []string {
	return c.inner.Endpoints()
}

func (c *wrappedEtcdClient) ActiveConnection() *grpc.ClientConn {
	return c.inner.ActiveConnection()
}

func (c *wrappedEtcdClient) Close() error {
	c.cancel()
	c.Lease.Close()
	c.Watcher.Close()
	return nil
}

type wrappedLeaseClient struct {
	sync.Mutex
	leaseIDs map[clientv3.LeaseID]struct{}
	inner    clientv3.Lease
}

func wrapLeaseClient(inner clientv3.Lease) clientv3.Lease {
	return &wrappedLeaseClient{
		inner:    inner,
		leaseIDs: make(map[clientv3.LeaseID]struct{}),
	}
}

func (c *wrappedLeaseClient) Grant(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error) {
	res, err := c.inner.Grant(ctx, ttl)
	if err != nil {
		return res, err
	}
	c.Lock()
	defer c.Unlock()
	c.leaseIDs[res.ID] = struct{}{}
	return res, nil
}

func (c *wrappedLeaseClient) Revoke(ctx context.Context, id clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error) {
	res, err := c.inner.Revoke(ctx, id)
	if err != nil {
		return res, err
	}
	c.Lock()
	defer c.Unlock()
	delete(c.leaseIDs, id)
	return res, nil
}

func (c *wrappedLeaseClient) TimeToLive(ctx context.Context, id clientv3.LeaseID, opts ...clientv3.LeaseOption) (*clientv3.LeaseTimeToLiveResponse, error) {
	return c.inner.TimeToLive(ctx, id, opts...)
}

func (c *wrappedLeaseClient) Leases(ctx context.Context) (*clientv3.LeaseLeasesResponse, error) {
	resp, err := c.inner.Leases(ctx)
	if err != nil {
		return resp, err
	}

	c.Lock()
	defer c.Unlock()
	// Filter out leases that are not in our map
	filteredLeases := make([]clientv3.LeaseStatus, 0, len(resp.Leases))
	for _, lease := range resp.Leases {
		if _, ok := c.leaseIDs[lease.ID]; ok {
			filteredLeases = append(filteredLeases, lease)
		}
	}
	resp.Leases = filteredLeases
	return resp, nil
}

func (c *wrappedLeaseClient) KeepAlive(ctx context.Context, id clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	return c.inner.KeepAlive(ctx, id)
}

func (c *wrappedLeaseClient) KeepAliveOnce(ctx context.Context, id clientv3.LeaseID) (*clientv3.LeaseKeepAliveResponse, error) {
	return c.inner.KeepAliveOnce(ctx, id)
}

func (c *wrappedLeaseClient) Close() error {
	c.Lock()
	defer c.Unlock()
	for id := range c.leaseIDs {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
		_, err := c.inner.Revoke(ctx, id)
		cancel()
		if err != nil {
			log.Warn("failed to revoke lease", zap.Int64("lease-id", int64(id)), zap.Error(err))
		}
	}
	c.leaseIDs = make(map[clientv3.LeaseID]struct{})
	return nil
}

type wrappedWatchClient struct {
	inner clientv3.Watcher
	sync.Mutex
	cancels []context.CancelFunc
}

func wrapWatchClient(inner clientv3.Watcher) *wrappedWatchClient {
	return &wrappedWatchClient{
		inner: inner,
	}
}

func (c *wrappedWatchClient) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	c.Lock()
	ctx, cancel := context.WithCancel(ctx)
	c.cancels = append(c.cancels, cancel)
	c.Unlock()
	return c.inner.Watch(ctx, key, opts...)
}

func (c *wrappedWatchClient) RequestProgress(ctx context.Context) error {
	return c.inner.RequestProgress(ctx)
}

func (c *wrappedWatchClient) Close() error {
	c.Lock()
	defer c.Unlock()
	for _, cancel := range c.cancels {
		cancel()
	}
	c.cancels = nil
	return nil
}
