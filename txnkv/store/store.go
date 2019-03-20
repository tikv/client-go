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

package store

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/errors"
	pd "github.com/pingcap/pd/client"
	log "github.com/sirupsen/logrus"
	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/locate"
	"github.com/tikv/client-go/metrics"
	"github.com/tikv/client-go/retry"
	"github.com/tikv/client-go/rpc"
	"github.com/tikv/client-go/txnkv/latch"
	"github.com/tikv/client-go/txnkv/oracle"
	"github.com/tikv/client-go/txnkv/oracle/oracles"
)

// update oracle's lastTS every 2000ms.
var oracleUpdateInterval = 2000

// TiKVStore contains methods to interact with a TiKV cluster.
type TiKVStore struct {
	clusterID    uint64
	uuid         string
	oracle       oracle.Oracle
	client       rpc.Client
	pdClient     pd.Client
	regionCache  *locate.RegionCache
	lockResolver *LockResolver
	txnLatches   *latch.LatchesScheduler
	etcdAddrs    []string
	tlsConfig    *tls.Config

	spkv      SafePointKV
	safePoint uint64
	spTime    time.Time
	spMutex   sync.RWMutex  // this is used to update safePoint and spTime
	closed    chan struct{} // this is used to nofity when the store is closed
}

// NewStore creates a TiKVStore instance.
func NewStore(pdAddrs []string, security config.Security) (*TiKVStore, error) {
	pdCli, err := pd.NewClient(pdAddrs, pd.SecurityOption{
		CAPath:   security.SSLCA,
		CertPath: security.SSLCert,
		KeyPath:  security.SSLKey,
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	oracle, err := oracles.NewPdOracle(pdCli, time.Duration(oracleUpdateInterval)*time.Millisecond)
	if err != nil {
		return nil, errors.Trace(err)
	}

	tlsConfig, err := security.ToTLSConfig()
	if err != nil {
		return nil, errors.Trace(err)
	}

	spkv, err := NewEtcdSafePointKV(pdAddrs, tlsConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}

	clusterID := pdCli.GetClusterID(context.TODO())

	store := &TiKVStore{
		clusterID:   clusterID,
		uuid:        fmt.Sprintf("tikv-%d", clusterID),
		oracle:      oracle,
		client:      rpc.NewRPCClient(security),
		pdClient:    &locate.CodecPDClient{Client: pdCli},
		regionCache: locate.NewRegionCache(pdCli),
		etcdAddrs:   pdAddrs,
		tlsConfig:   tlsConfig,
		spkv:        spkv,
		spTime:      time.Now(),
		closed:      make(chan struct{}),
	}

	store.lockResolver = newLockResolver(store)

	if config.EnableTxnLocalLatch {
		store.txnLatches = latch.NewScheduler(config.TxnLocalLatchCapacity)
	}

	go store.runSafePointChecker()
	return store, nil
}

// GetLockResolver returns the lock resolver instance.
func (s *TiKVStore) GetLockResolver() *LockResolver {
	return s.lockResolver
}

// GetOracle returns the oracle instance.
func (s *TiKVStore) GetOracle() oracle.Oracle {
	return s.oracle
}

// GetRegionCache returns the region cache instance.
func (s *TiKVStore) GetRegionCache() *locate.RegionCache {
	return s.regionCache
}

// GetRPCClient returns the rpc client instance.
func (s *TiKVStore) GetRPCClient() rpc.Client {
	return s.client
}

// GetTxnLatches returns the latch scheduler instance.
func (s *TiKVStore) GetTxnLatches() *latch.LatchesScheduler {
	return s.txnLatches
}

// GetSnapshot creates a snapshot for read.
func (s *TiKVStore) GetSnapshot(ts uint64) *TiKVSnapshot {
	return newTiKVSnapshot(s, ts)
}

// SendReq sends a request to TiKV server.
func (s *TiKVStore) SendReq(bo *retry.Backoffer, req *rpc.Request, regionID locate.RegionVerID, timeout time.Duration) (*rpc.Response, error) {
	sender := rpc.NewRegionRequestSender(s.regionCache, s.client)
	return sender.SendReq(bo, req, regionID, timeout)
}

// Closed returns a channel that will be closed when TiKVStore is closed.
func (s *TiKVStore) Closed() <-chan struct{} {
	return s.closed
}

// Close stops the TiKVStore instance and releases resources.
func (s *TiKVStore) Close() error {
	s.oracle.Close()
	s.pdClient.Close()

	close(s.closed)
	if err := s.client.Close(); err != nil {
		return errors.Trace(err)
	}

	if s.txnLatches != nil {
		s.txnLatches.Close()
	}
	return nil
}

// GetTimestampWithRetry queries PD for a new timestamp.
func (s *TiKVStore) GetTimestampWithRetry(bo *retry.Backoffer) (uint64, error) {
	for {
		startTS, err := s.oracle.GetTimestamp(bo.GetContext())
		if err == nil {
			return startTS, nil
		}
		err = bo.Backoff(retry.BoPDRPC, errors.Errorf("get timestamp failed: %v", err))
		if err != nil {
			return 0, errors.Trace(err)
		}
	}
}

func (s *TiKVStore) runSafePointChecker() {
	d := gcSafePointUpdateInterval
	for {
		select {
		case spCachedTime := <-time.After(d):
			cachedSafePoint, err := loadSafePoint(s.spkv, GcSavedSafePoint)
			if err == nil {
				metrics.LoadSafepointCounter.WithLabelValues("ok").Inc()
				s.spMutex.Lock()
				s.safePoint, s.spTime = cachedSafePoint, spCachedTime
				s.spMutex.Unlock()
				d = gcSafePointUpdateInterval
			} else {
				metrics.LoadSafepointCounter.WithLabelValues("fail").Inc()
				log.Errorf("fail to load safepoint from pd: %v", err)
				d = gcSafePointQuickRepeatInterval
			}
		case <-s.Closed():
			return
		}
	}
}

// CheckVisibility checks if it is safe to read using startTS (the startTS should
//  be greater than current GC safepoint).
func (s *TiKVStore) CheckVisibility(startTS uint64) error {
	s.spMutex.RLock()
	cachedSafePoint := s.safePoint
	cachedTime := s.spTime
	s.spMutex.RUnlock()
	diff := time.Since(cachedTime)

	if diff > (GcSafePointCacheInterval - gcCPUTimeInaccuracyBound) {
		return errors.Trace(ErrPDServerTimeout)
	}

	if startTS < cachedSafePoint {
		return errors.Trace(ErrStartTSFallBehind)
	}

	return nil
}
