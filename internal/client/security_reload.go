// Copyright 2026 TiKV Authors
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

package client

import (
	"context"
	"slices"

	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/config"
	"google.golang.org/grpc/connectivity"
)

// UpdateKVServerSecurity preflights enforce policies on fresh connections before
// atomically switching admission. Off cancels pending preflight and invalidates
// older queued updates, immediately retiring active pools without dialing or
// reading TLS files. Subsequent requests build pools lazily with CN checks off.
// Previously admitted calls may finish on retired pools; neither mode waits for
// their completion. A failed update does not publish its candidate policy.
// CA/key paths cannot change. The caller should use a bounded context.
func (c *RPCClient) UpdateKVServerSecurity(ctx context.Context, security config.Security) error {
	if err := security.ValidateKVServerCN(); err != nil {
		return err
	}
	security.KVServerVerifyCN = slices.Clone(security.KVServerVerifyCN)
	if security.KVServerVerifyCNMode != "enforce" {
		return c.disableKVServerCN(ctx, security)
	}
	c.RLock()
	epoch := c.securityUpdateEpoch
	c.RUnlock()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := c.securityUpdateSem.Acquire(ctx, 1); err != nil {
		return err
	}
	defer c.securityUpdateSem.Release(1)
	c.Lock()
	if err := c.validateKVServerSecurityUpdate(ctx, security); err != nil {
		c.Unlock()
		return err
	}
	if epoch != c.securityUpdateEpoch {
		c.Unlock()
		return errors.New("KV server security update superseded by off")
	}
	c.securityUpdateCancel = cancel
	c.Unlock()
	defer func() {
		c.Lock()
		if epoch == c.securityUpdateEpoch {
			c.securityUpdateCancel = nil
		}
		c.Unlock()
	}()
	// Validate files even when the client has no active addresses yet.
	if _, err := security.ToKVServerTLSConfig(); err != nil {
		return err
	}
	candidates := make(map[string]*connPool)
	defer func() {
		for _, pool := range candidates {
			pool.Close()
		}
	}()

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		c.Lock()
		if c.isClosed {
			c.Unlock()
			return errors.New("rpcClient is closed")
		}
		if err := ctx.Err(); err != nil {
			c.Unlock()
			return err
		}
		missing := make(map[string]*connPool)
		for addr, pool := range c.connPools {
			if candidates[addr] == nil {
				missing[addr] = pool
			}
		}
		if len(missing) == 0 {
			// No slow operations in the admission critical section. New addresses
			// discovered during preparation are handled by the next iteration.
			c.option.security = security
			c.securityGeneration++
			for addr, old := range c.connPools {
				pool := candidates[addr]
				delete(candidates, addr)
				c.vers[addr]++
				pool.ver = c.vers[addr]
				pool.monitor = c.connMonitor
				for _, conn := range pool.conns {
					c.connMonitor.AddConn(conn)
				}
				c.connPools[addr] = pool
				c.retirePool(old)
			}
			c.Unlock()
			return nil
		}
		c.Unlock()

		for addr, old := range missing {
			// Candidates use an isolated monitor until committed, so a failed
			// preparation cannot replace/remove active connection metrics.
			pool, err := newConnPool(ctx, uint(len(old.conns)), addr, 0, old.keyspaceID, security,
				&c.idleNotify, old.batchConn != nil, c.option.dialTimeout,
				&connMonitor{}, c.eventListener, c.option.gRPCDialOptions)
			if err != nil {
				return errors.Wrapf(err, "prepare KV server security for %s", addr)
			}
			candidates[addr] = pool
			for _, conn := range pool.conns {
				conn.Connect()
				for state := conn.GetState(); state != connectivity.Ready; state = conn.GetState() {
					if state == connectivity.Shutdown {
						return errors.Errorf("candidate KV connection to %s was closed", addr)
					}
					if !conn.WaitForStateChange(ctx, state) {
						return errors.Wrapf(ctx.Err(), "KV server security preflight failed for %s", addr)
					}
				}
			}
		}
	}
}

// The caller holds RPCClient's admission lock. This validation does no I/O.
func (c *RPCClient) validateKVServerSecurityUpdate(ctx context.Context, security config.Security) error {
	if c.isClosed {
		return errors.New("rpcClient is closed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	previous := c.option.security
	if security.ClusterSSLCA != previous.ClusterSSLCA || security.ClusterSSLCert != previous.ClusterSSLCert || security.ClusterSSLKey != previous.ClusterSSLKey {
		return errors.New("KV server CN hot update cannot change TLS CA, certificate, or key paths")
	}
	return nil
}

func (c *RPCClient) disableKVServerCN(ctx context.Context, security config.Security) error {
	c.Lock()
	defer c.Unlock()
	if err := c.validateKVServerSecurityUpdate(ctx, security); err != nil {
		return err
	}
	if c.securityUpdateCancel != nil {
		c.securityUpdateCancel()
		c.securityUpdateCancel = nil
	}
	c.securityUpdateEpoch++
	c.securityGeneration++
	c.option.security = security
	for addr, pool := range c.connPools {
		delete(c.connPools, addr)
		c.retirePool(pool)
	}
	return nil
}

// The caller has removed pool from admission under RPCClient's lock.
func (c *RPCClient) retirePool(pool *connPool) {
	c.retiredPools[pool] = struct{}{}
	pool.retire()
	go c.finishDraining(pool)
}

func (c *RPCClient) finishDraining(pool *connPool) {
	select {
	case <-pool.drained:
		pool.Close()
	case <-pool.done:
	}
	c.Lock()
	delete(c.retiredPools, pool)
	c.Unlock()
}
