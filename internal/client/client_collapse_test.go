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
	"fmt"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/internal/txnprotocol"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util/async"
)

type collapseTestTransport struct {
	Client
	calls   atomic.Int32
	release <-chan struct{}
}

func (c *collapseTestTransport) SendRequest(ctx context.Context, _ string, req *tikvrpc.Request, _ time.Duration) (*tikvrpc.Response, error) {
	c.calls.Add(1)
	<-c.release
	rpcCtx, err := PrepareContextForTransport(ctx, req)
	if err != nil {
		return nil, err
	}
	tikvrpc.AttachContext(req, rpcCtx)
	return &tikvrpc.Response{Resp: &kvrpcpb.ResolveLockResponse{}}, nil
}

func TestDetachedTxnProtocolContext(t *testing.T) {
	type unrelatedKey struct{}
	declaration := txnprotocol.Declaration{Version: 2}
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), unrelatedKey{}, "request value"))
	ctx = txnprotocol.WithDeclaration(ctx, declaration)
	detached := detachedTxnProtocolContext(ctx)
	cancel()
	got, ok := txnprotocol.DeclarationFrom(detached)
	require.True(t, ok)
	require.Equal(t, declaration, got)
	require.Nil(t, detached.Done())
	require.Nil(t, detached.Value(unrelatedKey{}))
	_, ok = txnprotocol.DeclarationFrom(detachedTxnProtocolContext(context.Background()))
	require.False(t, ok)
}

func TestCollapseSeparatesPhysicalAttempts(t *testing.T) {
	for _, useAsync := range []bool{false, true} {
		for _, variant := range []string{
			"same", "version", "presence", "region", "start", "commit", "resolve_async",
			"address", "forwarding", "peer", "epoch", "client",
		} {
			t.Run(fmt.Sprintf("async=%t/%s", useAsync, variant), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					release := make(chan struct{})
					transport := &collapseTestTransport{release: release}
					first := NewReqCollapse(transport)
					second := first
					if variant == "client" {
						second = NewReqCollapse(transport)
					}
					loop := async.NewRunLoop()
					completed := 0
					errs := make(chan error, 2)
					for i, c := range []Client{first, second} {
						req := tikvrpc.NewRequest(tikvrpc.CmdResolveLock, &kvrpcpb.ResolveLockRequest{StartVersion: 10, CommitVersion: 20})
						req.RegionId = 1
						req.Peer = &metapb.Peer{Id: 2, StoreId: 3}
						req.RegionEpoch = &metapb.RegionEpoch{Version: 1}
						declaration := txnprotocol.Declaration{Version: 1}
						hasDeclaration := true
						addr := "store"
						if i == 1 {
							switch variant {
							case "version":
								declaration.Version = 2
							case "presence":
								hasDeclaration = false
							case "address":
								addr = "other-store"
							case "region":
								req.RegionId++
							case "start":
								req.ResolveLock().StartVersion++
							case "forwarding":
								req.ForwardedHost = "execution-store"
							case "peer":
								req.Peer.Id++
							case "epoch":
								req.RegionEpoch.Version++
							case "commit":
								req.ResolveLock().CommitVersion++
							case "resolve_async":
								req.ResolveLock().IsAsync = true
							}
						}
						ctx := context.Background()
						if hasDeclaration {
							ctx = txnprotocol.WithDeclaration(ctx, declaration)
						}
						if useAsync {
							c.SendRequestAsync(ctx, addr, req, async.NewCallback(loop, func(_ *tikvrpc.Response, err error) {
								require.NoError(t, err)
								completed++
							}))
						} else {
							go func() { _, err := c.SendRequest(ctx, addr, req, time.Second); errs <- err }()
						}
					}
					synctest.Wait()
					wantCalls := int32(2)
					switch variant {
					case "same", "address", "forwarding", "peer", "epoch":
						wantCalls = 1
					}
					require.Equal(t, wantCalls, transport.calls.Load())
					close(release)
					if useAsync {
						for completed < 2 {
							_, err := loop.Exec(context.Background())
							require.NoError(t, err)
						}
					} else {
						require.NoError(t, <-errs)
						require.NoError(t, <-errs)
					}
				})
			})
		}
	}
}
