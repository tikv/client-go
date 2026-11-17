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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/internal/apicodec"
	"github.com/tikv/client-go/v2/internal/client/mockserver"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util/async"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

type kvTestCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	path string
}

type drainingKVServer struct {
	mockserver.MockServer
	keyspaceID string
	started    chan struct{}
	finish     chan struct{}
	once       sync.Once
}

func (s *drainingKVServer) wait(ctx context.Context) error {
	s.once.Do(func() { close(s.started) })
	select {
	case <-s.finish:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *drainingKVServer) KvGet(ctx context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if got := md.Get(keyspaceIdMetadataKey); s.keyspaceID != "" && (len(got) != 1 || got[0] != s.keyspaceID) {
		return nil, fmt.Errorf("unexpected keyspace metadata: %v", got)
	}
	_, key, err := apicodec.DecodeKey(req.Key, req.GetContext().GetApiVersion())
	if err != nil {
		return nil, err
	}
	if string(key) == "hold" {
		if err := s.wait(ctx); err != nil {
			return nil, err
		}
	}
	return &kvrpcpb.GetResponse{Value: req.Key}, nil
}

func (s *drainingKVServer) BatchCommands(stream tikvpb.Tikv_BatchCommandsServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		resp := &tikvpb.BatchCommandsResponse{RequestIds: req.RequestIds}
		for _, entry := range req.Requests {
			value, err := s.KvGet(stream.Context(), entry.GetGet())
			if err != nil {
				return err
			}
			resp.Responses = append(resp.Responses, &tikvpb.BatchCommandsResponse_Response{Cmd: &tikvpb.BatchCommandsResponse_Response_Get{Get: value}})
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

func (s *drainingKVServer) CoprocessorStream(_ *coprocessor.Request, stream tikvpb.Tikv_CoprocessorStreamServer) error {
	if err := stream.Send(&coprocessor.Response{Data: []byte("first")}); err != nil {
		return err
	}
	if err := s.wait(stream.Context()); err != nil {
		return err
	}
	return stream.Send(&coprocessor.Response{Data: []byte("last")})
}

func newDrainServer(t *testing.T, ca kvTestCA) (*drainingKVServer, string) {
	s := &drainingKVServer{started: make(chan struct{}), finish: make(chan struct{})}
	return s, startKVTLSServer(t, ca.issue(t, "trusted-server", true), s)
}

func kvGet(key string) *tikvrpc.Request {
	return tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte(key)})
}

func awaitKVSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for signal")
	}
}

func rotateKV(t *testing.T, cli *RPCClient, ca kvTestCA) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	sec := config.NewSecurity(ca.path, "", "", nil)
	sec.KVServerVerifyCNMode = "enforce"
	sec.KVServerVerifyCN = []string{"trusted-server"}
	require.NoError(t, cli.UpdateKVServerSecurity(ctx, sec))
}

func requireKVOpen(t *testing.T, pool *connPool) {
	t.Helper()
	select {
	case <-pool.done:
		t.Fatal("drain closed an in-flight request")
	default:
	}
}

func TestKVServerSecurityDrain(t *testing.T) {
	for _, mode := range []string{"unary", "batch", "async", "cop-stream"} {
		for _, disable := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/off=%t", mode, disable), func(t *testing.T) {
				defer config.UpdateGlobal(func(c *config.Config) {
					c.TiKVClient.GrpcConnectionCount = 2
					if mode == "unary" {
						c.TiKVClient.MaxBatchSize = 0
					} else {
						c.TiKVClient.MaxBatchSize = 128
					}
				})()
				ca := newKVTestCA(t)
				srv, addr := newDrainServer(t, ca)
				security := config.NewSecurity(ca.path, "", "", nil)
				if disable {
					security.KVServerVerifyCNMode, security.KVServerVerifyCN = "enforce", []string{"trusted-server"}
				}
				opts := []Opt{WithSecurity(security)}
				// API v2 does not support CopStream response decoding.
				if mode != "cop-stream" {
					srv.keyspaceID = "42"
					codec, err := apicodec.NewCodecV2(apicodec.ModeTxn, &keyspacepb.KeyspaceMeta{
						Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 42},
					})
					require.NoError(t, err)
					opts = append(opts, WithCodec(codec))
				}
				cli := NewRPCClient(opts...)
				defer cli.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
				defer cancel()
				result := make(chan error, 1)
				var stream *tikvrpc.CopStreamResponse
				switch mode {
				case "async":
					rl := async.NewRunLoop()
					cli.SendRequestAsync(ctx, addr, kvGet("hold"), async.NewCallback(rl, func(_ *tikvrpc.Response, err error) { result <- err }))
					go func() { _, _ = rl.Exec(ctx) }()
				case "cop-stream":
					resp, err := cli.SendRequest(ctx, addr, tikvrpc.NewRequest(tikvrpc.CmdCopStream, &coprocessor.Request{}), 5*time.Second)
					require.NoError(t, err)
					stream = resp.Resp.(*tikvrpc.CopStreamResponse)
					defer stream.Close()
					require.Equal(t, "first", string(stream.Data))
				default:
					go func() { _, err := cli.SendRequest(ctx, addr, kvGet("hold"), 5*time.Second); result <- err }()
				}
				awaitKVSignal(t, srv.started)
				old, err := cli.getConnPool(addr, true)
				require.NoError(t, err)
				if disable {
					security.KVServerVerifyCNMode, security.KVServerVerifyCN = "off", nil
					require.NoError(t, cli.UpdateKVServerSecurity(ctx, security))
				} else {
					rotateKV(t, cli, ca)
				}
				current, err := cli.getConnPool(addr, true)
				require.NoError(t, err)
				require.NotSame(t, old, current)
				require.Greater(t, current.ver, old.ver)
				requireKVOpen(t, old)
				_, err = cli.SendRequest(ctx, addr, kvGet("new"), time.Second)
				require.NoError(t, err)
				// An error associated with the old pool must not close its replacement.
				require.NoError(t, cli.CloseAddrVer(addr, old.ver))
				requireKVOpen(t, current)
				close(srv.finish)
				if stream != nil {
					resp, err := stream.Recv()
					require.NoError(t, err)
					require.Equal(t, "last", string(resp.Data))
					_, err = stream.Recv()
					require.ErrorIs(t, err, io.EOF)
				} else {
					select {
					case err := <-result:
						require.NoError(t, err)
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
				}
				awaitKVSignal(t, old.done)
				requireKVOpen(t, current)
			})
		}
	}
}

func TestKVServerSecurityDrainQueuedBatch(t *testing.T) {
	defer config.UpdateGlobal(func(c *config.Config) { c.TiKVClient.GrpcConnectionCount = 1; c.TiKVClient.MaxBatchSize = 1 })()
	ca := newKVTestCA(t)
	_, addr := newDrainServer(t, ca)
	entered, resume := make(chan struct{}), make(chan struct{})
	var blockOnce atomic.Bool
	blockOnce.Store(true)
	intercept := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		if method == "/tikvpb.Tikv/BatchCommands" && blockOnce.CompareAndSwap(true, false) {
			close(entered)
			<-resume
		}
		return streamer(ctx, desc, cc, method, opts...)
	}
	cli := NewRPCClient(WithSecurity(config.NewSecurity(ca.path, "", "", nil)), WithGRPCDialOptions(grpc.WithStreamInterceptor(intercept)))
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	result := make(chan error, 2)
	go func() { _, err := cli.SendRequest(ctx, addr, kvGet("building"), 5*time.Second); result <- err }()
	awaitKVSignal(t, entered)
	old, err := cli.getConnPool(addr, true)
	require.NoError(t, err)
	go func() { _, err := cli.SendRequest(ctx, addr, kvGet("queued"), 5*time.Second); result <- err }()
	require.Eventually(t, func() bool { return old.users.Load() == 2 && len(old.batchCommandsCh) == 1 }, time.Second, time.Millisecond)
	require.Zero(t, old.batchCommandsClients[0].sent.Load())
	rotateKV(t, cli, ca)
	requireKVOpen(t, old)
	_, err = cli.SendRequest(ctx, addr, kvGet("new"), time.Second)
	require.NoError(t, err)
	close(resume)
	for range 2 {
		select {
		case err := <-result:
			require.NoError(t, err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	awaitKVSignal(t, old.done)
}

func TestKVServerSecurityRejectedUpdate(t *testing.T) {
	defer config.UpdateGlobal(func(c *config.Config) { c.TiKVClient.GrpcConnectionCount = 1; c.TiKVClient.MaxBatchSize = 0 })()
	ca := newKVTestCA(t)
	sec := config.NewSecurity(ca.path, "", "", nil)
	cli := NewRPCClient(WithSecurity(sec))
	defer cli.Close()
	addrs := []string{
		startKVTLSServer(t, ca.issue(t, "trusted-server", true), &mockserver.MockServer{}),
		startKVTLSServer(t, ca.issue(t, "unauthorized-service", true), &mockserver.MockServer{}),
	}
	old := make([]*connPool, 2)
	for i, addr := range addrs {
		_, err := cli.SendRequest(context.Background(), addr, kvGet("ok"), time.Second)
		require.NoError(t, err)
		old[i], err = cli.getConnPool(addr, false)
		require.NoError(t, err)
	}
	sec.KVServerVerifyCNMode, sec.KVServerVerifyCN = "enforce", []string{"trusted-server"}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	require.Error(t, cli.UpdateKVServerSecurity(ctx, sec))
	require.Empty(t, cli.option.security.KVServerVerifyCNMode)
	for i, addr := range addrs {
		current, err := cli.getConnPool(addr, false)
		require.NoError(t, err)
		require.Same(t, old[i], current)
		requireKVOpen(t, old[i])
		tracked, ok := cli.connMonitor.m.Load(old[i].conns[0].Name)
		require.True(t, ok)
		require.Same(t, old[i].conns[0], tracked)
		_, err = cli.SendRequest(context.Background(), addr, kvGet("still-off"), time.Second)
		require.NoError(t, err)
	}
	sec.KVServerVerifyCN = []string{"trusted-server", "unauthorized-service"}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()
	require.NoError(t, cli.UpdateKVServerSecurity(ctx2, sec))
	// Caller-owned slices must not mutate the committed policy.
	sec.KVServerVerifyCN[0] = "changed"
	require.Equal(t, "trusted-server", cli.option.security.KVServerVerifyCN[0])
	for _, pool := range old {
		awaitKVSignal(t, pool.done)
	}
	addr := startKVTLSServer(t, ca.issue(t, "new-denied-server", true), &mockserver.MockServer{})
	_, err := cli.SendRequest(context.Background(), addr, kvGet("denied"), time.Second)
	require.Error(t, err)
}

func TestKVServerSecurityConcurrentRotation(t *testing.T) {
	defer config.UpdateGlobal(func(c *config.Config) {
		c.TiKVClient.GrpcConnectionCount = 2
		c.TiKVClient.MaxBatchSize = 128
	})()
	ca := newKVTestCA(t)
	_, addr := newDrainServer(t, ca)
	cli := NewRPCClient(WithSecurity(config.NewSecurity(ca.path, "", "", nil)))
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var workers sync.WaitGroup
	start := make(chan struct{})
	errors := make(chan error, 4)
	for range 4 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for range 100 {
				if _, err := cli.SendRequest(ctx, addr, kvGet("concurrent"), time.Second); err != nil {
					errors <- err
					return
				}
			}
		}()
	}
	_, err := cli.SendRequest(ctx, addr, kvGet("warm"), time.Second)
	require.NoError(t, err)
	close(start)
	for i := range 8 {
		if i%2 == 0 {
			rotateKV(t, cli, ca)
		} else {
			sec := config.NewSecurity(ca.path, "", "", nil)
			sec.KVServerVerifyCNMode = "off"
			require.NoError(t, cli.UpdateKVServerSecurity(ctx, sec))
		}
	}
	workers.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool {
		cli.RLock()
		defer cli.RUnlock()
		return len(cli.retiredPools) == 0
	}, 3*time.Second, time.Millisecond)
	current, err := cli.getConnPool(addr, true)
	require.NoError(t, err)
	for _, conn := range current.conns {
		tracked, ok := cli.connMonitor.m.Load(conn.Name)
		require.True(t, ok)
		require.Same(t, conn, tracked)
	}
}

func TestKVServerSecurityCloseDuringPreflight(t *testing.T) {
	defer config.UpdateGlobal(func(c *config.Config) { c.TiKVClient.GrpcConnectionCount = 1; c.TiKVClient.MaxBatchSize = 0 })()
	ca := newKVTestCA(t)
	_, addr := newDrainServer(t, ca)
	var block atomic.Bool
	var once sync.Once
	entered := make(chan struct{})
	dial := func(ctx context.Context, addr string) (net.Conn, error) {
		if block.Load() {
			once.Do(func() { close(entered) })
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	}
	sec := config.NewSecurity(ca.path, "", "", nil)
	cli := NewRPCClient(WithSecurity(sec), WithGRPCDialOptions(grpc.WithContextDialer(dial)))
	defer cli.Close()
	_, err := cli.SendRequest(context.Background(), addr, kvGet("warm"), time.Second)
	require.NoError(t, err)
	block.Store(true)
	sec.KVServerVerifyCNMode, sec.KVServerVerifyCN = "enforce", []string{"trusted-server"}
	result := make(chan error, 1)
	go func() { result <- cli.UpdateKVServerSecurity(context.Background(), sec) }()
	awaitKVSignal(t, entered)
	require.NoError(t, cli.Close())
	select {
	case err := <-result:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("Close did not cancel pending security preflight")
	}
}

func TestKVServerSecurityDisableWithoutPreflight(t *testing.T) {
	defer config.UpdateGlobal(func(c *config.Config) { c.TiKVClient.GrpcConnectionCount = 1; c.TiKVClient.MaxBatchSize = 0 })()
	ca := newKVTestCA(t)
	sec := config.NewSecurity(ca.path, "", "", nil)
	sec.KVServerVerifyCNMode, sec.KVServerVerifyCN = "enforce", []string{"trusted-server"}
	cli := NewRPCClient(WithSecurity(sec))
	defer cli.Close()
	// A cached destination need not be reachable for an operator to disable CN checks.
	old, err := cli.getConnPool("127.0.0.1:0", false)
	require.NoError(t, err)
	sec.KVServerVerifyCNMode, sec.KVServerVerifyCN = "off", nil
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, cli.UpdateKVServerSecurity(ctx, sec))
	require.Empty(t, cli.connPools)
	require.Equal(t, "off", cli.option.security.KVServerVerifyCNMode)
	awaitKVSignal(t, old.done)
	// Lazy replacement must actually use off, while normal CA/SAN checks remain.
	for _, validSAN := range []bool{true, false} {
		addr := startKVTLSServer(t, ca.issue(t, "not-in-the-old-list", validSAN), &mockserver.MockServer{})
		_, err = cli.SendRequest(context.Background(), addr, kvGet("after-off"), time.Second)
		if validSAN {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
		}
	}
}

func TestKVServerSecurityDisablePreemptsUpdate(t *testing.T) {
	defer config.UpdateGlobal(func(c *config.Config) { c.TiKVClient.GrpcConnectionCount = 1; c.TiKVClient.MaxBatchSize = 0 })()
	ca := newKVTestCA(t)
	_, addr := newDrainServer(t, ca)
	var block atomic.Bool
	var once sync.Once
	entered := make(chan struct{})
	dial := func(ctx context.Context, addr string) (net.Conn, error) {
		if block.Load() {
			once.Do(func() { close(entered) })
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	}
	sec := config.NewSecurity(ca.path, "", "", nil)
	sec.KVServerVerifyCNMode, sec.KVServerVerifyCN = "enforce", []string{"trusted-server"}
	cli := NewRPCClient(WithSecurity(sec), WithGRPCDialOptions(grpc.WithContextDialer(dial)))
	defer cli.Close()
	_, err := cli.SendRequest(context.Background(), addr, kvGet("warm"), time.Second)
	require.NoError(t, err)
	block.Store(true)
	enableCtx, cancelEnable := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelEnable()
	result := make(chan error, 1)
	go func() { result <- cli.UpdateKVServerSecurity(enableCtx, sec) }()
	awaitKVSignal(t, entered)
	off := sec
	off.KVServerVerifyCNMode, off.KVServerVerifyCN = "off", nil
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, cli.UpdateKVServerSecurity(ctx, off))
	select {
	case err := <-result:
		require.Error(t, err, "superseded enforce must not commit after off")
	case <-time.After(time.Second):
		t.Fatal("off did not cancel the pending preflight")
	}
	require.Equal(t, "off", cli.option.security.KVServerVerifyCNMode)
	block.Store(false)
	_, err = cli.SendRequest(context.Background(), addr, kvGet("after-off"), time.Second)
	require.NoError(t, err)
	// A later, intentional enable is still allowed.
	rotateKV(t, cli, ca)
	_, err = cli.SendRequest(context.Background(), addr, kvGet("after-reenable"), time.Second)
	require.NoError(t, err)
}

// Observe queue admission without sleeps or a test-only hook in the client.
type observedKVUpdateContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

func (c *observedKVUpdateContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.entered) })
	return c.Context.Done()
}

func TestKVServerSecurityDisableInvalidatesQueuedUpdate(t *testing.T) {
	ca := newKVTestCA(t)
	sec := config.NewSecurity(ca.path, "", "", nil)
	cli := NewRPCClient(WithSecurity(sec))
	defer cli.Close()
	require.NoError(t, cli.securityUpdateSem.Acquire(context.Background(), 1))
	unlock := sync.OnceFunc(func() { cli.securityUpdateSem.Release(1) })
	defer unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	observed := &observedKVUpdateContext{Context: ctx, entered: make(chan struct{})}
	enforce := sec
	enforce.KVServerVerifyCNMode, enforce.KVServerVerifyCN = "enforce", []string{"trusted-server"}
	result := make(chan error, 1)
	go func() { result <- cli.UpdateKVServerSecurity(observed, enforce) }()
	awaitKVSignal(t, observed.entered)
	sec.KVServerVerifyCNMode = "off"
	off := make(chan error, 1)
	go func() { off <- cli.UpdateKVServerSecurity(ctx, sec) }()
	select {
	case err := <-off:
		unlock()
		require.NoError(t, err)
	case <-time.After(time.Second):
		unlock()
		<-result
		<-off
		t.Fatal("off waited behind a queued update")
	}
	require.Error(t, <-result)
	require.Equal(t, "off", cli.option.security.KVServerVerifyCNMode)
}

func TestKVServerSecurityDisableDuringBlockingDial(t *testing.T) {
	defer config.UpdateGlobal(func(c *config.Config) { c.TiKVClient.GrpcConnectionCount = 1; c.TiKVClient.MaxBatchSize = 0 })()
	ca := newKVTestCA(t)
	_, addr := newDrainServer(t, ca)
	entered, resume := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	var once sync.Once
	release := func() { once.Do(func() { close(resume) }) }
	defer release()
	dial := func(ctx context.Context, addr string) (net.Conn, error) {
		if calls.Add(1) == 1 {
			close(entered)
			select {
			case <-resume:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	}
	sec := config.NewSecurity(ca.path, "", "", nil)
	sec.KVServerVerifyCNMode, sec.KVServerVerifyCN = "enforce", []string{"trusted-server"}
	// A library consumer can opt into blocking dialing, unlike TiDB's default.
	//nolint:staticcheck // WithBlock intentionally exercises blocking DialContext behavior.
	cli := NewRPCClient(WithSecurity(sec), WithGRPCDialOptions(grpc.WithBlock(), grpc.WithContextDialer(dial)))
	defer cli.Close()
	request := make(chan error, 1)
	go func() {
		_, err := cli.SendRequest(context.Background(), addr, kvGet("first"), 5*time.Second)
		request <- err
	}()
	awaitKVSignal(t, entered)
	sec.KVServerVerifyCNMode, sec.KVServerVerifyCN = "off", nil
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	off := make(chan error, 1)
	go func() { off <- cli.UpdateKVServerSecurity(ctx, sec) }()
	select {
	case err := <-off:
		release()
		require.NoError(t, err)
	case <-time.After(time.Second):
		release()
		<-request
		<-off
		t.Fatal("off waited for normal connection creation")
	}
	require.NoError(t, <-request)
	require.GreaterOrEqual(t, calls.Load(), int32(2), "a pool created with the old policy must be discarded")
	require.Equal(t, "off", cli.option.security.KVServerVerifyCNMode)
}

func TestKVServerSecurityDrainCancellationAndClose(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		t.Run(fmt.Sprint("shutdown=", shutdown), func(t *testing.T) {
			defer config.UpdateGlobal(func(c *config.Config) { c.TiKVClient.GrpcConnectionCount = 1 })()
			ca := newKVTestCA(t)
			srv, addr := newDrainServer(t, ca)
			cli := NewRPCClient(WithSecurity(config.NewSecurity(ca.path, "", "", nil)))
			defer cli.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			resp, err := cli.SendRequest(ctx, addr, tikvrpc.NewRequest(tikvrpc.CmdCopStream, &coprocessor.Request{}), time.Second)
			require.NoError(t, err)
			stream := resp.Resp.(*tikvrpc.CopStreamResponse)
			defer stream.Close()
			awaitKVSignal(t, srv.started)
			old, err := cli.getConnPool(addr, true)
			require.NoError(t, err)
			rotateKV(t, cli, ca)
			requireKVOpen(t, old)
			if shutdown {
				require.NoError(t, cli.Close())
			} else {
				cancel()
			}
			_, err = stream.Recv()
			require.Error(t, err)
			awaitKVSignal(t, old.done)
			require.Eventually(t, func() bool {
				cli.RLock()
				defer cli.RUnlock()
				return len(cli.retiredPools) == 0
			}, time.Second, time.Millisecond)
		})
	}
}

func TestKVServerSecurityAsyncDrainWithoutCallback(t *testing.T) {
	for _, outcome := range []string{"response", "cancel"} {
		t.Run(outcome, func(t *testing.T) {
			defer config.UpdateGlobal(func(c *config.Config) {
				c.TiKVClient.GrpcConnectionCount = 1
				c.TiKVClient.MaxBatchSize = 128
			})()
			ca := newKVTestCA(t)
			srv, addr := newDrainServer(t, ca)
			finish := sync.OnceFunc(func() { close(srv.finish) })
			defer finish()
			sec := config.NewSecurity(ca.path, "", "", nil)
			cli := NewRPCClient(WithSecurity(sec))
			defer cli.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			rl := async.NewRunLoop()
			callbackResult := make(chan error, 1)
			cli.SendRequestAsync(ctx, addr, kvGet("hold"), async.NewCallback(rl, func(_ *tikvrpc.Response, err error) {
				callbackResult <- err
			}))
			awaitKVSignal(t, srv.started)
			old, err := cli.getConnPool(addr, true)
			require.NoError(t, err)
			sec.KVServerVerifyCNMode = "off"
			require.NoError(t, cli.UpdateKVServerSecurity(context.Background(), sec))
			requireKVOpen(t, old)
			if outcome == "cancel" {
				cancel()
			} else {
				finish()
			}
			require.Eventually(t, func() bool { return rl.NumRunnable() == 1 }, time.Second, time.Millisecond)
			stopped, stop := context.WithCancel(context.Background())
			stop()
			n, err := rl.Exec(stopped)
			require.Zero(t, n)
			require.ErrorIs(t, err, context.Canceled)
			require.Len(t, callbackResult, 0)
			select {
			case <-old.done:
			case <-time.After(time.Second):
				t.Fatal("retired pool waits for a callback left in the stopped run loop")
			}
			// Executing the queued callback later must not release the pool again.
			n, err = rl.Exec(context.Background())
			require.NoError(t, err)
			require.Equal(t, 1, n)
			err = <-callbackResult
			if outcome == "cancel" {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.NoError(t, err)
			}
			require.Zero(t, old.users.Load())
		})
	}
}

func TestKVServerSecurityQueuedUpdateCancellation(t *testing.T) {
	ca := newKVTestCA(t)
	sec := config.NewSecurity(ca.path, "", "", nil)
	sec.KVServerVerifyCNMode, sec.KVServerVerifyCN = "enforce", []string{"trusted-server"}
	cli := NewRPCClient(WithSecurity(sec))
	defer cli.Close()
	require.NoError(t, cli.securityUpdateSem.Acquire(context.Background(), 1))
	unlock := sync.OnceFunc(func() { cli.securityUpdateSem.Release(1) })
	defer unlock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	observed := &observedKVUpdateContext{Context: ctx, entered: make(chan struct{})}
	result := make(chan error, 1)
	go func() { result <- cli.UpdateKVServerSecurity(observed, sec) }()
	awaitKVSignal(t, observed.entered)
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("queued update did not honor cancellation")
	}
}

func TestKVServerSecurityCancelBlockingPreflight(t *testing.T) {
	for _, action := range []string{"cancel", "off", "close"} {
		t.Run(action, func(t *testing.T) {
			defer config.UpdateGlobal(func(c *config.Config) {
				c.TiKVClient.GrpcConnectionCount = 1
				c.TiKVClient.MaxBatchSize = 0
			})()
			ca := newKVTestCA(t)
			_, addr := newDrainServer(t, ca)
			var block atomic.Bool
			var once sync.Once
			entered, resume := make(chan struct{}), make(chan struct{})
			release := sync.OnceFunc(func() { close(resume) })
			dial := func(ctx context.Context, addr string) (net.Conn, error) {
				if block.Load() {
					once.Do(func() { close(entered) })
					select {
					case <-ctx.Done():
						return nil, ctx.Err()
					case <-resume:
					}
				}
				return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
			}
			sec := config.NewSecurity(ca.path, "", "", nil)
			//nolint:staticcheck // WithBlock intentionally exercises blocking DialContext behavior.
			cli := NewRPCClient(WithSecurity(sec), WithGRPCDialOptions(grpc.WithBlock(), grpc.WithContextDialer(dial)))
			defer cli.Close()
			defer release()
			_, err := cli.SendRequest(context.Background(), addr, kvGet("warm"), time.Second)
			require.NoError(t, err)
			block.Store(true)
			sec.KVServerVerifyCNMode, sec.KVServerVerifyCN = "enforce", []string{"trusted-server"}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- cli.UpdateKVServerSecurity(ctx, sec) }()
			awaitKVSignal(t, entered)
			switch action {
			case "cancel":
				cancel()
			case "off":
				off := sec
				off.KVServerVerifyCNMode = "off"
				require.NoError(t, cli.UpdateKVServerSecurity(context.Background(), off))
			case "close":
				require.NoError(t, cli.Close())
			}
			select {
			case err := <-result:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(time.Second):
				t.Fatal("canceled update is still blocked in candidate dialing")
			}
		})
	}
}

func newKVTestCA(t *testing.T) kvTestCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	cert := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	require.NoError(t, err)
	cert, err = x509.ParseCertificate(der)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600))
	return kvTestCA{cert, key, path}
}

func (ca kvTestCA) issue(t *testing.T, cn string, validSAN bool) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	cert := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: cn},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	if validSAN {
		cert.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
	} else {
		cert.DNSNames = []string{"wrong-target.example"}
	}
	der, err := x509.CreateCertificate(rand.Reader, cert, ca.cert, &key.PublicKey, ca.key)
	require.NoError(t, err)
	return tls.Certificate{Certificate: [][]byte{der, ca.cert.Raw}, PrivateKey: key}
}

func startKVTLSServer(t *testing.T, cert tls.Certificate, service tikvpb.TikvServer) string {
	t.Helper()
	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}})))
	tikvpb.RegisterTikvServer(srv, service)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// Use a real TLS handshake with a trusted CA and valid SAN for the denied CN.
func TestKVServerCNEnforcement(t *testing.T) {
	defer config.UpdateGlobal(func(c *config.Config) {
		c.TiKVClient.MaxBatchSize = 0
		c.TiKVClient.GrpcConnectionCount = 1
	})()
	ca, otherCA := newKVTestCA(t), newKVTestCA(t)
	for _, tc := range []struct {
		name, mode, cn string
		issuer         kvTestCA
		validSAN, ok   bool
	}{
		{"off-denied-CN", "off", "unauthorized-service", ca, true, true},
		{"enforce-allowed-CN", "enforce", "trusted-server", ca, true, true},
		{"enforce-denied-CN", "enforce", "unauthorized-service", ca, true, false},
		{"enforce-wrong-SAN", "enforce", "trusted-server", ca, false, false},
		{"enforce-untrusted-CA", "enforce", "trusted-server", otherCA, true, false},
		{"off-wrong-SAN", "off", "trusted-server", ca, false, false},
		{"off-untrusted-CA", "off", "trusted-server", otherCA, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sec := config.NewSecurity(ca.path, "", "", nil)
			// Decode the new fields so the same regression runs on the old implementation.
			data, err := json.Marshal(map[string]any{"kv-server-verify-cn-mode": tc.mode, "kv-server-verify-cn": []string{"trusted-server"}})
			require.NoError(t, err)
			require.NoError(t, json.Unmarshal(data, &sec))
			addr := startKVTLSServer(t, tc.issuer.issue(t, tc.cn, tc.validSAN), &mockserver.MockServer{})
			cli := NewRPCClient(WithSecurity(sec))
			t.Cleanup(func() { require.NoError(t, cli.Close()) })
			_, err = cli.SendRequest(context.Background(), addr, tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{}), time.Second)
			if tc.ok {
				require.NoError(t, err)
			} else {
				require.Error(t, err, "invalid server identity must be rejected")
			}
		})
	}
}
