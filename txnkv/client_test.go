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

package txnkv_test

import (
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/meta_storagepb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/rawkv"
	"github.com/tikv/client-go/v2/txnkv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/stats"
)

var (
	_ io.Closer = txnkv.Client{}
	_ io.Closer = (*txnkv.Client)(nil)
)

func TestClientClosesPDOnKeyspaceError(t *testing.T) {
	for _, tc := range []struct {
		name      string
		newClient func(*testing.T, []string) error
	}{
		{
			name: "raw",
			newClient: func(t *testing.T, addrs []string) error {
				client, err := rawkv.NewClientWithOpts(t.Context(), addrs,
					rawkv.WithAPIVersion(kvrpcpb.APIVersion_V2), rawkv.WithKeyspace("disabled-keyspace"))
				require.Nil(t, client)
				return err
			},
		},
		{
			name: "txn",
			newClient: func(t *testing.T, addrs []string) error {
				client, err := txnkv.NewClient(addrs,
					txnkv.WithAPIVersion(kvrpcpb.APIVersion_V2), txnkv.WithKeyspace("disabled-keyspace"))
				require.Nil(t, client)
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var lc net.ListenConfig
			listener, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
			require.NoError(t, err)
			pdServer := &constructorPDServer{url: "http://" + listener.Addr().String()}
			server := grpc.NewServer(grpc.StatsHandler(pdServer))
			pdpb.RegisterPDServer(server, pdServer)
			keyspacepb.RegisterKeyspaceServer(server, pdServer)
			meta_storagepb.RegisterMetaStorageServer(server, pdServer)
			healthpb.RegisterHealthServer(server, health.NewServer())
			serveErr := make(chan error, 1)
			go func() { serveErr <- server.Serve(listener) }()
			t.Cleanup(func() {
				server.Stop()
				require.NoError(t, <-serveErr)
			})

			err = tc.newClient(t, []string{pdServer.url})
			require.ErrorContains(t, err, "not enabled")
			require.Positive(t, pdServer.opened.Load(), "the constructor must initialize a real PD connection")
			require.Eventually(t, func() bool {
				return pdServer.active.Load() == 0
			}, 5*time.Second, 10*time.Millisecond, "failed initialization must close all PD connections")
		})
	}
}

type constructorPDServer struct {
	pdpb.UnimplementedPDServer
	keyspacepb.UnimplementedKeyspaceServer
	meta_storagepb.UnimplementedMetaStorageServer
	url    string
	opened atomic.Int64
	active atomic.Int64
}

func (s *constructorPDServer) GetMembers(context.Context, *pdpb.GetMembersRequest) (*pdpb.GetMembersResponse, error) {
	member := &pdpb.Member{MemberId: 1, ClientUrls: []string{s.url}}
	return &pdpb.GetMembersResponse{
		Header:  &pdpb.ResponseHeader{ClusterId: 1},
		Members: []*pdpb.Member{member},
		Leader:  member,
	}, nil
}

func (s *constructorPDServer) GetClusterInfo(context.Context, *pdpb.GetClusterInfoRequest) (*pdpb.GetClusterInfoResponse, error) {
	return &pdpb.GetClusterInfoResponse{
		Header:       &pdpb.ResponseHeader{ClusterId: 1},
		ServiceModes: []pdpb.ServiceMode{pdpb.ServiceMode_PD_SVC_MODE},
	}, nil
}

func (s *constructorPDServer) LoadKeyspace(_ context.Context, req *keyspacepb.LoadKeyspaceRequest) (*keyspacepb.LoadKeyspaceResponse, error) {
	return &keyspacepb.LoadKeyspaceResponse{
		Header: &pdpb.ResponseHeader{ClusterId: 1},
		Keyspace: &keyspacepb.KeyspaceMeta{
			Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 1},
			Name:     req.Name,
			State:    keyspacepb.KeyspaceState_DISABLED,
		},
	}, nil
}

func (*constructorPDServer) Get(context.Context, *meta_storagepb.GetRequest) (*meta_storagepb.GetResponse, error) {
	return &meta_storagepb.GetResponse{}, nil
}

func (*constructorPDServer) Tso(stream pdpb.PD_TsoServer) error {
	<-stream.Context().Done()
	return stream.Context().Err()
}

func (*constructorPDServer) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return ctx
}

func (*constructorPDServer) HandleRPC(context.Context, stats.RPCStats) {}

func (*constructorPDServer) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (s *constructorPDServer) HandleConn(_ context.Context, stat stats.ConnStats) {
	switch stat.(type) {
	case *stats.ConnBegin:
		s.opened.Add(1)
		s.active.Add(1)
	case *stats.ConnEnd:
		s.active.Add(-1)
	}
}
