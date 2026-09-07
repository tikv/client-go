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
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/txnkv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

type constructorConnectionStats struct {
	active atomic.Int64
	opened atomic.Int64
}

func (s *constructorConnectionStats) TagRPC(c context.Context, _ *stats.RPCTagInfo) context.Context {
	return c
}
func (s *constructorConnectionStats) HandleRPC(context.Context, stats.RPCStats) {}
func (s *constructorConnectionStats) TagConn(c context.Context, _ *stats.ConnTagInfo) context.Context {
	return c
}
func (s *constructorConnectionStats) HandleConn(_ context.Context, event stats.ConnStats) {
	switch event.(type) {
	case *stats.ConnBegin:
		s.active.Add(1)
		s.opened.Add(1)
	case *stats.ConnEnd:
		s.active.Add(-1)
	}
}

type constructorPD struct {
	pdpb.UnimplementedPDServer
	endpoint string
}

func (s *constructorPD) GetMembers(context.Context, *pdpb.GetMembersRequest) (*pdpb.GetMembersResponse, error) {
	member := &pdpb.Member{Name: "fake-pd", MemberId: 1, ClientUrls: []string{s.endpoint}}
	return &pdpb.GetMembersResponse{Header: &pdpb.ResponseHeader{ClusterId: 42}, Members: []*pdpb.Member{member}, Leader: member}, nil
}
func (s *constructorPD) GetClusterInfo(context.Context, *pdpb.GetClusterInfoRequest) (*pdpb.GetClusterInfoResponse, error) {
	return &pdpb.GetClusterInfoResponse{Header: &pdpb.ResponseHeader{ClusterId: 42}, ServiceModes: []pdpb.ServiceMode{pdpb.ServiceMode_PD_SVC_MODE}}, nil
}
func (s *constructorPD) Tso(stream pdpb.PD_TsoServer) error {
	for {
		request, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		err = stream.Send(&pdpb.TsoResponse{Header: &pdpb.ResponseHeader{ClusterId: 42}, Count: request.Count, Timestamp: &pdpb.Timestamp{Physical: time.Now().UnixMilli(), Logical: 1}})
		if err != nil {
			return err
		}
	}
}

type constructorKeyspace struct {
	keyspacepb.UnimplementedKeyspaceServer
	calls atomic.Int64
}

func (s *constructorKeyspace) LoadKeyspace(context.Context, *keyspacepb.LoadKeyspaceRequest) (*keyspacepb.LoadKeyspaceResponse, error) {
	s.calls.Add(1)
	return nil, status.Error(codes.NotFound, "injected keyspace lookup failure")
}

func TestNewClientClosesConnectionsOnError(t *testing.T) {
	for _, test := range []struct {
		name       string
		api        kvrpcpb.APIVersion
		wantLookup bool
	}{
		{"keyspace lookup", kvrpcpb.APIVersion_V2, true},
		{"invalid API version", kvrpcpb.APIVersion(999), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			tracker := &constructorConnectionStats{}
			keyspace := &constructorKeyspace{}
			server := grpc.NewServer(grpc.StatsHandler(tracker))
			endpoint := "http://" + listener.Addr().String()
			pdpb.RegisterPDServer(server, &constructorPD{endpoint: endpoint})
			keyspacepb.RegisterKeyspaceServer(server, keyspace)
			go func() { _ = server.Serve(listener) }()
			t.Cleanup(server.Stop)
			for i := 0; i < 3; i++ {
				client, err := txnkv.NewClient([]string{endpoint}, txnkv.WithAPIVersion(test.api), txnkv.WithKeyspace("missing-keyspace"))
				require.Error(t, err)
				require.Nil(t, client)
				if test.wantLookup {
					require.Contains(t, err.Error(), "injected keyspace lookup failure")
				} else {
					require.Contains(t, err.Error(), "unknown api version")
				}
				require.Eventually(t, func() bool { return tracker.active.Load() == 0 }, 5*time.Second, 10*time.Millisecond, "failed constructor retained PD connections")
			}
			require.GreaterOrEqual(t, tracker.opened.Load(), int64(3))
			if test.wantLookup {
				require.Equal(t, int64(3), keyspace.calls.Load())
			}
		})
	}
}
