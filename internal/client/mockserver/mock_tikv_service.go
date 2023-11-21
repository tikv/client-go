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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/client/mock_tikv_service_test.go
//

package mockserver

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/tikv/client-go/v2/internal/logutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// MockServer is a mock tikv server for testing purpose.
type MockServer struct {
	tikvpb.TikvServer
	grpcServer *grpc.Server
	addr       string
	running    int64 // 0: not running, 1: running
	// metaChecker check the metadata of each request. Now only requests
	// which need redirection set it.
	metaChecker struct {
		sync.Mutex
		check func(context.Context) error
	}
}

// KvGet implements the TikvServer interface.
func (s *MockServer) KvGet(ctx context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	if err := s.checkMetadata(ctx); err != nil {
		return nil, err
	}
	return &kvrpcpb.GetResponse{}, nil
}

// KvPrewrite implements the TikvServer interface.
func (s *MockServer) KvPrewrite(ctx context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	if err := s.checkMetadata(ctx); err != nil {
		return nil, err
	}
	return &kvrpcpb.PrewriteResponse{}, nil
}

// CoprocessorStream implements the TikvServer interface.
func (s *MockServer) CoprocessorStream(req *coprocessor.Request, ss tikvpb.Tikv_CoprocessorStreamServer) error {
	if err := s.checkMetadata(ss.Context()); err != nil {
		return err
	}
	return ss.Send(&coprocessor.Response{})
}

// BatchCommands implements the TikvServer interface.
func (s *MockServer) BatchCommands(ss tikvpb.Tikv_BatchCommandsServer) error {
	if err := s.checkMetadata(ss.Context()); err != nil {
		return err
	}
	for {
		req, err := ss.Recv()
		if err != nil {
			logutil.BgLogger().Error("batch commands receive fail", zap.Error(err))
			return err
		}

		responses := make([]*tikvpb.BatchCommandsResponse_Response, 0, len(req.GetRequestIds()))
		for i := 0; i < len(req.GetRequestIds()); i++ {
			responses = append(responses, &tikvpb.BatchCommandsResponse_Response{
				Cmd: &tikvpb.BatchCommandsResponse_Response_Empty{
					Empty: &tikvpb.BatchCommandsEmptyResponse{},
				},
			})
		}

		err = ss.Send(&tikvpb.BatchCommandsResponse{
			Responses:  responses,
			RequestIds: req.GetRequestIds(),
		})
		if err != nil {
			logutil.BgLogger().Error("batch commands send fail", zap.Error(err))
			return err
		}
	}
}

// SetMetaChecker implements the TikvServer interface.
func (s *MockServer) SetMetaChecker(check func(context.Context) error) {
	s.metaChecker.Lock()
	s.metaChecker.check = check
	s.metaChecker.Unlock()
}

func (s *MockServer) checkMetadata(ctx context.Context) error {
	s.metaChecker.Lock()
	defer s.metaChecker.Unlock()
	if s.metaChecker.check != nil {
		return s.metaChecker.check(ctx)
	}
	return nil
}

// IsRunning returns whether the mock server is running.
func (s *MockServer) IsRunning() bool {
	return atomic.LoadInt64(&s.running) == 1
}

// Addr returns the address of the mock server.
func (s *MockServer) Addr() string {
	return s.addr
}

// Stop stops the mock server.
func (s *MockServer) Stop() {
	s.grpcServer.Stop()
	atomic.StoreInt64(&s.running, 0)
}

// Start starts the mock server.
func (s *MockServer) Start(addr string) int {
	if addr == "" {
		addr = fmt.Sprintf("%s:%d", "127.0.0.1", 0)
	}
	port := -1
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		logutil.BgLogger().Error("can't listen", zap.Error(err))
		logutil.BgLogger().Error("can't start mock tikv service because no available ports")
		return port
	}
	port = lis.Addr().(*net.TCPAddr).Port

	grpcServer := grpc.NewServer(grpc.ConnectionTimeout(time.Minute))
	tikvpb.RegisterTikvServer(grpcServer, s)
	s.grpcServer = grpcServer
	go func() {
		if err = grpcServer.Serve(lis); err != nil {
			logutil.BgLogger().Error(
				"can't serve gRPC requests",
				zap.Error(err),
			)
		}
	}()
	atomic.StoreInt64(&s.running, 1)
	s.addr = fmt.Sprintf("%s:%d", "127.0.0.1", port)
	logutil.BgLogger().Info("mock server started", zap.String("addr", s.addr))
	return port
}

// StartMockTikvService try to start a gRPC server and return the server instance and binded port.
func StartMockTikvService() (*MockServer, int) {
	server := &MockServer{}
	port := server.Start("")
	return server, port
}
