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

package txnkv

import (
	"context"
	"fmt"

	"github.com/tikv/client-go/v2/util"
	"google.golang.org/grpc"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/internal/retry"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
)

// Client is a txn client.
type Client struct {
	*tikv.KVStore
}

type option struct {
	apiVersion      kvrpcpb.APIVersion
	keyspaceName    string
	gRPCDialOptions []grpc.DialOption
}

// ClientOpt is factory to set the client options.
type ClientOpt func(*option)

// WithKeyspace is used to set client's keyspace.
func WithKeyspace(keyspaceName string) ClientOpt {
	return func(opt *option) {
		opt.keyspaceName = keyspaceName
	}
}

// WithAPIVersion is used to set client's apiVersion.
func WithAPIVersion(apiVersion kvrpcpb.APIVersion) ClientOpt {
	return func(opt *option) {
		opt.apiVersion = apiVersion
	}
}

// WithGRPCDialOptions is used to set the grpc.DialOption.
func WithGRPCDialOptions(opts ...grpc.DialOption) ClientOpt {
	return func(o *option) {
		o.gRPCDialOptions = append(o.gRPCDialOptions, opts...)
	}
}

// NewClient creates a txn client with pdAddrs.
func NewClient(pdAddrs []string, opts ...ClientOpt) (*Client, error) {
	// Apply options.
	opt := &option{}
	for _, o := range opts {
		o(opt)
	}
	// Use an unwrapped PDClient to obtain keyspace meta.
	pdClient, err := tikv.NewPDClient(pdAddrs)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	pdClient = util.InterceptedPDClient{Client: pdClient}

	// Construct codec from options.
	var codecCli *tikv.CodecPDClient
	switch opt.apiVersion {
	case kvrpcpb.APIVersion_V1:
		codecCli = tikv.NewCodecPDClient(tikv.ModeTxn, pdClient)
	case kvrpcpb.APIVersion_V2:
		codecCli, err = tikv.NewCodecPDClientWithKeyspace(tikv.ModeTxn, pdClient, opt.keyspaceName)
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.Errorf("unknown api version: %d", opt.apiVersion)
	}

	pdClient = codecCli

	cfg := config.GetGlobalConfig()
	// init uuid
	uuid := fmt.Sprintf("tikv-%v", pdClient.GetClusterID(context.TODO()))
	tlsConfig, err := cfg.Security.ToTLSConfig()
	if err != nil {
		return nil, err
	}

	spkv, err := tikv.NewEtcdSafePointKV(pdAddrs, tlsConfig)
	if err != nil {
		return nil, err
	}

	rpcClient := tikv.NewRPCClient(
		tikv.WithSecurity(cfg.Security),
		tikv.WithGRPCDialOptions(opt.gRPCDialOptions...),
		tikv.WithCodec(codecCli.GetCodec()))

	s, err := tikv.NewKVStore(uuid, pdClient, spkv, rpcClient)
	if err != nil {
		return nil, err
	}
	if cfg.TxnLocalLatches.Enabled {
		s.EnableTxnLocalLatches(cfg.TxnLocalLatches.Capacity)
	}
	return &Client{KVStore: s}, nil
}

// GetTimestamp returns the current global timestamp.
func (c *Client) GetTimestamp(ctx context.Context) (uint64, error) {
	bo := retry.NewBackofferWithVars(ctx, transaction.TsoMaxBackoff, nil)
	startTS, err := c.GetTimestampWithRetry(bo, oracle.GlobalTxnScope)
	if err != nil {
		return 0, err
	}
	return startTS, nil
}
