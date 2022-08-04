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

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/internal/apicodec"
	"github.com/tikv/client-go/v2/internal/retry"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	pd "github.com/tikv/pd/client"
)

// Client is a txn client.
type Client struct {
	*tikv.KVStore
}

type option struct {
	apiVersion   kvrpcpb.APIVersion
	keyspaceName string
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

func getKeyspaceID(pdCli pd.Client, keyspaceName string) (uint32, error) {
	meta, err := pdCli.LoadKeyspace(context.TODO(), keyspaceName)
	if err != nil {
		return 0, err
	}
	// If keyspace is not enabled, user should not be able to connect.
	if meta.State != keyspacepb.KeyspaceState_ENABLED {
		return 0, errors.Errorf("keyspace %s not enabled", keyspaceName)
	}
	return meta.Id, nil
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
	// Construct codec from options.
	var codec apicodec.Codec
	switch opt.apiVersion {
	case kvrpcpb.APIVersion_V1:
		codec = apicodec.NewCodecV1(apicodec.ModeTxn)
	case kvrpcpb.APIVersion_V1TTL:
		codec = apicodec.NewCodecV1(apicodec.ModeTxn)
	case kvrpcpb.APIVersion_V2:
		var keyspaceID uint32
		if len(opt.keyspaceName) != 0 {
			// If keyspaceName is set, obtain keyspaceID from PD.
			keyspaceID, err = getKeyspaceID(pdClient, opt.keyspaceName)
			if err != nil {
				return nil, err
			}
		} else {
			// If KeyspaceName is unset, use default keyspaceID.
			keyspaceID = apicodec.DefaultKeyspaceID
		}
		codec, err = apicodec.NewCodecV2(apicodec.ModeTxn, keyspaceID)
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.Errorf("unknown api version: %d", opt.apiVersion)
	}

	// Wrapping the pd client with the new codec.
	pdClient = tikv.WrapPDClient(pdClient, codec)

	cfg := config.GetGlobalConfig()
	if err != nil {
		return nil, err
	}
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

	rpcClient := tikv.NewRPCClient(tikv.WithSecurity(cfg.Security), tikv.WithCodec(codec))
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
