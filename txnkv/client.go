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

// NewClient creates a txn client with pdAddrs.
func NewClient(pdAddrs []string) (*Client, error) {
	cfg := config.GetGlobalConfig()
	pdClient, err := tikv.NewPDClient(pdAddrs)
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

	s, err := tikv.NewKVStore(uuid, pdClient, spkv, tikv.NewRPCClient(tikv.WithSecurity(cfg.Security)))
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
