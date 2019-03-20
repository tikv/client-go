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

package txnkv

import (
	"context"

	"github.com/pkg/errors"
	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/retry"
	"github.com/tikv/client-go/txnkv/store"
)

type TxnClient struct {
	tikvStore *store.TiKVStore
}

func NewTxnClient(pdAddrs []string, security config.Security) (*TxnClient, error) {
	tikvStore, err := store.NewStore(pdAddrs, security)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &TxnClient{
		tikvStore: tikvStore,
	}, nil
}

func (c *TxnClient) Close() error {
	err := c.tikvStore.Close()
	return errors.Trace(err)
}

func (c *TxnClient) Begin() (*Transaction, error) {
	ts, err := c.GetTS()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return c.BeginWithTS(ts), nil
}

func (c *TxnClient) BeginWithTS(ts uint64) *Transaction {
	return newTransaction(c.tikvStore, ts)
}

func (c *TxnClient) GetTS() (uint64, error) {
	return c.tikvStore.GetTimestampWithRetry(retry.NewBackoffer(context.TODO(), retry.TsoMaxBackoff))
}
