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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/test_util.go
//

// Copyright 2017 PingCAP, Inc.
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

package tikv

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/tikv/client-go/v2/internal/apicodec"
	"github.com/tikv/client-go/v2/internal/locate"
	"github.com/tikv/client-go/v2/tikvrpc"
	pd "github.com/tikv/pd/client"
)

// CodecClient warps Client to provide codec encode and decode.
type CodecClient struct {
	Client
	codec apicodec.Codec
}

// SendRequest uses codec to encode request before send, and decode response before return.
func (c *CodecClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	req, err := c.codec.EncodeRequest(req)
	if err != nil {
		return nil, err
	}
	resp, err := c.Client.SendRequest(ctx, addr, req, timeout)
	if err != nil {
		return nil, err
	}
	return c.codec.DecodeResponse(req, resp)
}

// NewTestTiKVStore creates a test store with Option
func NewTestTiKVStore(client Client, pdClient pd.Client, clientHijack func(Client) Client, pdClientHijack func(pd.Client) pd.Client, txnLocalLatches uint, opt ...Option) (*KVStore, error) {
	codec := apicodec.NewCodecV1(apicodec.ModeTxn)
	client = &CodecClient{
		Client: client,
		codec:  codec,
	}
	pdCli := pd.Client(locate.NewCodecPDClient(ModeTxn, pdClient))

	if clientHijack != nil {
		client = clientHijack(client)
	}

	if pdClientHijack != nil {
		pdCli = pdClientHijack(pdCli)
	}

	// Make sure the uuid is unique.
	uid := uuid.New().String()
	spkv := NewMockSafePointKV()
	tikvStore, err := NewKVStore(uid, pdCli, spkv, client, opt...)

	if txnLocalLatches > 0 {
		tikvStore.EnableTxnLocalLatches(txnLocalLatches)
	}

	tikvStore.mock = true
	return tikvStore, err
}
