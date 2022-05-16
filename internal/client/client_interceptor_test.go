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

package client

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/tikvrpc/interceptor"
)

type emptyClient struct{}

func (c emptyClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	return nil, nil
}

func (c emptyClient) Close() error {
	return nil
}

func (c emptyClient) CloseAddr(addr string) error {
	return nil
}

func TestInterceptedClient(t *testing.T) {
	executed := false
	client := NewInterceptedClient(emptyClient{})
	ctx := interceptor.WithRPCInterceptor(context.Background(), func(next interceptor.RPCInterceptorFunc) interceptor.RPCInterceptorFunc {
		return func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
			executed = true
			return next(target, req)
		}
	})
	_, _ = client.SendRequest(ctx, "", nil, 0)
	assert.True(t, executed)
}
