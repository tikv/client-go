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
	"time"

	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/tikvrpc/interceptor"
)

var _ Client = interceptedClient{}

type interceptedClient struct {
	Client
}

// NewInterceptedClient creates a Client which can execute interceptor.
func NewInterceptedClient(client Client) Client {
	return interceptedClient{client}
}

func (r interceptedClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	if it := interceptor.GetRPCInterceptorFromCtx(ctx); it != nil {
		return it(func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
			return r.Client.SendRequest(ctx, target, req, timeout)
		})(addr, req)
	}
	return r.Client.SendRequest(ctx, addr, req, timeout)
}
