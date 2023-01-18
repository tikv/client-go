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

	"github.com/tikv/client-go/v2/internal/resource_control"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/tikvrpc/interceptor"
	"github.com/tikv/pd/pkg/mcs/resource_manager/client"
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
	// Build the resource control interceptor if there is one and the resource group name is given.
	var rcInterceptor interceptor.RPCInterceptor
	resourceGroupName := req.GetResourceGroupName()
	if resourceControlInterceptor != nil && len(resourceGroupName) > 0 {
		rcInterceptor = func(next interceptor.RPCInterceptorFunc) interceptor.RPCInterceptorFunc {
			return func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
				reqInfo := resource_control.MakeRequestInfo(req)
				err := resourceControlInterceptor.OnRequestWait(ctx, resourceGroupName, reqInfo)
				if err != nil {
					return nil, err
				}
				resp, err := next(target, req)
				if resp != nil {
					respInfo := resource_control.MakeResponseInfo(resp)
					resourceControlInterceptor.OnResponse(ctx, resourceGroupName, reqInfo, respInfo)
				}
				return resp, err
			}
		}
	}
	// Chain the interceptors if there are multiple interceptors.
	var finalInterceptor interceptor.RPCInterceptor
	if rcInterceptor != nil {
		finalInterceptor = rcInterceptor
	}
	if it := interceptor.GetRPCInterceptorFromCtx(ctx); it != nil {
		if finalInterceptor != nil {
			finalInterceptor = interceptor.ChainRPCInterceptors(finalInterceptor, it)
		} else {
			finalInterceptor = it
		}
	}
	if finalInterceptor != nil {
		return finalInterceptor(func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
			return r.Client.SendRequest(ctx, target, req, timeout)
		})(addr, req)
	}
	return r.Client.SendRequest(ctx, addr, req, timeout)
}

var resourceControlInterceptor client.ResourceGroupKVInterceptor

// SetupResourceControlInterceptor sets the interceptor for resource control.
func SetupResourceControlInterceptor(interceptor client.ResourceGroupKVInterceptor) {
	resourceControlInterceptor = interceptor
}

// UnsetResourceControlInterceptor un-sets the interceptor for resource control.
func UnsetResourceControlInterceptor() {
	resourceControlInterceptor = nil
}
