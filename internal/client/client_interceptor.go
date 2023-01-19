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
	"sync/atomic"
	"time"

	"github.com/tikv/client-go/v2/internal/resourcecontrol"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/tikvrpc/interceptor"
	resourceControlClient "github.com/tikv/pd/pkg/mcs/resource_manager/client"
)

func init() {
	resourceControlSwitch.Store(false)
	resourceControlInterceptor = nil
}

var _ Client = interceptedClient{}

type interceptedClient struct {
	Client
}

// NewInterceptedClient creates a Client which can execute interceptor.
func NewInterceptedClient(client Client) Client {
	return interceptedClient{client}
}

func (r interceptedClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	// Build the resource control interceptor.
	resourceGroupName := req.GetResourceGroupName()
	var finalInterceptor interceptor.RPCInterceptor = buildResourceControlInterceptor(ctx, req, resourceGroupName)
	// Chain the interceptors if there are multiple interceptors.
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

var (
	resourceControlSwitch      atomic.Value
	resourceControlInterceptor resourceControlClient.ResourceGroupKVInterceptor
)

// EnableResourceControl enables the resource control.
func EnableResourceControl() {
	resourceControlSwitch.Store(true)
}

// DisableResourceControl disables the resource control.
func DisableResourceControl() {
	resourceControlSwitch.Store(false)
}

// SetResourceControlInterceptor sets the interceptor for resource control.
func SetResourceControlInterceptor(interceptor resourceControlClient.ResourceGroupKVInterceptor) {
	resourceControlInterceptor = interceptor
}

// buildResourceControlInterceptor builds a resource control interceptor with
// the given resource group name.
func buildResourceControlInterceptor(
	ctx context.Context,
	req *tikvrpc.Request,
	resourceGroupName string,
) interceptor.RPCInterceptor {
	if !resourceControlSwitch.Load().(bool) {
		return nil
	}
	// When the group name is empty or "default", we don't need to
	// perform the resource control.
	if len(resourceGroupName) == 0 || resourceGroupName == "default" {
		return nil
	}
	// No resource group interceptor is set.
	if resourceControlInterceptor == nil {
		return nil
	}
	// Make the request info.
	reqInfo := resourcecontrol.MakeRequestInfo(req)
	// Build the interceptor.
	return func(next interceptor.RPCInterceptorFunc) interceptor.RPCInterceptorFunc {
		return func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
			err := resourceControlInterceptor.OnRequestWait(ctx, resourceGroupName, reqInfo)
			if err != nil {
				return nil, err
			}
			resp, err := next(target, req)
			if resp != nil {
				respInfo := resourcecontrol.MakeResponseInfo(resp)
				resourceControlInterceptor.OnResponse(ctx, resourceGroupName, reqInfo, respInfo)
			}
			return resp, err
		}
	}
}
