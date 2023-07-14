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
	"sync"
	"sync/atomic"
	"time"

	"github.com/tikv/client-go/v2/internal/resourcecontrol"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/tikvrpc/interceptor"
	"github.com/tikv/client-go/v2/util"
	resourceControlClient "github.com/tikv/pd/client/resource_group/controller"
)

func init() {
	ResourceControlSwitch.Store(false)
}

var _ Client = interceptedClient{}

type interceptedClient struct {
	Client
	ruRuntimeStatsMap *sync.Map
}

// NewInterceptedClient creates a Client which can execute interceptor.
func NewInterceptedClient(client Client, ruRuntimeStatsMap *sync.Map) Client {
	return interceptedClient{client, ruRuntimeStatsMap}
}

func (r interceptedClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	// Build the resource control interceptor.
	var finalInterceptor interceptor.RPCInterceptor = buildResourceControlInterceptor(ctx, req, r.getRURuntimeStats(req.GetStartTS()))
	// Chain the interceptors if there are multiple interceptors.
	if it := interceptor.GetRPCInterceptorFromCtx(ctx); it != nil {
		if finalInterceptor != nil {
			finalInterceptor = interceptor.ChainRPCInterceptors(finalInterceptor, it)
		} else {
			finalInterceptor = it
		}
	}
	if finalInterceptor != nil {
		return finalInterceptor.Wrap(func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
			return r.Client.SendRequest(ctx, target, req, timeout)
		})(addr, req)
	}
	return r.Client.SendRequest(ctx, addr, req, timeout)
}

func (r interceptedClient) getRURuntimeStats(startTS uint64) *util.RURuntimeStats {
	if r.ruRuntimeStatsMap == nil || startTS == 0 {
		return nil
	}
	if v, ok := r.ruRuntimeStatsMap.Load(startTS); ok {
		return v.(*util.RURuntimeStats)
	}
	return nil
}

var (
	// ResourceControlSwitch is used to control whether to enable the resource control.
	ResourceControlSwitch atomic.Value
	// ResourceControlInterceptor is used to build the resource control interceptor.
	ResourceControlInterceptor atomic.Pointer[resourceControlClient.ResourceGroupKVInterceptor]
)

// buildResourceControlInterceptor builds a resource control interceptor with
// the given resource group name.
func buildResourceControlInterceptor(
	ctx context.Context,
	req *tikvrpc.Request,
	ruRuntimeStats *util.RURuntimeStats,
) interceptor.RPCInterceptor {
	if !ResourceControlSwitch.Load().(bool) {
		return nil
	}
	resourceGroupName := req.GetResourceControlContext().GetResourceGroupName()
	// When the group name is empty, we don't need to perform the resource control.
	if len(resourceGroupName) == 0 {
		return nil
	}

	rcInterceptor := ResourceControlInterceptor.Load()
	// No resource group interceptor is set.
	if rcInterceptor == nil {
		return nil
	}
	resourceControlInterceptor := *rcInterceptor

	// Make the request info.
	reqInfo := resourcecontrol.MakeRequestInfo(req)
	// Build the interceptor.
	interceptFn := func(next interceptor.RPCInterceptorFunc) interceptor.RPCInterceptorFunc {
		return func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
			// bypass some internal requests and it's may influence user experience. For example, the
			// request of `alter user password`, totally bypasses the resource control. it's not cost
			// many resources, but it's may influence the user experience.
			if reqInfo.Bypass() {
				return next(target, req)
			}
			consumption, penalty, err := resourceControlInterceptor.OnRequestWait(ctx, resourceGroupName, reqInfo)
			if err != nil {
				return nil, err
			}
			req.GetResourceControlContext().Penalty = penalty
			ruRuntimeStats.Update(consumption)
			resp, err := next(target, req)
			if resp != nil {
				respInfo := resourcecontrol.MakeResponseInfo(resp)
				consumption, err = resourceControlInterceptor.OnResponse(resourceGroupName, reqInfo, respInfo)
				if err != nil {
					return nil, err
				}
				ruRuntimeStats.Update(consumption)
			}
			return resp, err
		}
	}
	return interceptor.NewRPCInterceptor("resource_control", interceptFn)
}
