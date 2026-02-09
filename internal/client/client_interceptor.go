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
	"github.com/tikv/client-go/v2/util"
	resourceControlClient "github.com/tikv/pd/client/resource_group/controller"
)

func init() {
	ResourceControlSwitch.Store(false)
}

var _ Client = interceptedClient{}

type interceptedClient struct {
	Client
}

// NewInterceptedClient creates a Client which can execute interceptor.
func NewInterceptedClient(client Client) Client {
	return interceptedClient{client}
}

func (r interceptedClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (resp *tikvrpc.Response, err error) {
	var ruDetails *util.RUDetails

	resourceGroupName, resourceControlInterceptor, reqInfo := getResourceControlInfo(ctx, req)
	if resourceControlInterceptor != nil {
		consumption, penalty, waitDuration, priority, err := resourceControlInterceptor.OnRequestWait(ctx, resourceGroupName, reqInfo)
		if err != nil {
			return nil, err
		}
		req.GetResourceControlContext().Penalty = penalty
		// override request priority with resource group priority if it's not set.
		// Get the priority at tikv side has some performance issue, so we pass it
		// at client side. See: https://github.com/tikv/tikv/issues/15994 for more details.
		if req.GetResourceControlContext().OverridePriority == 0 {
			req.GetResourceControlContext().OverridePriority = uint64(priority)
		}

		if val := ctx.Value(util.RUDetailsCtxKey); val != nil {
			ruDetails = val.(*util.RUDetails)
			ruDetails.Update(consumption, waitDuration)
		}
	}

	if ctxInterceptor := interceptor.GetRPCInterceptorFromCtx(ctx); ctxInterceptor == nil {
		resp, err = r.Client.SendRequest(ctx, addr, req, timeout)
	} else {
		resp, err = ctxInterceptor.Wrap(func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
			return r.Client.SendRequest(ctx, target, req, timeout)
		})(addr, req)
	}

	if resourceControlInterceptor != nil && resp != nil {
		respInfo := resourcecontrol.MakeResponseInfo(resp)
		consumption, waitDuration, err := resourceControlInterceptor.OnResponseWait(ctx, resourceGroupName, reqInfo, respInfo)
		if err != nil {
			return nil, err
		}
		if ruDetails != nil {
			ruDetails.Update(consumption, waitDuration)
		}
	}

	return resp, err
}

var (
	// ResourceControlSwitch is used to control whether to enable the resource control.
	ResourceControlSwitch atomic.Value
	// ResourceControlInterceptor is used to build the resource control interceptor.
	ResourceControlInterceptor atomic.Pointer[resourceControlClient.ResourceGroupKVInterceptor]
)

func getResourceControlInfo(ctx context.Context, req *tikvrpc.Request) (
	string,
	resourceControlClient.ResourceGroupKVInterceptor,
	*resourcecontrol.RequestInfo,
) {
	resourceGroupName := req.GetResourceControlContext().GetResourceGroupName()
	if len(resourceGroupName) == 0 {
		return "", nil, nil
	}
	if !ResourceControlSwitch.Load().(bool) {
		return "", nil, nil
	}
	rcInterceptor := ResourceControlInterceptor.Load()
	if rcInterceptor == nil {
		return "", nil, nil
	}
	// bypass some internal requests and it's may influence user experience. For example, the
	// request of `alter user password`, totally bypasses the resource control. it's not cost
	// many resources, but it's may influence the user experience.
	// If the resource group has background jobs, we should not record consumption and wait for it.
	// Background jobs will record and report in tikv side.
	resourceControlInterceptor := *rcInterceptor
	if resourceControlInterceptor.IsBackgroundRequest(ctx, resourceGroupName, req.RequestSource) {
		return "", nil, nil
	}
	reqInfo := resourcecontrol.MakeRequestInfo(req)
	if reqInfo.Bypass() {
		return "", nil, nil
	}
	return resourceGroupName, resourceControlInterceptor, reqInfo
}

// buildResourceControlInterceptor builds a resource control interceptor with
// the given resource group name.
//
// Deprecated: embedded in `interceptedClient.SendRequest` directly to reduce overhead.
func buildResourceControlInterceptor( //nolint:unused
	ctx context.Context,
	req *tikvrpc.Request,
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

	ruDetails := ctx.Value(util.RUDetailsCtxKey)

	// Make the request info.
	reqInfo := resourcecontrol.MakeRequestInfo(req)
	// Build the interceptor.
	interceptFn := func(next interceptor.RPCInterceptorFunc) interceptor.RPCInterceptorFunc {
		return func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
			// bypass some internal requests and it's may influence user experience. For example, the
			// request of `alter user password`, totally bypasses the resource control. it's not cost
			// many resources, but it's may influence the user experience.
			// If the resource group has background jobs, we should not record consumption and wait for it.
			// Background jobs will record and report in tikv side.
			if reqInfo.Bypass() || resourceControlInterceptor.IsBackgroundRequest(ctx, resourceGroupName, req.RequestSource) {
				return next(target, req)
			}

			consumption, penalty, waitDuration, priority, err := resourceControlInterceptor.OnRequestWait(ctx, resourceGroupName, reqInfo)
			if err != nil {
				return nil, err
			}
			req.GetResourceControlContext().Penalty = penalty
			// override request priority with resource group priority if it's not set.
			// Get the priority at tikv side has some performance issue, so we pass it
			// at client side. See: https://github.com/tikv/tikv/issues/15994 for more details.
			if req.GetResourceControlContext().OverridePriority == 0 {
				req.GetResourceControlContext().OverridePriority = uint64(priority)
			}
			if ruDetails != nil {
				detail := ruDetails.(*util.RUDetails)
				detail.Update(consumption, waitDuration)
			}

			resp, err := next(target, req)
			if resp != nil {
				respInfo := resourcecontrol.MakeResponseInfo(resp)
				consumption, waitDuration, err = resourceControlInterceptor.OnResponseWait(ctx, resourceGroupName, reqInfo, respInfo)
				if err != nil {
					return nil, err
				}
				if ruDetails != nil {
					detail := ruDetails.(*util.RUDetails)
					detail.Update(consumption, waitDuration)
				}
			}
			return resp, err
		}
	}
	return interceptor.NewRPCInterceptor("resource_control", interceptFn)
}
