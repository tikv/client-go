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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/tikv/client-go/v2/internal/resourcecontrol"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/tikvrpc/interceptor"
	resourceControlClient "github.com/tikv/pd/client/resource_group/controller"
)

func init() {
	ResourceControlSwitch.Store(false)
	ResourceControlInterceptor = nil
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
	var finalInterceptor interceptor.RPCInterceptor = buildResourceControlInterceptor(ctx, req, r.getRURuntimeStats(req.GetStartTs()))
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

func (r interceptedClient) getRURuntimeStats(startTS uint64) *RURuntimeStats {
	if r.ruRuntimeStatsMap == nil {
		return nil
	}
	if v, ok := r.ruRuntimeStatsMap.Load(startTS); ok {
		return v.(*RURuntimeStats)
	}
	return nil
}

var (
	// ResourceControlSwitch is used to control whether to enable the resource control.
	ResourceControlSwitch atomic.Value
	// ResourceControlInterceptor is used to build the resource control interceptor.
	ResourceControlInterceptor resourceControlClient.ResourceGroupKVInterceptor
)

// buildResourceControlInterceptor builds a resource control interceptor with
// the given resource group name.
func buildResourceControlInterceptor(
	ctx context.Context,
	req *tikvrpc.Request,
	ruRuntimeStats *RURuntimeStats,
) interceptor.RPCInterceptor {
	if !ResourceControlSwitch.Load().(bool) {
		return nil
	}
	resourceGroupName := req.GetResourceGroupName()
	// When the group name is empty or "default", we don't need to
	// perform the resource control.
	if len(resourceGroupName) == 0 || resourceGroupName == "default" {
		return nil
	}
	// No resource group interceptor is set.
	if ResourceControlInterceptor == nil {
		return nil
	}
	// Make the request info.
	reqInfo := resourcecontrol.MakeRequestInfo(req)
	// Build the interceptor.
	return func(next interceptor.RPCInterceptorFunc) interceptor.RPCInterceptorFunc {
		return func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
			consumption, err := ResourceControlInterceptor.OnRequestWait(ctx, resourceGroupName, reqInfo)
			if err != nil {
				return nil, err
			}
			ruRuntimeStats.Update(consumption)
			resp, err := next(target, req)
			if resp != nil {
				respInfo := resourcecontrol.MakeResponseInfo(resp)
				consumption, err = ResourceControlInterceptor.OnResponse(resourceGroupName, reqInfo, respInfo)
				if err != nil {
					return nil, err
				}
				ruRuntimeStats.Update(consumption)
			}
			return resp, err
		}
	}
}

// RURuntimeStats is the runtime stats collector for RU.
type RURuntimeStats struct {
	readRU  float64
	writeRU float64
}

// Clone implements the RuntimeStats interface.
func (rs *RURuntimeStats) Clone() *RURuntimeStats {
	return &RURuntimeStats{
		readRU:  rs.readRU,
		writeRU: rs.writeRU,
	}
}

// Merge implements the RuntimeStats interface.
func (rs *RURuntimeStats) Merge(other *RURuntimeStats) {
	rs.readRU += other.readRU
	rs.writeRU += other.writeRU
}

// String implements fmt.Stringer interface.
func (rs *RURuntimeStats) String() string {
	return fmt.Sprintf("RRU: %f, WRU: %f", rs.readRU, rs.writeRU)
}

// Update updates the RU runtime stats with the given consumption info.
func (rs *RURuntimeStats) Update(consumption *rmpb.Consumption) {
	if rs == nil || consumption == nil {
		return
	}
	rs.readRU += consumption.RRU
	rs.writeRU += consumption.WRU
}
