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
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/internal/resourcecontrol"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/tikvrpc/interceptor"
	"github.com/tikv/client-go/v2/util/async"
	resourceControlClient "github.com/tikv/pd/client/resource_group/controller"
)

type emptyClient struct{}

func (c emptyClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	return nil, nil
}

func (c emptyClient) SendRequestAsync(ctx context.Context, addr string, req *tikvrpc.Request, cb async.Callback[*tikvrpc.Response]) {
	cb.Invoke(nil, nil)
}

func (c emptyClient) Close() error {
	return nil
}

func (c emptyClient) CloseAddr(addr string) error {
	return nil
}

func (c emptyClient) SetEventListener(listener ClientEventListener) {}

type testResourceControlInterceptor struct{}

func (testResourceControlInterceptor) OnRequestWait(context.Context, string, resourceControlClient.RequestInfo) (*rmpb.Consumption, *rmpb.Consumption, time.Duration, uint32, error) {
	return nil, nil, 0, 0, nil
}

func (testResourceControlInterceptor) OnResponse(string, resourceControlClient.RequestInfo, resourceControlClient.ResponseInfo) (*rmpb.Consumption, error) {
	return nil, nil
}

func (testResourceControlInterceptor) OnResponseWait(context.Context, string, resourceControlClient.RequestInfo, resourceControlClient.ResponseInfo) (*rmpb.Consumption, time.Duration, error) {
	return nil, 0, nil
}

func (testResourceControlInterceptor) IsBackgroundRequest(context.Context, string, string) bool {
	return false
}

func TestInterceptedClient(t *testing.T) {
	executed := false
	client := NewInterceptedClient(emptyClient{})
	ctx := interceptor.WithRPCInterceptor(context.Background(), interceptor.NewRPCInterceptor("test", func(next interceptor.RPCInterceptorFunc) interceptor.RPCInterceptorFunc {
		return func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
			executed = true
			return next(target, req)
		}
	}))
	_, _ = client.SendRequest(ctx, "", &tikvrpc.Request{}, 0)
	assert.True(t, executed)
}

func TestAppendChainedInterceptor(t *testing.T) {
	executed := make([]int, 0, 10)
	client := NewInterceptedClient(emptyClient{})

	mkInterceptorFn := func(i int) interceptor.RPCInterceptor {
		return interceptor.NewRPCInterceptor(fmt.Sprintf("%d", i), func(next interceptor.RPCInterceptorFunc) interceptor.RPCInterceptorFunc {
			return func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
				executed = append(executed, i)
				return next(target, req)
			}
		})
	}

	checkChained := func(it interceptor.RPCInterceptor, count int, expected []int) {
		chain, ok := it.(*interceptor.RPCInterceptorChain)
		assert.True(t, ok)
		assert.Equal(t, chain.Len(), count)

		executed = executed[:0]
		ctx := interceptor.WithRPCInterceptor(context.Background(), it)
		_, _ = client.SendRequest(ctx, "", &tikvrpc.Request{}, 0)
		assert.Equal(t, executed, expected)
	}

	it := mkInterceptorFn(0)
	expected := []int{0}
	for i := 1; i < 3; i++ {
		it = interceptor.ChainRPCInterceptors(it, mkInterceptorFn(i))
		expected = append(expected, i)
		checkChained(it, i+1, expected)
	}

	it2 := interceptor.ChainRPCInterceptors(mkInterceptorFn(3), mkInterceptorFn(4))
	checkChained(it2, 2, []int{3, 4})

	chain := interceptor.ChainRPCInterceptors(it, it2)
	checkChained(chain, 5, []int{0, 1, 2, 3, 4})

	// add duplciated
	chain = interceptor.ChainRPCInterceptors(chain, mkInterceptorFn(1))
	checkChained(chain, 5, []int{0, 2, 3, 4, 1})
}

func TestBypassRUV2FollowsRequestInfoBypass(t *testing.T) {
	req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{})
	req.RequestSource = "xxx_internal_others"
	assert.True(t, resourcecontrol.MakeRequestInfo(req).Bypass())
	assert.False(t, resourcecontrol.MakeRequestInfo(tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{})).Bypass())

	backgroundReq := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{})
	backgroundReq.ResourceControlContext = &kvrpcpb.ResourceControlContext{ResourceGroupName: "rg"}
	backgroundReq.RequestSource = "background-task"
	assert.False(t, resourcecontrol.MakeRequestInfo(backgroundReq).Bypass())
}

func TestGetResourceControlInfoBypassesResourceControl(t *testing.T) {
	require := require.New(t)
	ResourceControlSwitch.Store(true)
	defer ResourceControlSwitch.Store(false)

	interceptor := resourceControlClient.ResourceGroupKVInterceptor(testResourceControlInterceptor{})
	ResourceControlInterceptor.Store(&interceptor)
	defer ResourceControlInterceptor.Store(nil)

	req := &tikvrpc.Request{
		Context: kvrpcpb.Context{
			ResourceControlContext: &kvrpcpb.ResourceControlContext{ResourceGroupName: "rg-1"},
			RequestSource:          "xxx_internal_others",
		},
	}

	resourceGroupName, resourceControlInterceptor, reqInfo := getResourceControlInfo(context.Background(), req)
	require.Equal("rg-1", resourceGroupName)
	require.Nil(resourceControlInterceptor)
	require.NotNil(reqInfo)
	require.True(reqInfo.Bypass())
}
