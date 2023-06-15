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
	client := NewInterceptedClient(emptyClient{}, nil)
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
	client := NewInterceptedClient(emptyClient{}, nil)

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
