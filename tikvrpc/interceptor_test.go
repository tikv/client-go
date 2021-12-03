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

package tikvrpc

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInterceptorChain(t *testing.T) {
	chain := NewInterceptorChain()
	manager := MockInterceptorManager{}
	it := chain.
		Link(manager.CreateMockInterceptor()).
		Link(manager.CreateMockInterceptor()).
		Build()
	_, _ = it(func(target string, req *Request) (*Response, error) {
		return nil, nil
	})("", nil)
	assert.Equal(t, 2, manager.BeginCount())
	assert.Equal(t, 2, manager.EndCount())
}

func TestGetAndSetInterceptorCtx(t *testing.T) {
	ctx := context.Background()
	assert.Nil(t, GetInterceptorFromCtx(ctx))
	var it1 Interceptor = func(next InterceptorFunc) InterceptorFunc {
		return next
	}
	ctx = SetInterceptorIntoCtx(ctx, it1)
	it2 := GetInterceptorFromCtx(ctx)
	assert.Equal(t, funcKey(it1), funcKey(it2))
	var it3 Interceptor = func(next InterceptorFunc) InterceptorFunc {
		return next
	}
	assert.NotEqual(t, funcKey(it1), funcKey(it3))
	ctx = SetInterceptorIntoCtx(ctx, it3)
	it4 := GetInterceptorFromCtx(ctx)
	assert.Equal(t, funcKey(it3), funcKey(it4))
}

func funcKey(v interface{}) string {
	return fmt.Sprintf("%v", v)
}
