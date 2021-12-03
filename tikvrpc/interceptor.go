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
	"sync/atomic"
)

// Interceptor is used to decorate the RPC requests to TiKV.
//
// The definition of an interceptor is: Given an InterceptorFunc, we will
// get the decorated InterceptorFunc with additional logic before and after
// the execution of the given InterceptorFunc.
//
// The decorated InterceptorFunc will be executed before and after the real
// RPC request is initiated to TiKV.
//
// We can implement an Interceptor like this:
// ```
// func LogInterceptor(next InterceptorFunc) InterceptorFunc {
//     return func(target string, req *Request) (*Response, error) {
//         log.Println("before")
//         resp, err := next(target, req)
//         log.Println("after")
//         return resp, err
//     }
// }
// txn.SetInterceptor(LogInterceptor)
// ```
//
// Or you want to inject some dependent modules:
// ```
// func GetLogInterceptor(lg *log.Logger) Interceptor {
//     return func(next InterceptorFunc) InterceptorFunc {
//         return func(target string, req *Request) (*Response, error) {
//             lg.Println("before")
//             resp, err := next(target, req)
//             lg.Println("after")
//             return resp, err
//         }
//     }
// }
// txn.SetInterceptor(GetLogInterceptor())
// ```
type Interceptor func(next InterceptorFunc) InterceptorFunc

// InterceptorFunc is a callable function used to initiate a request to TiKV.
// It is mainly used as the parameter and return value of Interceptor.
type InterceptorFunc func(target string, req *Request) (*Response, error)

// InterceptorChain is used to combine multiple Interceptors into one.
// Multiple interceptors will be executed in the order of link time, but are more
// similar to the onion model: The earlier the interceptor is executed, the later
// it will return.
//
// We can use InterceptorChain like this:
// ```
// func Interceptor1(next InterceptorFunc) InterceptorFunc {
//     return func(target string, req *Request) (*Response, error) {
//         fmt.Println("begin-interceptor-1")
//         defer fmt.Println("end-interceptor-1")
//         return next(target, req)
//     }
// }
// func Interceptor2(next InterceptorFunc) InterceptorFunc {
//     return func(target string, req *Request) (*Response, error) {
//         fmt.Println("begin-interceptor-2")
//         defer fmt.Println("end-interceptor-2")
//         return next(target, req)
//     }
// }
// txn.SetInterceptor(NewInterceptorChain().Link(Interceptor1).Link(Interceptor2).Build())
// ```
// Then every time an RPC request is initiated, it will be printed in the following order:
// ```
// begin-interceptor-1
// begin-interceptor-2
// /* do request & respond here */
// end-interceptor-2
// end-interceptor-1
// ```
type InterceptorChain struct {
	chain []Interceptor
}

// NewInterceptorChain creates an empty InterceptorChain.
func NewInterceptorChain() *InterceptorChain {
	return &InterceptorChain{}
}

// Link is used to link the next Interceptor.
// Multiple interceptors will be executed in the order of link time.
func (c *InterceptorChain) Link(it Interceptor) *InterceptorChain {
	c.chain = append(c.chain, it)
	return c
}

// Build merges the previously linked interceptors into one.
func (c *InterceptorChain) Build() Interceptor {
	return func(next InterceptorFunc) InterceptorFunc {
		for n := len(c.chain) - 1; n >= 0; n-- {
			next = c.chain[n](next)
		}
		return next
	}
}

type interceptorCtxKeyType struct{}

var interceptorCtxKey = interceptorCtxKeyType{}

// SetInterceptorIntoCtx is a helper function used to write Interceptor into ctx.
// Different from the behavior of calling context.WithValue() directly, calling
// SetInterceptorIntoCtx multiple times will not bind multiple Interceptors, but
// will replace the original value each time.
// Be careful not to forget to use the returned ctx.
func SetInterceptorIntoCtx(ctx context.Context, interceptor Interceptor) context.Context {
	if v := ctx.Value(interceptorCtxKey); v != nil {
		v.(*atomic.Value).Store(interceptor)
		return ctx
	}
	v := new(atomic.Value)
	v.Store(interceptor)
	return context.WithValue(ctx, interceptorCtxKey, v)
}

// GetInterceptorFromCtx gets the Interceptor bound by the previous call to SetInterceptorIntoCtx,
// and returns nil if there is none.
func GetInterceptorFromCtx(ctx context.Context) Interceptor {
	if v := ctx.Value(interceptorCtxKey); v != nil {
		v := v.(*atomic.Value).Load()
		if interceptor, ok := v.(Interceptor); ok && interceptor != nil {
			return interceptor
		}
	}
	return nil
}

/* Suite for testing */

// MockInterceptorManager can be used to create Interceptor and record the
// number of executions of the created Interceptor.
type MockInterceptorManager struct {
	begin int32
	end   int32
}

// CreateMockInterceptor creates an Interceptor for testing.
func (m *MockInterceptorManager) CreateMockInterceptor() Interceptor {
	return func(next InterceptorFunc) InterceptorFunc {
		return func(target string, req *Request) (*Response, error) {
			atomic.AddInt32(&m.begin, 1)
			defer atomic.AddInt32(&m.end, 1)
			return next(target, req)
		}
	}
}

// Reset clear all counters.
func (m *MockInterceptorManager) Reset() {
	atomic.StoreInt32(&m.begin, 0)
	atomic.StoreInt32(&m.end, 0)
}

// BeginCount gets how many times the previously created Interceptor has been executed.
func (m *MockInterceptorManager) BeginCount() int {
	return int(atomic.LoadInt32(&m.begin))
}

// EndCount gets how many times the previously created Interceptor has been returned.
func (m *MockInterceptorManager) EndCount() int {
	return int(atomic.LoadInt32(&m.end))
}
