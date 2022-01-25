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

package interceptor

import (
	"context"
	"sync/atomic"

	"github.com/tikv/client-go/v2/tikvrpc"
)

// RPCInterceptor is used to decorate the RPC requests to TiKV.
//
// The definition of an interceptor is: Given an RPCInterceptorFunc, we will
// get the decorated RPCInterceptorFunc with additional logic before and after
// the execution of the given RPCInterceptorFunc.
//
// The decorated RPCInterceptorFunc will be executed before and after the real
// RPC request is initiated to TiKV.
//
// We can implement an RPCInterceptor like this:
// ```
// func LogInterceptor(next InterceptorFunc) RPCInterceptorFunc {
//     return func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
//         log.Println("before")
//         resp, err := next(target, req)
//         log.Println("after")
//         return resp, err
//     }
// }
// txn.SetRPCInterceptor(LogInterceptor)
// ```
//
// Or you want to inject some dependent modules:
// ```
// func GetLogInterceptor(lg *log.Logger) RPCInterceptor {
//     return func(next RPCInterceptorFunc) RPCInterceptorFunc {
//         return func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
//             lg.Println("before")
//             resp, err := next(target, req)
//             lg.Println("after")
//             return resp, err
//         }
//     }
// }
// txn.SetRPCInterceptor(GetLogInterceptor())
// ```
//
// NOTE: Interceptor calls may not correspond one-to-one with the underlying gRPC requests.
// This is because there may be some exceptions, such as: request batched, no
// valid connection etc. If you have questions about the execution location of
// RPCInterceptor, please refer to:
//     tikv/kv.go#NewKVStore()
//     internal/client/client_interceptor.go#SendRequest.
type RPCInterceptor func(next RPCInterceptorFunc) RPCInterceptorFunc

// RPCInterceptorFunc is a callable function used to initiate a request to TiKV.
// It is mainly used as the parameter and return value of RPCInterceptor.
type RPCInterceptorFunc func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error)

// RPCInterceptorChain is used to combine multiple interceptors into one.
// Multiple interceptors will be executed in the order of link time, but are more
// similar to the onion model: The earlier the interceptor is executed, the later
// it will return.
//
// We can use RPCInterceptorChain like this:
// ```
// func Interceptor1(next InterceptorFunc) RPCInterceptorFunc {
//     return func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
//         fmt.Println("begin-interceptor-1")
//         defer fmt.Println("end-interceptor-1")
//         return next(target, req)
//     }
// }
// func Interceptor2(next InterceptorFunc) RPCInterceptorFunc {
//     return func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
//         fmt.Println("begin-interceptor-2")
//         defer fmt.Println("end-interceptor-2")
//         return next(target, req)
//     }
// }
// txn.SetRPCInterceptor(NewRPCInterceptorChain().Link(Interceptor1).Link(Interceptor2).Build())
// ```
//
// Then every time an RPC request is initiated, the following text will be printed:
// ```
// begin-interceptor-1
// begin-interceptor-2
// /* do request & respond here */
// end-interceptor-2
// end-interceptor-1
// ```
type RPCInterceptorChain struct {
	chain []RPCInterceptor
}

// NewRPCInterceptorChain creates an empty RPCInterceptorChain.
func NewRPCInterceptorChain() *RPCInterceptorChain {
	return &RPCInterceptorChain{}
}

// Link is used to link the next RPCInterceptor.
// Multiple interceptors will be executed in the order of link time.
func (c *RPCInterceptorChain) Link(it RPCInterceptor) *RPCInterceptorChain {
	c.chain = append(c.chain, it)
	return c
}

// Build merges the previously linked interceptors into one.
func (c *RPCInterceptorChain) Build() RPCInterceptor {
	return func(next RPCInterceptorFunc) RPCInterceptorFunc {
		for n := len(c.chain) - 1; n >= 0; n-- {
			next = c.chain[n](next)
		}
		return next
	}
}

// ChainRPCInterceptors chains multiple RPCInterceptors into one.
// Multiple RPCInterceptors will be executed in the order of their parameters.
// See RPCInterceptorChain for more information.
func ChainRPCInterceptors(its ...RPCInterceptor) RPCInterceptor {
	chain := NewRPCInterceptorChain()
	for _, it := range its {
		chain.Link(it)
	}
	return chain.Build()
}

type interceptorCtxKeyType struct{}

var interceptorCtxKey = interceptorCtxKeyType{}

// WithRPCInterceptor is a helper function used to bind RPCInterceptor with ctx.
func WithRPCInterceptor(ctx context.Context, interceptor RPCInterceptor) context.Context {
	return context.WithValue(ctx, interceptorCtxKey, interceptor)
}

// GetRPCInterceptorFromCtx gets the RPCInterceptor bound by the previous call
// to WithRPCInterceptor, and returns nil if there is none.
func GetRPCInterceptorFromCtx(ctx context.Context) RPCInterceptor {
	if v := ctx.Value(interceptorCtxKey); v != nil {
		return v.(RPCInterceptor)
	}
	return nil
}

/* Suite for testing */

// MockInterceptorManager can be used to create Interceptor and record the
// number of executions of the created Interceptor.
type MockInterceptorManager struct {
	begin   int32
	end     int32
	execLog []string // interceptor name list
}

// NewMockInterceptorManager creates an empty MockInterceptorManager.
func NewMockInterceptorManager() *MockInterceptorManager {
	return &MockInterceptorManager{execLog: []string{}}
}

// CreateMockInterceptor creates an RPCInterceptor for testing.
func (m *MockInterceptorManager) CreateMockInterceptor(name string) RPCInterceptor {
	return func(next RPCInterceptorFunc) RPCInterceptorFunc {
		return func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
			m.execLog = append(m.execLog, name)
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
	m.execLog = []string{}
}

// BeginCount gets how many times the previously created Interceptor has been executed.
func (m *MockInterceptorManager) BeginCount() int {
	return int(atomic.LoadInt32(&m.begin))
}

// EndCount gets how many times the previously created Interceptor has been returned.
func (m *MockInterceptorManager) EndCount() int {
	return int(atomic.LoadInt32(&m.end))
}

// ExecLog gets execution log of all interceptors.
func (m *MockInterceptorManager) ExecLog() []string {
	return m.execLog
}
