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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tikv/client-go/v2/tikvrpc"
)

func TestInterceptor(t *testing.T) {
	chain := NewRPCInterceptorChain()
	manager := MockInterceptorManager{}
	it := chain.
		Link(manager.CreateMockInterceptor("INTERCEPTOR-1")).
		Link(manager.CreateMockInterceptor("INTERCEPTOR-2")).
		Build()
	_, _ = it(func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
		return nil, nil
	})("", nil)
	assert.Equal(t, 2, manager.BeginCount())
	assert.Equal(t, 2, manager.EndCount())
	execLog := manager.ExecLog()
	assert.Len(t, execLog, 2)
	assert.Equal(t, "INTERCEPTOR-1", execLog[0])
	assert.Equal(t, "INTERCEPTOR-2", execLog[1])
}
