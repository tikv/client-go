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

package tikv_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tikv/client-go/v2/tikvrpc/interceptor"
)

func TestInterceptor(t *testing.T) {
	store := NewTestStore(t)
	defer func() {
		assert.NoError(t, store.Close())
	}()
	manager := interceptor.MockInterceptorManager{}

	txn, err := store.Begin()
	txn.SetRPCInterceptor(manager.CreateMockInterceptor("INTERCEPTOR-1"))
	assert.NoError(t, err)
	err = txn.Set([]byte("KEY-1"), []byte("VALUE-1"))
	assert.NoError(t, err)
	err = txn.Commit(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 2, manager.BeginCount())
	assert.Equal(t, 2, manager.EndCount())
	assert.Len(t, manager.ExecLog(), 2)
	assert.Equal(t, "INTERCEPTOR-1", manager.ExecLog()[0])
	assert.Equal(t, "INTERCEPTOR-1", manager.ExecLog()[1])
	manager.Reset()

	txn, err = store.Begin()
	txn.SetRPCInterceptor(manager.CreateMockInterceptor("INTERCEPTOR-2"))
	assert.NoError(t, err)
	value, err := txn.Get(context.Background(), []byte("KEY-1"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("VALUE-1"), value)
	assert.Equal(t, 1, manager.BeginCount())
	assert.Equal(t, 1, manager.EndCount())
	assert.Len(t, manager.ExecLog(), 1)
	assert.Equal(t, "INTERCEPTOR-2", manager.ExecLog()[0])
	manager.Reset()
}
