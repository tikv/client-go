// Copyright 2025 TiKV Authors
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

package transaction

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

// spawnKVStore records the functions submitted through Go, and can be told to
// reject them the way a closing store does.
type spawnKVStore struct {
	unimplementedKVStore
	rejectErr error
	wg        sync.WaitGroup
}

func (s *spawnKVStore) Go(f func()) error {
	if s.rejectErr != nil {
		return s.rejectErr
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		f()
	}()
	return nil
}

func TestSpawnKeepsLifecycleHooksBalanced(t *testing.T) {
	rejectErr := errors.New("the store is closed")

	for _, rejected := range []bool{false, true} {
		store := &spawnKVStore{}
		if rejected {
			store.rejectErr = rejectErr
		}

		var pre, post atomic.Int32
		txn := &KVTxn{store: store}
		txn.SetBackgroundGoroutineLifecycleHooks(LifecycleHooks{
			Pre:  func() { pre.Add(1) },
			Post: func() { post.Add(1) },
		})

		var executed atomic.Bool
		err := txn.spawn(func() { executed.Store(true) })
		store.wg.Wait()

		if rejected {
			require.ErrorIs(t, err, rejectErr)
			require.False(t, executed.Load(), "a rejected function must not run")
		} else {
			require.NoError(t, err)
			require.True(t, executed.Load())
		}
		// Post must run even when the goroutine never starts, otherwise the
		// caller of Pre waits for a goroutine that does not exist.
		require.Equal(t, int32(1), pre.Load())
		require.Equal(t, int32(1), post.Load())
	}
}
