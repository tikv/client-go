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

package trace

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestImmediateLoggingExtractor(t *testing.T) {
	// Save original extractor
	originalExtractor := globalImmediateLoggingExtractorFunc.Load()
	defer func() {
		globalImmediateLoggingExtractorFunc.Store(originalExtractor)
	}()

	// Test default behavior (returns false)
	ctx := context.Background()
	require.False(t, ImmediateLoggingEnabled(ctx))

	// Test custom extractor that returns true
	SetImmediateLoggingExtractor(func(ctx context.Context) bool {
		return true
	})
	require.True(t, ImmediateLoggingEnabled(ctx))

	// Test custom extractor that checks context value
	type immediateLogKey struct{}
	SetImmediateLoggingExtractor(func(ctx context.Context) bool {
		val, ok := ctx.Value(immediateLogKey{}).(bool)
		return ok && val
	})

	// Context without the key should return false
	require.False(t, ImmediateLoggingEnabled(ctx))

	// Context with the key set to true should return true
	ctxWithFlag := context.WithValue(ctx, immediateLogKey{}, true)
	require.True(t, ImmediateLoggingEnabled(ctxWithFlag))

	// Context with the key set to false should return false
	ctxWithFalseflag := context.WithValue(ctx, immediateLogKey{}, false)
	require.False(t, ImmediateLoggingEnabled(ctxWithFalseflag))

	// Test setting nil extractor (should use no-op that returns false)
	SetImmediateLoggingExtractor(nil)
	require.False(t, ImmediateLoggingEnabled(ctxWithFlag))
}

func TestTraceEventFunc(t *testing.T) {
	// Save original handlers
	originalTraceEvent := globalTraceEventFunc.Load()
	originalIsCategoryEnabled := globalIsCategoryEnabledFunc.Load()
	defer func() {
		globalTraceEventFunc.Store(originalTraceEvent)
		globalIsCategoryEnabledFunc.Store(originalIsCategoryEnabled)
	}()

	// Test custom trace event func
	called := false
	SetTraceEventFunc(func(ctx context.Context, category Category, name string, fields ...zap.Field) {
		called = true
	})
	TraceEvent(context.Background(), CategoryTxn2PC, "test", zap.String("key", "value"))
	require.True(t, called)

	// Test setting nil trace event func (should use no-op)
	called = false
	SetTraceEventFunc(nil)
	TraceEvent(context.Background(), CategoryTxn2PC, "test", zap.String("key", "value"))
	require.False(t, called)
}

func TestIsCategoryEnabledFunc(t *testing.T) {
	// Save original handler
	originalIsCategoryEnabled := globalIsCategoryEnabledFunc.Load()
	defer func() {
		globalIsCategoryEnabledFunc.Store(originalIsCategoryEnabled)
	}()

	// Test default behavior (returns false)
	require.False(t, IsCategoryEnabled(CategoryTxn2PC))

	// Test custom category enabled func
	SetIsCategoryEnabledFunc(func(category Category) bool {
		return category == CategoryTxn2PC
	})
	require.True(t, IsCategoryEnabled(CategoryTxn2PC))
	require.False(t, IsCategoryEnabled(CategoryTxnLockResolve))

	// Test setting nil category enabled func (should use no-op that returns false)
	SetIsCategoryEnabledFunc(nil)
	require.False(t, IsCategoryEnabled(CategoryTxn2PC))
}

func TestTraceIDContext(t *testing.T) {
	ctx := context.Background()

	// Test context without trace ID
	require.Nil(t, TraceIDFromContext(ctx))
	require.Nil(t, TraceIDFromContext(nil))

	// Test context with trace ID
	traceID := []byte{1, 2, 3, 4, 5}
	ctxWithTrace := ContextWithTraceID(ctx, traceID)
	extractedID := TraceIDFromContext(ctxWithTrace)
	require.Equal(t, traceID, extractedID)

	// Test nested contexts
	traceID2 := []byte{6, 7, 8, 9, 10}
	ctxWithTrace2 := ContextWithTraceID(ctxWithTrace, traceID2)
	extractedID2 := TraceIDFromContext(ctxWithTrace2)
	require.Equal(t, traceID2, extractedID2)
}
