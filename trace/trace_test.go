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

func TestTraceControlExtractor(t *testing.T) {
	// Save original extractor
	originalExtractor := globalTraceControlExtractorFunc.Load()
	defer func() {
		globalTraceControlExtractorFunc.Store(originalExtractor)
	}()

	// Test default behavior (returns FlagTiKVCategoryRequest)
	ctx := context.Background()
	defaultFlags := GetTraceControlFlags(ctx)
	require.Equal(t, FlagTiKVCategoryRequest, defaultFlags)
	require.True(t, defaultFlags.Has(FlagTiKVCategoryRequest))
	require.False(t, defaultFlags.Has(FlagImmediateLog))
	require.False(t, ImmediateLoggingEnabled(ctx))

	// Test custom extractor that returns specific flags
	SetTraceControlExtractor(func(ctx context.Context) TraceControlFlags {
		return FlagImmediateLog | FlagTiKVCategoryRequest
	})
	flags := GetTraceControlFlags(ctx)
	require.True(t, flags.Has(FlagImmediateLog))
	require.True(t, flags.Has(FlagTiKVCategoryRequest))
	require.False(t, flags.Has(FlagTiKVCategoryWriteDetails))
	require.True(t, ImmediateLoggingEnabled(ctx))

	// Test custom extractor that checks context value
	type flagsKey struct{}
	SetTraceControlExtractor(func(ctx context.Context) TraceControlFlags {
		if val, ok := ctx.Value(flagsKey{}).(TraceControlFlags); ok {
			return val
		}
		return 0
	})

	// Context without the key should return 0
	require.Equal(t, TraceControlFlags(0), GetTraceControlFlags(ctx))
	require.False(t, ImmediateLoggingEnabled(ctx))

	// Context with specific flags
	ctxWithFlags := context.WithValue(ctx, flagsKey{}, FlagTiKVCategoryWriteDetails|FlagTiKVCategoryReadDetails)
	flags = GetTraceControlFlags(ctxWithFlags)
	require.True(t, flags.Has(FlagTiKVCategoryWriteDetails))
	require.True(t, flags.Has(FlagTiKVCategoryReadDetails))
	require.False(t, flags.Has(FlagImmediateLog))
	require.False(t, ImmediateLoggingEnabled(ctxWithFlags))

	// Context with immediate log flag
	ctxWithImmediate := context.WithValue(ctx, flagsKey{}, FlagImmediateLog)
	require.True(t, ImmediateLoggingEnabled(ctxWithImmediate))

	// Test setting nil extractor (should use no-op that returns FlagTiKVCategoryRequest)
	SetTraceControlExtractor(nil)
	require.Equal(t, FlagTiKVCategoryRequest, GetTraceControlFlags(ctxWithFlags))
	require.False(t, ImmediateLoggingEnabled(ctxWithFlags))
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

func TestCheckFlightRecorderDumpTrigger(t *testing.T) {
	original := globalCheckFlightRecorderDumpTriggerFunc.Load()
	defer func() {
		globalCheckFlightRecorderDumpTriggerFunc.Store(original)
	}()

	// Test default behavior (returns false)
	ctx := context.Background()
	CheckFlightRecorderDumpTrigger(ctx, "just cover some code", 42)

	// Test custom behavior
	var covered bool
	mock := func(ctx context.Context, triggerName string, val any) {
		covered = true
	}
	SetCheckFlightRecorderDumpTriggerFunc(CheckFlightRecorderDumpTriggerFunc(mock))

	require.False(t, covered)
	CheckFlightRecorderDumpTrigger(ctx, "custom trigger", 123)
	require.True(t, covered)
}
