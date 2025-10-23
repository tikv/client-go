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
	"sync/atomic"

	"go.uber.org/zap"
)

// Category identifies a trace event family emitted from client-go.
type Category uint32

const (
	// CategoryTxn2PC traces two-phase commit prewrite and commit phases.
	CategoryTxn2PC Category = iota
	// CategoryTxnLockResolve traces lock resolution and conflict handling.
	CategoryTxnLockResolve
	// CategoryKVRequest traces individual KV request send and result events.
	CategoryKVRequest
)

// TraceEventFunc is the function signature for recording trace events.
type TraceEventFunc func(ctx context.Context, category Category, name string, fields ...zap.Field)

// IsCategoryEnabledFunc is the function signature for checking if a category is enabled.
type IsCategoryEnabledFunc func(category Category) bool

// Default no-op implementations
func noopTraceEvent(context.Context, Category, string, ...zap.Field) {}
func noopIsCategoryEnabled(Category) bool                            { return false }

// Global function pointers stored independently
var (
	globalTraceEventFunc        atomic.Pointer[TraceEventFunc]
	globalIsCategoryEnabledFunc atomic.Pointer[IsCategoryEnabledFunc]
)

func init() {
	// Set default no-op implementations
	defaultTraceEvent := TraceEventFunc(noopTraceEvent)
	defaultIsCategoryEnabled := IsCategoryEnabledFunc(noopIsCategoryEnabled)
	globalTraceEventFunc.Store(&defaultTraceEvent)
	globalIsCategoryEnabledFunc.Store(&defaultIsCategoryEnabled)
}

// SetTraceEventFunc registers the trace event handler function.
// This is typically called once during application initialization (e.g., by TiDB).
// Passing nil will use a no-op implementation.
func SetTraceEventFunc(fn TraceEventFunc) {
	if fn == nil {
		fn = noopTraceEvent
	}
	globalTraceEventFunc.Store(&fn)
}

// SetIsCategoryEnabledFunc registers the category enablement check function.
// This can be updated independently of the trace event function.
// Passing nil will use a no-op implementation that returns false.
func SetIsCategoryEnabledFunc(fn IsCategoryEnabledFunc) {
	if fn == nil {
		fn = noopIsCategoryEnabled
	}
	globalIsCategoryEnabledFunc.Store(&fn)
}

// TraceEvent records a trace event using the registered function.
func TraceEvent(ctx context.Context, category Category, name string, fields ...zap.Field) {
	fn := globalTraceEventFunc.Load()
	(*fn)(ctx, category, name, fields...)
}

// IsCategoryEnabled checks if a category is enabled for tracing using the registered function.
func IsCategoryEnabled(category Category) bool {
	fn := globalIsCategoryEnabledFunc.Load()
	return (*fn)(category)
}

// Trace ID context management

// traceIDKey is the context key for storing trace IDs.
// This key is shared between TiDB and client-go for trace ID propagation.
type traceIDKey struct{}

// ContextWithTraceID returns a new context with the given trace ID attached.
func ContextWithTraceID(ctx context.Context, traceID []byte) context.Context {
	return context.WithValue(ctx, traceIDKey{}, traceID)
}

// TraceIDFromContext extracts the trace ID from the context.
// Returns nil if no trace ID is present.
func TraceIDFromContext(ctx context.Context) []byte {
	if ctx == nil {
		return nil
	}
	if traceID, ok := ctx.Value(traceIDKey{}).([]byte); ok {
		return traceID
	}
	return nil
}
