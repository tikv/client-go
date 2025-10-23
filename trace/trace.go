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

// EventTracer is the interface for recording trace events.
// This allows TiDB to inject its trace event implementation without creating a dependency.
type EventTracer interface {
	// TraceEvent records a trace event with the given category, name, and fields.
	TraceEvent(ctx context.Context, category Category, name string, fields ...zap.Field)
}

// CategoryChecker is an optional interface that EventTracer implementations can provide
// to allow efficient category enablement checks before expensive event construction.
type CategoryChecker interface {
	// IsCategoryEnabled returns true if the specified category is currently enabled for tracing.
	IsCategoryEnabled(category Category) bool
}

// TraceIDProvider is an optional interface that EventTracer implementations can provide
// to enable trace ID extraction from context. This allows client-go to propagate trace IDs
// to TiKV without knowing about the application's (e.g., TiDB's) context key implementation.
type TraceIDProvider interface {
	// ExtractTraceID extracts the trace ID from the context.
	// Returns nil if no trace ID is present.
	ExtractTraceID(ctx context.Context) []byte
}

// noopTracer is a no-op implementation used when no tracer is set.
type noopTracer struct{}

func (noopTracer) TraceEvent(context.Context, Category, string, ...zap.Field) {}

var globalTracer atomic.Value // stores EventTracer

func init() {
	globalTracer.Store(EventTracer(noopTracer{}))
}

// SetGlobalTracer sets the global tracer implementation.
// This should be called once during initialization by the TiDB layer.
func SetGlobalTracer(tracer EventTracer) {
	if tracer == nil {
		tracer = noopTracer{}
	}
	globalTracer.Store(tracer)
}

// TraceEvent records a trace event using the global tracer.
func TraceEvent(ctx context.Context, category Category, name string, fields ...zap.Field) {
	tracer := globalTracer.Load().(EventTracer)
	tracer.TraceEvent(ctx, category, name, fields...)
}

// IsCategoryEnabled checks if a category is enabled for tracing.
// Returns true if the tracer supports category checking and the category is enabled,
// or true if the tracer doesn't support checking (conservative default).
func IsCategoryEnabled(category Category) bool {
	tracer := globalTracer.Load().(EventTracer)
	if checker, ok := tracer.(CategoryChecker); ok {
		return checker.IsCategoryEnabled(category)
	}
	// If tracer doesn't implement CategoryChecker, assume enabled
	return true
}

// TraceIDFromContext extracts the trace ID from the context using the global tracer.
// Returns nil if the tracer doesn't implement TraceIDProvider or if no trace ID is present.
func TraceIDFromContext(ctx context.Context) []byte {
	if ctx == nil {
		return nil
	}
	tracer := globalTracer.Load().(EventTracer)
	if provider, ok := tracer.(TraceIDProvider); ok {
		return provider.ExtractTraceID(ctx)
	}
	return nil
}
