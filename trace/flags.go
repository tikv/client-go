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

// TraceControlFlags represents trace logging control flags.
// Each bit controls a different aspect of trace logging behavior.
type TraceControlFlags uint64

const (
	// FlagImmediateLog forces immediate logging without buffering.
	// When set, TiKV will log trace events immediately instead of buffering them.
	FlagImmediateLog TraceControlFlags = 1 << 0

	// FlagTiKVCategoryRequest enables TiKV request category tracing.
	// When set, TiKV will trace request processing.
	FlagTiKVCategoryRequest TraceControlFlags = 1 << 1

	// FlagTiKVCategoryWriteDetails enables TiKV detailed write operation tracing.
	// When set, TiKV will trace detailed write operation steps.
	FlagTiKVCategoryWriteDetails TraceControlFlags = 1 << 2

	// FlagTiKVCategoryReadDetails enables TiKV detailed read operation tracing.
	// When set, TiKV will trace detailed read operation steps.
	FlagTiKVCategoryReadDetails TraceControlFlags = 1 << 3
)

// Has checks if the given flag is set.
func (f TraceControlFlags) Has(flag TraceControlFlags) bool {
	return f&flag != 0
}

// Set returns a new flags value with the given flag set.
func (f TraceControlFlags) Set(flag TraceControlFlags) TraceControlFlags {
	return f | flag
}

// Clear returns a new flags value with the given flag cleared.
func (f TraceControlFlags) Clear(flag TraceControlFlags) TraceControlFlags {
	return f &^ flag
}

// Toggle returns a new flags value with the given flag toggled.
func (f TraceControlFlags) Toggle(flag TraceControlFlags) TraceControlFlags {
	return f ^ flag
}

// IsZero returns true if no flags are set.
func (f TraceControlFlags) IsZero() bool {
	return f == 0
}
