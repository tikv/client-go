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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTraceControlFlags_Has(t *testing.T) {
	flags := FlagImmediateLog | FlagTiKVCategoryRequest

	require.True(t, flags.Has(FlagImmediateLog))
	require.True(t, flags.Has(FlagTiKVCategoryRequest))
	require.False(t, flags.Has(FlagTiKVCategoryWriteDetails))
	require.False(t, flags.Has(FlagTiKVCategoryReadDetails))

	// Empty flags
	var emptyFlags TraceControlFlags
	require.False(t, emptyFlags.Has(FlagImmediateLog))
}

func TestTraceControlFlags_Set(t *testing.T) {
	var flags TraceControlFlags

	// Set individual flags
	flags = flags.Set(FlagImmediateLog)
	require.True(t, flags.Has(FlagImmediateLog))
	require.False(t, flags.Has(FlagTiKVCategoryRequest))

	flags = flags.Set(FlagTiKVCategoryRequest)
	require.True(t, flags.Has(FlagImmediateLog))
	require.True(t, flags.Has(FlagTiKVCategoryRequest))

	// Setting an already-set flag is idempotent
	flags = flags.Set(FlagImmediateLog)
	require.True(t, flags.Has(FlagImmediateLog))
}

func TestTraceControlFlags_Clear(t *testing.T) {
	flags := FlagImmediateLog | FlagTiKVCategoryRequest | FlagTiKVCategoryWriteDetails

	// Clear individual flags
	flags = flags.Clear(FlagTiKVCategoryRequest)
	require.True(t, flags.Has(FlagImmediateLog))
	require.False(t, flags.Has(FlagTiKVCategoryRequest))
	require.True(t, flags.Has(FlagTiKVCategoryWriteDetails))

	flags = flags.Clear(FlagImmediateLog)
	require.False(t, flags.Has(FlagImmediateLog))
	require.True(t, flags.Has(FlagTiKVCategoryWriteDetails))

	// Clearing an already-cleared flag is a no-op
	flags = flags.Clear(FlagImmediateLog)
	require.False(t, flags.Has(FlagImmediateLog))
}

func TestTraceControlFlags_Toggle(t *testing.T) {
	var flags TraceControlFlags

	// Toggle on
	flags = flags.Toggle(FlagImmediateLog)
	require.True(t, flags.Has(FlagImmediateLog))

	// Toggle off
	flags = flags.Toggle(FlagImmediateLog)
	require.False(t, flags.Has(FlagImmediateLog))

	// Toggle multiple flags
	flags = flags.Toggle(FlagTiKVCategoryRequest | FlagTiKVCategoryWriteDetails)
	require.True(t, flags.Has(FlagTiKVCategoryRequest))
	require.True(t, flags.Has(FlagTiKVCategoryWriteDetails))
}

func TestTraceControlFlags_IsZero(t *testing.T) {
	var flags TraceControlFlags
	require.True(t, flags.IsZero())

	flags = flags.Set(FlagImmediateLog)
	require.False(t, flags.IsZero())

	flags = flags.Clear(FlagImmediateLog)
	require.True(t, flags.IsZero())
}

func TestTraceControlFlags_CombinedOperations(t *testing.T) {
	// Build up flags step by step
	flags := TraceControlFlags(0).
		Set(FlagImmediateLog).
		Set(FlagTiKVCategoryRequest).
		Set(FlagTiKVCategoryWriteDetails).
		Set(FlagTiKVCategoryReadDetails)

	// All four flags should be set
	require.True(t, flags.Has(FlagImmediateLog))
	require.True(t, flags.Has(FlagTiKVCategoryRequest))
	require.True(t, flags.Has(FlagTiKVCategoryWriteDetails))
	require.True(t, flags.Has(FlagTiKVCategoryReadDetails))

	// Clear some flags
	flags = flags.Clear(FlagTiKVCategoryRequest | FlagTiKVCategoryReadDetails)
	require.True(t, flags.Has(FlagImmediateLog))
	require.False(t, flags.Has(FlagTiKVCategoryRequest))
	require.True(t, flags.Has(FlagTiKVCategoryWriteDetails))
	require.False(t, flags.Has(FlagTiKVCategoryReadDetails))
}

func TestTraceControlFlags_BitValues(t *testing.T) {
	// Verify the bit positions are correct
	require.Equal(t, TraceControlFlags(1<<0), FlagImmediateLog)
	require.Equal(t, TraceControlFlags(1<<1), FlagTiKVCategoryRequest)
	require.Equal(t, TraceControlFlags(1<<2), FlagTiKVCategoryWriteDetails)
	require.Equal(t, TraceControlFlags(1<<3), FlagTiKVCategoryReadDetails)

	// Verify flags don't overlap
	require.NotEqual(t, FlagImmediateLog, FlagTiKVCategoryRequest)
	require.NotEqual(t, FlagTiKVCategoryWriteDetails, FlagTiKVCategoryReadDetails)

	// Verify we can combine all flags
	all := FlagImmediateLog | FlagTiKVCategoryRequest | FlagTiKVCategoryWriteDetails | FlagTiKVCategoryReadDetails
	require.Equal(t, TraceControlFlags(0b1111), all)
}
