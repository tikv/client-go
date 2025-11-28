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

func TestTraceControlFlags_With(t *testing.T) {
	var flags TraceControlFlags

	// Set individual flags
	flags = flags.With(FlagImmediateLog)
	require.True(t, flags.Has(FlagImmediateLog))
	require.False(t, flags.Has(FlagTiKVCategoryRequest))

	flags = flags.With(FlagTiKVCategoryRequest)
	require.True(t, flags.Has(FlagImmediateLog))
	require.True(t, flags.Has(FlagTiKVCategoryRequest))

	// Setting an already-set flag is idempotent
	flags = flags.With(FlagImmediateLog)
	require.True(t, flags.Has(FlagImmediateLog))
}

func TestTraceControlFlags_CombinedOperations(t *testing.T) {
	// Build up flags step by step (fluent style)
	flags := TraceControlFlags(0).
		With(FlagImmediateLog).
		With(FlagTiKVCategoryRequest).
		With(FlagTiKVCategoryWriteDetails).
		With(FlagTiKVCategoryReadDetails)

	// All four flags should be set
	require.True(t, flags.Has(FlagImmediateLog))
	require.True(t, flags.Has(FlagTiKVCategoryRequest))
	require.True(t, flags.Has(FlagTiKVCategoryWriteDetails))
	require.True(t, flags.Has(FlagTiKVCategoryReadDetails))
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
