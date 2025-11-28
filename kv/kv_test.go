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

package kv

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetOptions(t *testing.T) {
	cases := []struct {
		options []GetOption
		check   func(*GetOptions)
	}{
		{
			check: func(opt *GetOptions) {
				require.Equal(t, GetOptions{}, *opt)
			},
		},
		{
			options: []GetOption{WithRequireCommitTS()},
			check: func(opt *GetOptions) {
				require.True(t, opt.RequireCommitTS())
			},
		},
		{
			options: BatchGetToGetOptions(nil),
			check: func(opt *GetOptions) {
				require.Equal(t, GetOptions{}, *opt)
			},
		},
		{
			options: BatchGetToGetOptions([]BatchGetOption{WithRequireCommitTS()}),
			check: func(opt *GetOptions) {
				require.True(t, opt.RequireCommitTS())
			},
		},
	}

	for _, c := range cases {
		var opts GetOptions
		opts.Apply(c.options)
		c.check(&opts)
	}
}

func TestBatchGetOptions(t *testing.T) {
	cases := []struct {
		options []BatchGetOption
		check   func(*BatchGetOptions)
	}{
		{
			check: func(opt *BatchGetOptions) {
				require.Equal(t, BatchGetOptions{}, *opt)
			},
		},
		{
			options: []BatchGetOption{WithRequireCommitTS()},
			check: func(opt *BatchGetOptions) {
				require.True(t, opt.RequireCommitTS())
			},
		},
	}

	for _, c := range cases {
		var opts BatchGetOptions
		opts.Apply(c.options)
		c.check(&opts)
	}
}

func TestValueEntry(t *testing.T) {
	require.True(t, ValueEntry{}.IsValueEmpty())
	require.True(t, NewValueEntry([]byte{}, 123).IsValueEmpty())
	require.True(t, NewValueEntry(nil, 123).IsValueEmpty())
	require.False(t, NewValueEntry([]byte{'x'}, 123).IsValueEmpty())
	require.False(t, NewValueEntry([]byte{'x'}, 0).IsValueEmpty())
}
