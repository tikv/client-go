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
// See the License for the specific language governing permissions and
// limitations under the License.

// NOTE: The code in this file is based on code from the
// TiDB project, licensed under the Apache License v 2.0
//
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/tests/range_task_test.go
//

// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv_test

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"testing"

	"github.com/pingcap/tidb/store/mockstore/mockcopr"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/rangetask"
)

func TestRangeTask(t *testing.T) {
	suite.Run(t, new(testRangeTaskSuite))
}

type testRangeTaskSuite struct {
	suite.Suite
	cluster testutils.Cluster
	store   *tikv.KVStore

	testRanges     []kv.KeyRange
	expectedRanges [][]kv.KeyRange
}

func makeRange(startKey string, endKey string) kv.KeyRange {
	return kv.KeyRange{
		StartKey: []byte(startKey),
		EndKey:   []byte(endKey),
	}
}

func (s *testRangeTaskSuite) SetupTest() {
	// Split the store at "a" to "z"
	splitKeys := make([][]byte, 0)
	for k := byte('a'); k <= byte('z'); k++ {
		splitKeys = append(splitKeys, []byte{k})
	}

	// Calculate all region's ranges
	allRegionRanges := []kv.KeyRange{makeRange("", "a")}
	for i := 0; i < len(splitKeys)-1; i++ {
		allRegionRanges = append(allRegionRanges, kv.KeyRange{
			StartKey: splitKeys[i],
			EndKey:   splitKeys[i+1],
		})
	}
	allRegionRanges = append(allRegionRanges, makeRange("z", ""))

	client, cluster, pdClient, err := testutils.NewMockTiKV("", mockcopr.NewCoprRPCHandler())
	s.Require().Nil(err)
	testutils.BootstrapWithMultiRegions(cluster, splitKeys...)
	s.cluster = cluster

	store, err := tikv.NewTestTiKVStore(client, pdClient, nil, nil, 0)
	s.Require().Nil(err)

	s.store = store

	s.testRanges = []kv.KeyRange{
		makeRange("", ""),
		makeRange("", "b"),
		makeRange("b", ""),
		makeRange("b", "x"),
		makeRange("a", "d"),
		makeRange("a\x00", "d\x00"),
		makeRange("a\xff\xff\xff", "c\xff\xff\xff"),
		makeRange("a1", "a2"),
		makeRange("a", "a"),
		makeRange("a3", "a3"),
	}

	s.expectedRanges = [][]kv.KeyRange{
		allRegionRanges,
		allRegionRanges[:2],
		allRegionRanges[2:],
		allRegionRanges[2:24],
		{
			makeRange("a", "b"),
			makeRange("b", "c"),
			makeRange("c", "d"),
		},
		{
			makeRange("a\x00", "b"),
			makeRange("b", "c"),
			makeRange("c", "d"),
			makeRange("d", "d\x00"),
		},
		{
			makeRange("a\xff\xff\xff", "b"),
			makeRange("b", "c"),
			makeRange("c", "c\xff\xff\xff"),
		},
		{
			makeRange("a1", "a2"),
		},
		{},
		{},
	}
}

func (s *testRangeTaskSuite) TearDownTest() {
	err := s.store.Close()
	s.Require().Nil(err)
}

func collect(c chan *kv.KeyRange) []kv.KeyRange {
	c <- nil
	ranges := make([]kv.KeyRange, 0)

	for {
		r := <-c
		if r == nil {
			break
		}

		ranges = append(ranges, *r)
	}
	return ranges
}

func (s *testRangeTaskSuite) checkRanges(obtained []kv.KeyRange, expected []kv.KeyRange) {
	sort.Slice(obtained, func(i, j int) bool {
		return bytes.Compare(obtained[i].StartKey, obtained[j].StartKey) < 0
	})

	s.Equal(obtained, expected)
}

func batchRanges(ranges []kv.KeyRange, batchSize int) []kv.KeyRange {
	result := make([]kv.KeyRange, 0, len(ranges))

	for i := 0; i < len(ranges); i += batchSize {
		lastRange := i + batchSize - 1
		if lastRange >= len(ranges) {
			lastRange = len(ranges) - 1
		}

		result = append(result, kv.KeyRange{
			StartKey: ranges[i].StartKey,
			EndKey:   ranges[lastRange].EndKey,
		})
	}

	return result
}

func (s *testRangeTaskSuite) testRangeTaskImpl(concurrency int) {
	s.T().Logf("Test RangeTask, concurrency: %v", concurrency)

	ranges := make(chan *kv.KeyRange, 100)

	handler := func(ctx context.Context, r kv.KeyRange) (rangetask.TaskStat, error) {
		ranges <- &r
		stat := rangetask.TaskStat{
			CompletedRegions: 1,
		}
		return stat, nil
	}

	runner := rangetask.NewRangeTaskRunner("test-runner", s.store, concurrency, handler)

	for regionsPerTask := 1; regionsPerTask <= 5; regionsPerTask++ {
		for i, r := range s.testRanges {
			runner.SetRegionsPerTask(regionsPerTask)

			expectedRanges := batchRanges(s.expectedRanges[i], regionsPerTask)

			err := runner.RunOnRange(context.Background(), r.StartKey, r.EndKey)
			s.Nil(err)
			s.checkRanges(collect(ranges), expectedRanges)
			s.Equal(runner.CompletedRegions(), len(expectedRanges))
			s.Equal(runner.FailedRegions(), 0)
		}
	}
}

func (s *testRangeTaskSuite) TestRangeTask() {
	for concurrency := 1; concurrency < 5; concurrency++ {
		s.testRangeTaskImpl(concurrency)
	}
}

func (s *testRangeTaskSuite) testRangeTaskErrorImpl(concurrency int) {
	for i, r := range s.testRanges {
		// Iterate all sub tasks and make it an error
		subRanges := s.expectedRanges[i]
		for _, subRange := range subRanges {
			errKey := subRange.StartKey
			s.T().Logf("Test RangeTask Error concurrency: %v, range: [%+q, %+q), errKey: %+q", concurrency, r.StartKey, r.EndKey, errKey)

			handler := func(ctx context.Context, r kv.KeyRange) (rangetask.TaskStat, error) {
				stat := rangetask.TaskStat{CompletedRegions: 0, FailedRegions: 0}
				if bytes.Equal(r.StartKey, errKey) {
					stat.FailedRegions++
					return stat, errors.New("test error")
				}
				stat.CompletedRegions++
				return stat, nil
			}

			runner := rangetask.NewRangeTaskRunner("test-error-runner", s.store, concurrency, handler)
			runner.SetRegionsPerTask(1)
			err := runner.RunOnRange(context.Background(), r.StartKey, r.EndKey)
			// RunOnRange returns no error only when all sub tasks are done successfully.
			s.NotNil(err)
			s.Less(runner.CompletedRegions(), len(subRanges))
			s.Equal(runner.FailedRegions(), 1)
		}
	}
}

func (s *testRangeTaskSuite) TestRangeTaskError() {
	for concurrency := 1; concurrency < 5; concurrency++ {
		s.testRangeTaskErrorImpl(concurrency)
	}
}
