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

// NOTE: The code in this file is based on code from the
// TiDB project, licensed under the Apache License v 2.0
//
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/tests/lock_test.go
//

// Copyright 2016 PingCAP, Inc.
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

package tikv_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/suite"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"github.com/tikv/client-go/v2/util"
	"github.com/tikv/client-go/v2/util/intest"
)

type mockCommitTSOracle struct {
	oracle.Oracle
	mu struct {
		sync.Mutex
		startTS         uint64
		commitTSOffsets []uint64
	}
}

func (o *mockCommitTSOracle) ResetMock(startTS uint64, commitTSOffsets []uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.mu.startTS = startTS
	o.mu.commitTSOffsets = commitTSOffsets
}

func (o *mockCommitTSOracle) GetTimestamp(ctx context.Context, option *oracle.Option) (uint64, error) {
	if ts := ctx.Value(transaction.CtxInGetTimestampForCommitKey); ts != nil {
		o.mu.Lock()
		startTS, ok := ts.(uint64)
		if !ok || startTS != o.mu.startTS {
			o.mu.Unlock()
		} else {
			defer o.mu.Unlock()
			if len(o.mu.commitTSOffsets) == 0 {
				panic("empty mock oracle ts set")
			}
			offset := o.mu.commitTSOffsets[0]
			o.mu.commitTSOffsets = o.mu.commitTSOffsets[1:]
			return o.mu.startTS + offset, nil
		}
	}
	return o.Oracle.GetTimestamp(ctx, option)
}

type testOptionSuite struct {
	suite.Suite
	mockOracle *mockCommitTSOracle
	store      tikv.StoreProbe
}

func (s *testOptionSuite) SetupSuite() {
	s.store = tikv.StoreProbe{KVStore: NewTestStore(s.T())}
	s.mockOracle = &mockCommitTSOracle{Oracle: s.store.GetOracle()}
	s.store.SetOracle(s.mockOracle)
}

func (s *testOptionSuite) TearDownSuite() {
	s.store.Close()
}

func TestOption(t *testing.T) {
	suite.Run(t, new(testOptionSuite))
}

func (s *testOptionSuite) MetricValues(m any) *dto.Metric {
	metric := &dto.Metric{}
	s.NoError(m.(prometheus.Metric).Write(metric))
	return metric
}

func (s *testOptionSuite) GetHistogramMetricSampleCount(m any) uint64 {
	return s.MetricValues(m).GetHistogram().GetSampleCount()
}

func (s *testOptionSuite) TestSetCommitWaitUntilTSO() {
	origInTest := intest.InTest
	defer func() {
		intest.InTest = origInTest
	}()
	intest.InTest = true
	doWithCollectHistSamplesInc := func(do func() error, metrics []any) ([]uint64, error) {
		cnt := make([]uint64, len(metrics))
		for i, metric := range metrics {
			cnt[i] = s.GetHistogramMetricSampleCount(metric)
		}
		err := do()
		for i, metric := range metrics {
			cnt[i] = s.GetHistogramMetricSampleCount(metric) - cnt[i]
		}
		return cnt, err
	}

	for _, tc := range []struct {
		name string
		// We will call `txn.SetCommitWaitUntilTSO(commitWaitTSO + txn.StartTS())`
		commitWaitTSO uint64
		// Mock the PD TSOs in commit phase as {
		//		mockCommitTSO[0] + txn.StartTS(), // first attempt
		//		mockCommitTSO[1] + txn.StartTS(), // second attempt
		//      ...
		//	}
		mockCommitTSO  []uint64
		setWaitTimeout time.Duration
		setNoWait      bool
		extraPrepare   func(transaction.TxnProbe)
		err            bool
	}{
		{
			name:          "no lag commit ts",
			commitWaitTSO: 1,
			mockCommitTSO: []uint64{100},
		},
		{
			name:          "lag, retry once and success",
			commitWaitTSO: 200,
			mockCommitTSO: []uint64{100, 201},
		},
		{
			name:          "no wait",
			commitWaitTSO: 200,
			mockCommitTSO: []uint64{100},
			setNoWait:     true,
			err:           true,
		},
		{
			name:          "lag, retry twice and success",
			commitWaitTSO: 300,
			mockCommitTSO: []uint64{100, 200, 301},
		},
		{
			name:          "lag too much, fail directly",
			commitWaitTSO: oracle.ComposeTS(10000, 0),
			mockCommitTSO: []uint64{100},
			err:           true,
		},
		{
			name:           "lag, retry but timeout",
			commitWaitTSO:  100,
			mockCommitTSO:  []uint64{10, 20},
			setWaitTimeout: time.Millisecond,
			err:            true,
		},
		{
			name:          "should also check for 1pc",
			commitWaitTSO: oracle.ComposeTS(10000, 0),
			mockCommitTSO: []uint64{100},
			extraPrepare: func(txn transaction.TxnProbe) {
				txn.SetEnableAsyncCommit(true)
				txn.SetEnable1PC(true)
			},
			err: true,
		},
		{
			name:          "should also check for causal consistency",
			commitWaitTSO: oracle.ComposeTS(10000, 0),
			mockCommitTSO: []uint64{100},
			extraPrepare: func(txn transaction.TxnProbe) {
				txn.SetEnableAsyncCommit(true)
				txn.SetEnable1PC(true)
				txn.SetCausalConsistency(true)
			},
			err: true,
		},
	} {
		s.Run(tc.name, func() {
			txn, err := s.store.Begin()
			s.NoError(err)
			s.NoError(txn.Set([]byte("somekey:"+uuid.NewString()), []byte("somevalue")))
			// by default, 1pc and async commit are disabled, but it can be overridden in extraPrepare
			txn.SetEnableAsyncCommit(false)
			txn.SetEnable1PC(false)
			if tc.extraPrepare != nil {
				tc.extraPrepare(txn)
			}

			txn.SetCommitWaitUntilTSO(tc.commitWaitTSO + txn.StartTS())
			if tc.setNoWait {
				s.Zero(tc.setWaitTimeout)
				txn.SetCommitWaitUntilTSOTimeout(0)
			} else if tc.setWaitTimeout > 0 {
				txn.SetCommitWaitUntilTSOTimeout(tc.setWaitTimeout)
			}
			var commitDetail *util.CommitDetails
			inc, err := doWithCollectHistSamplesInc(func() error {
				s.mockOracle.ResetMock(txn.StartTS(), tc.mockCommitTSO)
				defer s.mockOracle.ResetMock(0, nil)
				return txn.Commit(context.WithValue(context.Background(), util.CommitDetailCtxKey, &commitDetail))
			}, []any{
				// ok metrics
				metrics.LagCommitTSWaitHistogramWithOK,
				metrics.LagCommitTSAttemptHistogramWithOK,
				// err metrics
				metrics.LagCommitTSWaitHistogramWithError,
				metrics.LagCommitTSAttemptHistogramWithError,
			})

			s.Equal(txn.StartTS()+tc.commitWaitTSO, txn.GetCommitWaitUntilTSO())
			if !tc.err {
				s.NoError(err)
				s.Equal(txn.CommitTS(), txn.StartTS()+tc.mockCommitTSO[len(tc.mockCommitTSO)-1])
				if len(tc.mockCommitTSO) == 1 {
					// if fetch TSO once and then success, there is no lag metrics
					s.Equal([]uint64{0, 0, 0, 0}, inc)
					s.Equal(util.CommitTSLagDetails{}, commitDetail.LagDetails)
				} else {
					s.Equal([]uint64{1, 1, 0, 0}, inc)
					s.NotZero(txn.KVTxn.GetCommitWaitUntilTSO())
					lagWaitTime := commitDetail.LagDetails.WaitTime
					s.Positive(lagWaitTime)
					s.Equal(util.CommitTSLagDetails{
						WaitTime:    lagWaitTime,
						BackoffCnt:  len(tc.mockCommitTSO) - 1,
						FirstLagTS:  txn.StartTS() + tc.mockCommitTSO[0],
						WaitUntilTS: txn.GetCommitWaitUntilTSO(),
					}, commitDetail.LagDetails)
				}
			} else {
				s.Error(err)
				s.True(tikverr.IsErrorCommitTSLag(err))
				s.Equal([]uint64{0, 0, 1, 1}, inc)
			}
		})
	}
}

func (s *testOptionSuite) TestSetCommitWaitUntilTSOTimeout() {
	txn, err := s.store.Begin()
	s.NoError(err)
	defer txn.Rollback()
	s.Equal(time.Second, txn.GetCommitWaitUntilTSOTimeout())
	txn.KVTxn.SetCommitWaitUntilTSOTimeout(2 * time.Second)
	s.Equal(2*time.Second, txn.GetCommitWaitUntilTSOTimeout())
}
