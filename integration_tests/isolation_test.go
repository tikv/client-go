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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// NOTE: The code in this file is based on code from the
// TiDB project, licensed under the Apache License v 2.0
//
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/tests/isolation_test.go
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

//go:build !race
// +build !race

package tikv_test

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
	kverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
)

func TestIsolation(t *testing.T) {
	suite.Run(t, new(testIsolationSuite))
}

// testIsolationSuite represents test isolation suite.
// The test suite takes too long under the race detector.
type testIsolationSuite struct {
	suite.Suite
	store *tikv.KVStore
}

func (s *testIsolationSuite) SetupSuite() {
	s.store = NewTestStore(s.T())
}

func (s *testIsolationSuite) TearDownSuite() {
	s.store.Close()
}

type writeRecord struct {
	startTS  uint64
	commitTS uint64
}

type writeRecords []writeRecord

func (r writeRecords) Len() int           { return len(r) }
func (r writeRecords) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }
func (r writeRecords) Less(i, j int) bool { return r[i].startTS <= r[j].startTS }

func (s *testIsolationSuite) SetWithRetry(k, v []byte) writeRecord {
	for {
		txnRaw, err := s.store.Begin()
		s.Nil(err)

		txn := transaction.TxnProbe{KVTxn: txnRaw}

		err = txn.Set(k, v)
		s.Nil(err)

		err = txn.Commit(context.Background())
		if err == nil {
			return writeRecord{
				startTS:  txn.StartTS(),
				commitTS: txn.GetCommitTS(),
			}
		}
	}
}

type readRecord struct {
	startTS uint64
	value   []byte
}

type readRecords []readRecord

func (r readRecords) Len() int           { return len(r) }
func (r readRecords) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }
func (r readRecords) Less(i, j int) bool { return r[i].startTS <= r[j].startTS }

func (s *testIsolationSuite) GetWithRetry(k []byte) readRecord {
	for {
		txn, err := s.store.Begin()
		s.Nil(err)

		val, err := txn.Get(context.TODO(), k)
		if err == nil {
			return readRecord{
				startTS: txn.StartTS(),
				value:   val,
			}
		}
		var e *kverr.ErrRetryable
		s.True(errors.As(err, &e))
	}
}

func (s *testIsolationSuite) TestWriteWriteConflict() {
	const (
		threadCount  = 10
		setPerThread = 50
	)
	var (
		mu     sync.Mutex
		writes []writeRecord
		wg     sync.WaitGroup
	)
	wg.Add(threadCount)
	for i := 0; i < threadCount; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < setPerThread; j++ {
				w := s.SetWithRetry([]byte("k"), []byte("v"))
				mu.Lock()
				writes = append(writes, w)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	// Check all transactions' [startTS, commitTS] are not overlapped.
	sort.Sort(writeRecords(writes))
	for i := 0; i < len(writes)-1; i++ {
		s.Less(writes[i].commitTS, writes[i+1].startTS)
	}
}

func (s *testIsolationSuite) TestReadWriteConflict() {
	const (
		readThreadCount = 10
		writeCount      = 10
	)

	var (
		writes []writeRecord
		mu     sync.Mutex
		reads  []readRecord
		wg     sync.WaitGroup
	)

	s.SetWithRetry([]byte("k"), []byte("0"))

	writeDone := make(chan struct{})
	go func() {
		for i := 1; i <= writeCount; i++ {
			w := s.SetWithRetry([]byte("k"), []byte(fmt.Sprintf("%d", i)))
			writes = append(writes, w)
			time.Sleep(time.Microsecond * 10)
		}
		close(writeDone)
	}()

	wg.Add(readThreadCount)
	for i := 0; i < readThreadCount; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-writeDone:
					return
				default:
				}
				r := s.GetWithRetry([]byte("k"))
				mu.Lock()
				reads = append(reads, r)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	sort.Sort(readRecords(reads))

	// Check all reads got the value committed before it's startTS.
	var i, j int
	for ; i < len(writes); i++ {
		for ; j < len(reads); j++ {
			w, r := writes[i], reads[j]
			if r.startTS >= w.commitTS {
				break
			}
			s.Equal(string(r.value), fmt.Sprintf("%d", i))
		}
	}
	for ; j < len(reads); j++ {
		s.Equal(string(reads[j].value), fmt.Sprintf("%d", len(writes)))
	}
}
