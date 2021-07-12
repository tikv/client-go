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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/tests/scan_test.go
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
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikv"
)

var scanBatchSize = tikv.ConfigProbe{}.GetScanBatchSize()

func TestScan(t *testing.T) {
	suite.Run(t, new(testScanSuite))
}

type testScanSuite struct {
	suite.Suite
	store        *tikv.KVStore
	recordPrefix []byte
	rowNums      []int
}

func (s *testScanSuite) SetupSuite() {
	s.store = NewTestStore(s.T())
	s.recordPrefix = []byte("prefix")
	s.rowNums = append(s.rowNums, 1, scanBatchSize, scanBatchSize+1, scanBatchSize*3)
}

func (s *testScanSuite) TearDownSuite() {
	txn := s.beginTxn()
	scanner, err := txn.Iter(s.recordPrefix, nil)
	s.Require().Nil(err)
	s.Require().NotNil(scanner)
	for scanner.Valid() {
		k := scanner.Key()
		err = txn.Delete(k)
		s.Require().Nil(err)
		scanner.Next()
	}
	err = txn.Commit(context.Background())
	s.Require().Nil(err)
	err = s.store.Close()
	s.Require().Nil(err)
}

func (s *testScanSuite) beginTxn() *tikv.KVTxn {
	txn, err := s.store.Begin()
	s.Require().Nil(err)
	return txn
}

func (s *testScanSuite) makeKey(i int) []byte {
	var key []byte
	key = append(key, s.recordPrefix...)
	key = append(key, []byte(fmt.Sprintf("%10d", i))...)
	return key
}

func (s *testScanSuite) makeValue(i int) []byte {
	return []byte(fmt.Sprintf("%d", i))
}

func (s *testScanSuite) TestScan() {
	check := func(scan tikv.Iterator, rowNum int, keyOnly bool) {
		for i := 0; i < rowNum; i++ {
			k := scan.Key()
			expectedKey := s.makeKey(i)
			s.Equal(k, expectedKey, "i=%v,rowNum=%v,key=%v,val=%v,expected=%v,keyOnly=%v", i, rowNum, kv.StrKey(k), kv.StrKey(scan.Value()), kv.StrKey(expectedKey), keyOnly)
			if !keyOnly {
				v := scan.Value()
				s.Equal(v, s.makeValue(i))
			}
			// Because newScan return first item without calling scan.Next() just like go-hbase,
			// for-loop count will decrease 1.
			if i < rowNum-1 {
				scan.Next()
			}
		}
		scan.Next()
		s.False(scan.Valid())
	}

	for _, rowNum := range s.rowNums {
		txn := s.beginTxn()
		for i := 0; i < rowNum; i++ {
			err := txn.Set(s.makeKey(i), s.makeValue(i))
			s.Nil(err)
		}
		err := txn.Commit(context.Background())
		s.Nil(err)
		mockTableID := int64(999)
		if rowNum > 123 {
			_, err = s.store.SplitRegions(context.Background(), [][]byte{s.makeKey(123)}, false, &mockTableID)
			s.Nil(err)
		}

		if rowNum > 456 {
			_, err = s.store.SplitRegions(context.Background(), [][]byte{s.makeKey(456)}, false, &mockTableID)
			s.Nil(err)
		}

		txn2 := s.beginTxn()
		val, err := txn2.Get(context.TODO(), s.makeKey(0))
		s.Nil(err)
		s.Equal(val, s.makeValue(0))
		// Test scan without upperBound
		scan, err := txn2.Iter(s.recordPrefix, nil)
		s.Nil(err)
		check(scan, rowNum, false)
		// Test scan with upperBound
		upperBound := rowNum / 2
		scan, err = txn2.Iter(s.recordPrefix, s.makeKey(upperBound))
		s.Nil(err)
		check(scan, upperBound, false)

		txn3 := s.beginTxn()
		txn3.GetSnapshot().SetKeyOnly(true)
		// Test scan without upper bound
		scan, err = txn3.Iter(s.recordPrefix, nil)
		s.Nil(err)
		check(scan, rowNum, true)
		// test scan with upper bound
		scan, err = txn3.Iter(s.recordPrefix, s.makeKey(upperBound))
		s.Nil(err)
		check(scan, upperBound, true)

		// Restore KeyOnly to false
		txn3.GetSnapshot().SetKeyOnly(false)
		scan, err = txn3.Iter(s.recordPrefix, nil)
		s.Nil(err)
		check(scan, rowNum, true)
		// test scan with upper bound
		scan, err = txn3.Iter(s.recordPrefix, s.makeKey(upperBound))
		s.Nil(err)
		check(scan, upperBound, true)
	}
}
