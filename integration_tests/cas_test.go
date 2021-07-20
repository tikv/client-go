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

package tikv_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/rawkv"
)

func TestCAS(t *testing.T) {
	suite.Run(t, new(casTestSuite))
}

type casTestSuite struct {
	suite.Suite
	client *rawkv.Client
}

func (s *casTestSuite) SetupTest() {
	t := s.T()
	if !*withTiKV {
		t.Skip("skipping TestCAS because with-tikv is not enabled")
	}
	addrs := strings.Split(*pdAddrs, ",")
	client, err := rawkv.NewClient(context.TODO(), addrs, config.DefaultConfig().Security)
	require.Nil(s.T(), err)
	s.client = client
}

func (s *casTestSuite) mustCompareAndSwap(key, previousValue, newValue []byte) ([]byte, bool) {
	previousValue, succeed, err := s.client.
		SetAtomicForCAS(true).
		CompareAndSwap(context.TODO(), key, previousValue, newValue)
	s.Nil(err)
	return previousValue, succeed
}

func (s *casTestSuite) mustGet(key []byte) []byte {
	v, err := s.client.Get(context.TODO(), key)
	s.Nil(err)
	return v
}

func (s *casTestSuite) mustPut(key, value []byte) {
	err := s.client.Put(context.TODO(), key, value)
	s.Nil(err)
}

func (s *casTestSuite) mustDelete(key []byte) {
	err := s.client.Delete(context.TODO(), key)
	s.Nil(err)
}

func (s *casTestSuite) TestBasic() {
	key := []byte("key")
	s.mustDelete(key)

	initial := []byte("value-0")
	prev, succeed := s.mustCompareAndSwap(key, nil, initial)
	s.Nil(prev)
	s.True(succeed)
	s.Equal(initial, s.mustGet(key))

	another := []byte("value-1")
	s.mustPut(key, another)
	prev, succeed = s.mustCompareAndSwap(key, another, initial)
	s.True(succeed)
	s.Equal(another, prev)

	prev, succeed = s.mustCompareAndSwap(key, another, initial)
	s.False(succeed)
	s.Equal(initial, prev)
}

func (s *casTestSuite) TearDownTest() {
	s.client.Close()
}
