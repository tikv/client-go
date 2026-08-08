// Copyright 2024 TiKV Authors
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

package config

import (
	"fmt"
	"time"
)

const (
	WorkloadTypeRawKV = "rawkv"
)

const (
	RawKVCommandTypePut         = "put"
	RawKVCommandTypeGet         = "get"
	RawKVCommandTypeDel         = "del"
	RawKVCommandTypeBatchPut    = "batch_put"
	RawKVCommandTypeBatchGet    = "batch_get"
	RawKVCommandTypeBatchDel    = "batch_del"
	RawKVCommandTypeScan        = "scan"
	RawKVCommandTypeReverseScan = "reverse_scan"
	RawKVCommandTypeCAS         = "cas"

	RawKVCommandDefaultKey    = "rawkv_key"
	RawKVCommandDefaultEndKey = "rawkv_key`"
	RawKVCommandDefaultValue  = "rawkv_value"
)

type RawKVConfig struct {
	KeySize   int
	ValueSize int
	BatchSize int

	ColumnFamily         string
	CommandType          string
	PrepareRetryCount    int
	PrepareRetryInterval time.Duration
	Randomize            bool

	Global *GlobalConfig
}

func (c *RawKVConfig) Validate() error {
	if c.KeySize <= 0 || c.ValueSize <= 0 {
		return fmt.Errorf("key size or value size must be greater than 0")
	}
	if c.ColumnFamily != WorkloadColumnFamilyDefault &&
		c.ColumnFamily != WorkloadColumnFamilyWrite &&
		c.ColumnFamily != WorkloadColumnFamilyLock {
		return fmt.Errorf("invalid column family: %s", c.ColumnFamily)
	}
	return c.Global.ParsePdAddrs()
}
