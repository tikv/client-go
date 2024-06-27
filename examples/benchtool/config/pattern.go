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
	"os"

	"gopkg.in/yaml.v3"
)

const (
	WorkloadTypeHybrid = "hybrid"
)

// TODO: convert the txnConfig and rawkvConfig to interfaces
type SubPatternConfig struct {
	txnConfig   *TxnKVConfig
	rawkvConfig *RawKVConfig
	workloads   []string
	name        string
}

func (s *SubPatternConfig) GetName() string {
	return s.name
}

func (s *SubPatternConfig) GetWorkloads() []string {
	return s.workloads
}

func (s *SubPatternConfig) GetTxnKVConfig() *TxnKVConfig {
	return s.txnConfig
}

func (s *SubPatternConfig) GetRawKVConfig() *RawKVConfig {
	return s.rawkvConfig
}

type SubPattern struct {
	Name         string   `yaml:"name,omitempty"`
	WorkloadType string   `yaml:"workload_type,omitempty"`
	Workloads    []string `yaml:"workloads,omitempty"`

	// for txnkv
	Mode        string `yaml:"mode,omitempty"`
	LockTimeout int    `yaml:"lock_timeout"`
	ColumnSize  int    `yaml:"column_size"`
	TxnSize     int    `yaml:"txn_size"`
	// common
	Count     int    `yaml:"count"`
	KeyPrefix string `yaml:"key_prefix,omitempty"`
	KeySize   int    `yaml:"key_size"`
	ValueSize int    `yaml:"value_size"`
	BatchSize int    `yaml:"batch_size"`
	Threads   int    `yaml:"threads"`
	Randomize bool   `yaml:"random"`
}

func (s *SubPattern) ConvertBasedOn(global *GlobalConfig) *SubPatternConfig {
	// Invalid workloads
	if s.Workloads == nil {
		return nil
	}

	globalCfg := &GlobalConfig{}
	if global != nil {
		globalCfg = global
	}
	globalCfg.TotalCount = s.Count
	globalCfg.Threads = s.Threads

	switch s.WorkloadType {
	case WorkloadTypeTxnKV:
		config := &TxnKVConfig{
			TxnMode:     s.Mode,
			LockTimeout: s.LockTimeout,
			// KeyPrefix:   s.key_prefix,
			KeySize:    s.KeySize,
			ValueSize:  s.ValueSize,
			ColumnSize: s.ColumnSize,
			TxnSize:    s.TxnSize,
		}
		config.Global = globalCfg
		return &SubPatternConfig{
			txnConfig: config,
			workloads: s.Workloads,
			name:      s.Name,
		}
	case WorkloadTypeRawKV:
		config := &RawKVConfig{
			// KeyPrefix: s.key_prefix,
			KeySize:   s.KeySize,
			BatchSize: s.BatchSize,
			ValueSize: s.ValueSize,
			Randomize: s.Randomize,
		}
		config.Global = globalCfg
		return &SubPatternConfig{
			rawkvConfig: config,
			workloads:   s.Workloads,
			name:        s.Name,
		}
	}
	return nil
}

type PatternsConfig struct {
	Items []*SubPattern `yaml:"patterns"`

	FilePath string

	Plans  []*SubPatternConfig
	Global *GlobalConfig
}

// Parse parses the yaml file.
func (p *PatternsConfig) Parse() error {
	data, err := os.ReadFile(p.FilePath)
	if err != nil {
		return err
	}
	err = yaml.Unmarshal(data, p)
	if err != nil {
		return err
	}
	p.Plans = make([]*SubPatternConfig, 0, len(p.Items))
	for _, item := range p.Items {
		p.Plans = append(p.Plans, item.ConvertBasedOn(p.Global))
	}
	return nil
}

func (p *PatternsConfig) Validate() error {
	if p.Global == nil {
		return fmt.Errorf("global config is missing")
	}
	if p.Items == nil {
		return fmt.Errorf("patterns config is missing")
	}
	return p.Global.ParsePdAddrs()
}
