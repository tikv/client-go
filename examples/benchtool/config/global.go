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
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/spf13/cobra"
	"github.com/tikv/client-go/v2/config"
	"go.uber.org/zap"
)

var initOnce = sync.Once{}

const (
	WorkloadColumnFamilyDefault = "default"
	WorkloadColumnFamilyWrite   = "write"
	WorkloadColumnFamilyLock    = "lock"
)

type GlobalConfig struct {
	hosts      []string
	port       int
	StatusPort int

	Threads        int
	TotalTime      time.Duration
	TotalCount     int
	DropData       bool
	IgnoreError    bool
	OutputInterval time.Duration
	Silence        bool
	OutputStyle    string

	Targets  []string
	Security config.Security

	// for log
	LogLevel string
	LogFile  string
}

func (c *GlobalConfig) ParsePdAddrs() error {
	if len(c.hosts) == 0 {
		return fmt.Errorf("PD address is empty")
	}
	targets := make([]string, 0, len(c.hosts))
	for _, host := range c.hosts {
		targets = append(targets, host+":"+strconv.Itoa(c.port))
	}
	c.Targets = targets
	return nil
}

func (c *GlobalConfig) Format() string {
	return fmt.Sprintf("Hosts: %v, Port: %d, StatusPort: %d, Threads: %d, TotalTime: %v, TotalCount: %d, DropData: %t, IgnoreError: %t, OutputInterval: %v, Silence: %t, OutputStyle: %s",
		c.hosts, c.port, c.StatusPort, c.Threads, c.TotalTime, c.TotalCount, c.DropData, c.IgnoreError, c.OutputInterval, c.Silence, c.OutputStyle)
}

func (c *GlobalConfig) InitLogger() (err error) {
	initOnce.Do(func() {
		// Initialize the logger.
		conf := &log.Config{
			Level: c.LogLevel,
			File: log.FileLogConfig{
				Filename: c.LogFile,
				MaxSize:  256,
			},
		}
		lg, p, e := log.InitLogger(conf)
		if e != nil {
			err = e
			return
		}
		log.ReplaceGlobals(lg, p)
	})
	return errors.Trace(err)
}

type CommandLineParser struct {
	command *cobra.Command
	config  *GlobalConfig
}

func NewCommandLineParser() *CommandLineParser {
	return &CommandLineParser{}
}

func (p *CommandLineParser) Initialize() {
	var globalCfg = &GlobalConfig{}
	var rootCmd = &cobra.Command{
		Use:   "bench-tool",
		Short: "Benchmark tikv with different workloads",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			if err := globalCfg.InitLogger(); err != nil {
				log.Error("InitLogger failed", zap.Error(err))
			}
		},
	}

	rootCmd.PersistentFlags().StringSliceVarP(&globalCfg.hosts, "host", "H", []string{"127.0.0.1"}, "PD host")
	rootCmd.PersistentFlags().IntVarP(&globalCfg.port, "port", "P", 4000, "PD port")
	rootCmd.PersistentFlags().IntVarP(&globalCfg.StatusPort, "statusPort", "S", 10080, "PD status port")

	rootCmd.PersistentFlags().IntVarP(&globalCfg.Threads, "threads", "T", 1, "Thread concurrency")
	rootCmd.PersistentFlags().DurationVar(&globalCfg.TotalTime, "time", 1<<63-1, "Total execution time")
	rootCmd.PersistentFlags().IntVar(&globalCfg.TotalCount, "count", 0, "Total execution count, 0 means infinite")
	rootCmd.PersistentFlags().BoolVar(&globalCfg.DropData, "dropdata", false, "Cleanup data before prepare")
	rootCmd.PersistentFlags().BoolVar(&globalCfg.IgnoreError, "ignore-error", false, "Ignore error when running workload")
	rootCmd.PersistentFlags().BoolVar(&globalCfg.Silence, "silence", false, "Don't print error when running workload")
	rootCmd.PersistentFlags().DurationVar(&globalCfg.OutputInterval, "interval", 10*time.Second, "Output interval time")
	rootCmd.PersistentFlags().StringVar(&globalCfg.OutputStyle, "output", "plain", "output style, valid values can be { plain | table | json }")

	rootCmd.PersistentFlags().StringVar(&globalCfg.LogFile, "log-file", "record.log", "filename of the log file")
	rootCmd.PersistentFlags().StringVar(&globalCfg.LogLevel, "log-level", "info", "log level { debug | info | warn | error | fatal }")

	rootCmd.SetOut(os.Stdout)

	cobra.EnablePrefixMatching = true

	p.command = rootCmd
	p.config = globalCfg
}

func (p *CommandLineParser) GetConfig() *GlobalConfig {
	return p.config
}

func (p *CommandLineParser) GetCommand() *cobra.Command {
	return p.command
}
