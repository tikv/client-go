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

package rawkv

import (
	"benchtool/config"
	"benchtool/utils"
	"benchtool/utils/statistics"
	"benchtool/workloads"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/spf13/cobra"
	"github.com/tikv/client-go/v2/rawkv"
)

const (
	WorkloadImplName = "rawkv"

	WorkloadTypePut            = "put"
	WorkloadTypeGet            = "get"
	WorkloadTypeDel            = "del"
	WorkloadTypeBatchPut       = "batch_put"
	WorkloadTypeBatchGet       = "batch_get"
	WorkloadTypeBatchDel       = "batch_del"
	WorkloadTypeScan           = "scan"
	WorkloadTypeReverseScan    = "reverse_scan"
	WorkloadTypeCompareAndSwap = "cas"

	WorkloadDefaultKey    = "rawkv_key"
	WorkloadDefaultEndKey = "rawkv_key`"
	WorkloadDefaultValue  = "rawkv_value"

	WorkloadColumnFamilyDefault = "CF_DEFAULT"
	WorkloadColumnFamilyWrite   = "CF_WRITE"
	WorkloadColumnFamilyLock    = "CF_LOCK"
)

func isReadCommand(cmd string) bool {
	return cmd == WorkloadTypeGet || cmd == WorkloadTypeBatchGet
}

type RawKVConfig struct {
	keySize   int
	valueSize int
	batchSize int

	columnFamily         string
	commandType          string
	prepareRetryCount    int
	prepareRetryInterval time.Duration
	randomize            bool

	global *config.GlobalConfig
}

func (c *RawKVConfig) Validate() error {
	if c.keySize <= 0 || c.valueSize <= 0 {
		return fmt.Errorf("key size or value size must be greater than 0")
	}
	if c.columnFamily != WorkloadColumnFamilyDefault &&
		c.columnFamily != WorkloadColumnFamilyWrite &&
		c.columnFamily != WorkloadColumnFamilyLock {
		return fmt.Errorf("invalid column family: %s", c.columnFamily)
	}
	return c.global.ParsePdAddrs()
}

// Register registers the workload to the command line parser
func Register(command *config.CommandLineParser) *RawKVConfig {
	if command == nil {
		return nil
	}
	rawKVConfig := &RawKVConfig{
		global: command.GetConfig(),
	}

	cmd := &cobra.Command{
		Use: WorkloadImplName,
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			workloads.GlobalContext = context.WithValue(workloads.GlobalContext, WorkloadImplName, rawKVConfig)
		},
	}
	cmd.PersistentFlags().StringVar(&rawKVConfig.columnFamily, "cf", "default", "Column family name (default|write|lock)")
	cmd.PersistentFlags().StringVar(&rawKVConfig.commandType, "cmd", "put", "Type of command to execute (put|get|del|batch_put|batch_get|batch_del|scan|reserve_scan|cas)")
	cmd.PersistentFlags().IntVar(&rawKVConfig.keySize, "key-size", 1, "Size of key in bytes")
	cmd.PersistentFlags().IntVar(&rawKVConfig.valueSize, "value-size", 1, "Size of value in bytes")
	cmd.PersistentFlags().IntVar(&rawKVConfig.batchSize, "batch-size", 1, "Size of batch for batch operations")
	cmd.PersistentFlags().BoolVar(&rawKVConfig.randomize, "random", false, "Whether to randomize each value")

	var cmdPrepare = &cobra.Command{
		Use:   "prepare",
		Short: "Prepare data for RawKV workload",
		Run: func(cmd *cobra.Command, _ []string) {
			execRawKV("prepare")
		},
	}
	cmdPrepare.PersistentFlags().IntVar(&rawKVConfig.prepareRetryCount, "retry-count", 50, "Retry count when errors occur")
	cmdPrepare.PersistentFlags().DurationVar(&rawKVConfig.prepareRetryInterval, "retry-interval", 10*time.Millisecond, "The interval for each retry")

	var cmdRun = &cobra.Command{
		Use:   "run",
		Short: "Run workload",
		Run: func(cmd *cobra.Command, _ []string) {
			execRawKV("run")
		},
	}

	var cmdCleanup = &cobra.Command{
		Use:   "cleanup",
		Short: "Cleanup data for the workload",
		Run: func(cmd *cobra.Command, _ []string) {
			execRawKV("cleanup")
		},
	}

	var cmdCheck = &cobra.Command{
		Use:   "check",
		Short: "Check data consistency for the workload",
		Run: func(cmd *cobra.Command, _ []string) {
			execRawKV("check")
		},
	}

	cmd.AddCommand(cmdRun, cmdPrepare, cmdCleanup, cmdCheck)

	command.GetCommand().AddCommand(cmd)

	return rawKVConfig
}

func getRawKvConfig(ctx context.Context) *RawKVConfig {
	c := ctx.Value(WorkloadImplName).(*RawKVConfig)
	return c
}

type WorkloadImpl struct {
	cfg     *RawKVConfig
	clients []*rawkv.Client

	wait sync.WaitGroup

	stats *statistics.PerfProfile
}

func NewRawKVWorkload(cfg *RawKVConfig) (*WorkloadImpl, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	w := &WorkloadImpl{
		cfg:   cfg,
		stats: statistics.NewPerfProfile(),
	}

	w.clients = make([]*rawkv.Client, 0, w.cfg.global.Threads)
	for i := 0; i < w.cfg.global.Threads; i++ {
		client, err := rawkv.NewClient(workloads.GlobalContext, w.cfg.global.Targets, w.cfg.global.Security)
		if err != nil {
			return nil, err
		}
		w.clients = append(w.clients, client)
	}
	return w, nil
}

func (w *WorkloadImpl) Name() string {
	return WorkloadImplName
}

func (w *WorkloadImpl) isValid() bool {
	return w.cfg != nil && w.cfg.global != nil && len(w.clients) > 0
}

func (w *WorkloadImpl) isValidThread(threadID int) bool {
	return w.isValid() && threadID < len(w.clients)
}

// InitThread implements WorkloadInterface
func (w *WorkloadImpl) InitThread(ctx context.Context, threadID int) error {
	// Nothing to do
	if !w.isValidThread(threadID) {
		return fmt.Errorf("no valid RawKV clients")
	}
	client := w.clients[threadID]
	client.SetAtomicForCAS(w.cfg.commandType == WorkloadTypeCompareAndSwap)
	client.SetColumnFamily(w.cfg.columnFamily)
	return nil
}

// CleanupThread implements WorkloadInterface
func (w *WorkloadImpl) CleanupThread(ctx context.Context, threadID int) {
	if w.isValidThread(threadID) {
		client := w.clients[threadID]
		if client != nil {
			client.Close()
		}
	}
}

// Prepare implements WorkloadInterface
func (w *WorkloadImpl) Prepare(ctx context.Context, threadID int) error {
	if !w.isValidThread(threadID) {
		return fmt.Errorf("no valid RawKV clients")
	}

	// return prepareWorkloadImpl(ctx, w, w.cfg.Threads, w.cfg.Warehouses, threadID)
	// TODO: add prepare stage
	return nil
}

// CheckPrepare implements WorkloadInterface
func (w *WorkloadImpl) CheckPrepare(ctx context.Context, threadID int) error {
	return nil
}

func (w *WorkloadImpl) Run(ctx context.Context, threadID int) error {
	if !w.isValidThread(threadID) {
		return fmt.Errorf("no valid RawKV clients")
	}

	client := w.clients[threadID]

	// For unary operations.
	key := WorkloadDefaultKey
	val := WorkloadDefaultValue

	// For batch operations.
	var (
		keys [][]byte
		vals [][]byte
		err  error
	)
	switch w.cfg.commandType {
	case WorkloadTypePut, WorkloadTypeGet, WorkloadTypeDel, WorkloadTypeCompareAndSwap, WorkloadTypeScan, WorkloadTypeReverseScan:
		if w.cfg.randomize {
			key = utils.GenRandomStr(WorkloadDefaultKey, w.cfg.keySize)
			if !isReadCommand(w.cfg.commandType) {
				val = utils.GenRandomStr(WorkloadDefaultValue, w.cfg.valueSize)
			}
		}
	case WorkloadTypeBatchPut, WorkloadTypeBatchGet, WorkloadTypeBatchDel:
		if w.cfg.randomize {
			keys = utils.GenRandomByteArrs(WorkloadDefaultKey, w.cfg.keySize, w.cfg.batchSize)
			if !isReadCommand(w.cfg.commandType) {
				vals = utils.GenRandomByteArrs(WorkloadDefaultValue, w.cfg.valueSize, w.cfg.batchSize)
			}
		}
	}

	start := time.Now()
	switch w.cfg.commandType {
	case WorkloadTypePut:
		err = client.Put(ctx, []byte(key), []byte(val))
	case WorkloadTypeGet:
		_, err = client.Get(ctx, []byte(key))
	case WorkloadTypeDel:
		err = client.Delete(ctx, []byte(key))
	case WorkloadTypeBatchPut:
		err = client.BatchPut(ctx, keys, vals)
	case WorkloadTypeBatchGet:
		_, err = client.BatchGet(ctx, keys)
	case WorkloadTypeBatchDel:
		err = client.BatchDelete(ctx, keys)
	case WorkloadTypeCompareAndSwap:
		var oldVal []byte
		oldVal, _ = client.Get(ctx, []byte(key))
		_, _, err = client.CompareAndSwap(ctx, []byte(key), []byte(oldVal), []byte(val)) // Experimental
	case WorkloadTypeScan:
		_, _, err = client.Scan(ctx, []byte(key), []byte(WorkloadDefaultEndKey), w.cfg.batchSize)
	case WorkloadTypeReverseScan:
		_, _, err = client.ReverseScan(ctx, []byte(key), []byte(WorkloadDefaultKey), w.cfg.batchSize)
	}
	if err != nil && !w.cfg.global.IgnoreError {
		return fmt.Errorf("execute %s failed: %v", w.cfg.commandType, err)
	}
	w.stats.Record(w.cfg.commandType, time.Since(start))
	return nil
}

// Check implements WorkloadInterface
func (w *WorkloadImpl) Check(ctx context.Context, threadID int) error {
	if !w.isValidThread(threadID) {
		return fmt.Errorf("no valid RawKV clients")
	}
	if threadID == 0 {
		client := w.clients[threadID]
		checksum, err := client.Checksum(ctx, []byte(WorkloadDefaultKey), []byte(WorkloadDefaultEndKey))
		if err != nil {
			return nil
		} else {
			fmt.Printf("RawKV checksum: %d\n", checksum)
		}
	}
	return nil
}

// Cleanup implements WorkloadInterface
func (w *WorkloadImpl) Cleanup(ctx context.Context, threadID int) error {
	if !w.isValidThread(threadID) {
		return fmt.Errorf("no valid RawKV clients")
	}
	if threadID == 0 {
		client := w.clients[threadID]
		client.DeleteRange(ctx, []byte(WorkloadDefaultKey), []byte(WorkloadDefaultEndKey)) // delete all keys
	}
	return nil
}

func (w *WorkloadImpl) OutputStats(ifSummaryReport bool) {
	w.stats.PrintFmt(ifSummaryReport, w.cfg.global.OutputStyle, statistics.HistogramOutputFunc)
}

func (w *WorkloadImpl) Execute(cmd string) {
	w.wait.Add(w.cfg.global.Threads)

	ctx, cancel := context.WithCancel(workloads.GlobalContext)
	ch := make(chan struct{}, 1)
	go func() {
		ticker := time.NewTicker(w.cfg.global.OutputInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				ch <- struct{}{}
				return
			case <-ticker.C:
				w.OutputStats(false)
			}
		}
	}()

	count := w.cfg.global.TotalCount / w.cfg.global.Threads
	for i := 0; i < w.cfg.global.Threads; i++ {
		go func(index int) {
			defer w.wait.Done()
			if err := workloads.DispatchExecution(ctx, w, cmd, count, index, w.cfg.global.Silence, w.cfg.global.IgnoreError); err != nil {
				fmt.Printf("[%s] execute %s failed, err %v\n", time.Now().Format("2006-01-02 15:04:05"), cmd, err)
				return
			}
		}(i)
	}

	w.wait.Wait()
	cancel()
	<-ch
}

func execRawKV(cmd string) {
	if cmd == "" {
		return
	}
	rawKVConfig := getRawKvConfig(workloads.GlobalContext)

	var workload *WorkloadImpl
	var err error
	if workload, err = NewRawKVWorkload(rawKVConfig); err != nil {
		fmt.Printf("create RawKV workload failed: %v\n", err)
		return
	}

	timeoutCtx, cancel := context.WithTimeout(workloads.GlobalContext, rawKVConfig.global.TotalTime)
	workloads.GlobalContext = timeoutCtx
	defer cancel()

	workload.Execute(cmd)
	fmt.Println("RawKV workload finished")
	workload.OutputStats(true)
}
