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

	"github.com/pingcap/log"
	"github.com/spf13/cobra"
	"github.com/tikv/client-go/v2/rawkv"
	"go.uber.org/zap"
)

func isReadCommand(cmd string) bool {
	return cmd == config.RawKVCommandTypeGet || cmd == config.RawKVCommandTypeBatchGet
}

func getRawKvConfig(ctx context.Context) *config.RawKVConfig {
	c := ctx.Value(config.WorkloadTypeRawKV).(*config.RawKVConfig)
	return c
}

func convertCfName(cf string) string {
	switch cf {
	case "default":
		return config.WorkloadColumnFamilyDefault
	case "write":
	case "lock":
		fmt.Printf("Column family %s is not supported, use default instead\n", cf)
		return config.WorkloadColumnFamilyDefault
	default:
		return cf
	}
	return config.WorkloadColumnFamilyDefault
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

	timeoutCtx, cancel := context.WithTimeout(workloads.GlobalContext, rawKVConfig.Global.TotalTime)
	workloads.GlobalContext = timeoutCtx
	defer cancel()

	workload.Execute(cmd)
	fmt.Println("RawKV workload finished")
	workload.OutputStats(true)
}

// Register registers the workload to the command line parser
func Register(command *config.CommandLineParser) *config.RawKVConfig {
	if command == nil {
		return nil
	}
	rawKVConfig := &config.RawKVConfig{
		Global: command.GetConfig(),
	}

	cmd := &cobra.Command{
		Use: config.WorkloadTypeRawKV,
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			if err := rawKVConfig.Global.InitLogger(); err != nil {
				log.Error("InitLogger failed", zap.Error(err))
			}
			rawKVConfig.ColumnFamily = convertCfName(rawKVConfig.ColumnFamily)
			workloads.GlobalContext = context.WithValue(workloads.GlobalContext, config.WorkloadTypeRawKV, rawKVConfig)
		},
	}
	cmd.PersistentFlags().StringVar(&rawKVConfig.ColumnFamily, "cf", "default", "Column family name (default|write|lock)")
	cmd.PersistentFlags().StringVar(&rawKVConfig.CommandType, "cmd", "put", "Type of command to execute (put|get|del|batch_put|batch_get|batch_del|scan|reserve_scan|cas)")
	cmd.PersistentFlags().IntVar(&rawKVConfig.KeySize, "key-size", 1, "Size of key in bytes")
	cmd.PersistentFlags().IntVar(&rawKVConfig.ValueSize, "value-size", 1, "Size of value in bytes")
	cmd.PersistentFlags().IntVar(&rawKVConfig.BatchSize, "batch-size", 1, "Size of batch for batch operations")
	cmd.PersistentFlags().BoolVar(&rawKVConfig.Randomize, "random", false, "Whether to randomize each value")

	var cmdPrepare = &cobra.Command{
		Use:   "prepare",
		Short: "Prepare data for RawKV workload",
		Run: func(cmd *cobra.Command, _ []string) {
			execRawKV("prepare")
		},
	}
	cmdPrepare.PersistentFlags().IntVar(&rawKVConfig.PrepareRetryCount, "retry-count", 50, "Retry count when errors occur")
	cmdPrepare.PersistentFlags().DurationVar(&rawKVConfig.PrepareRetryInterval, "retry-interval", 10*time.Millisecond, "The interval for each retry")

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

func RunRawKVCommand(ctx context.Context, client *rawkv.Client, commandType string, keySize int, valueSize int, batchSize int, randomize bool, stats *statistics.PerfProfile, ignoreErr bool) error {
	// For unary operations.
	key := config.RawKVCommandDefaultKey
	val := config.RawKVCommandDefaultValue

	// For batch operations.
	var (
		keys [][]byte
		vals [][]byte
		err  error
	)
	switch commandType {
	case config.RawKVCommandTypePut, config.RawKVCommandTypeGet, config.RawKVCommandTypeDel, config.RawKVCommandTypeCAS, config.RawKVCommandTypeScan, config.RawKVCommandTypeReverseScan:
		if randomize {
			key = utils.GenRandomStr(config.RawKVCommandDefaultKey, keySize)
			if !isReadCommand(commandType) {
				val = utils.GenRandomStr(config.RawKVCommandDefaultValue, valueSize)
			}
		}
	case config.RawKVCommandTypeBatchPut, config.RawKVCommandTypeBatchGet, config.RawKVCommandTypeBatchDel:
		if randomize {
			keys = utils.GenRandomByteArrs(config.RawKVCommandDefaultKey, keySize, batchSize)
			if !isReadCommand(commandType) {
				vals = utils.GenRandomByteArrs(config.RawKVCommandDefaultValue, valueSize, batchSize)
			}
		}
	}

	start := time.Now()
	switch commandType {
	case config.RawKVCommandTypePut:
		err = client.Put(ctx, []byte(key), []byte(val))
	case config.RawKVCommandTypeGet:
		_, err = client.Get(ctx, []byte(key))
	case config.RawKVCommandTypeDel:
		err = client.Delete(ctx, []byte(key))
	case config.RawKVCommandTypeBatchPut:
		err = client.BatchPut(ctx, keys, vals)
	case config.RawKVCommandTypeBatchGet:
		_, err = client.BatchGet(ctx, keys)
	case config.RawKVCommandTypeBatchDel:
		err = client.BatchDelete(ctx, keys)
	case config.RawKVCommandTypeCAS:
		var oldVal []byte
		oldVal, _ = client.Get(ctx, []byte(key))
		_, _, err = client.CompareAndSwap(ctx, []byte(key), []byte(oldVal), []byte(val)) // Experimental
	case config.RawKVCommandTypeScan:
		_, _, err = client.Scan(ctx, []byte(key), []byte(config.RawKVCommandDefaultEndKey), batchSize)
	case config.RawKVCommandTypeReverseScan:
		_, _, err = client.ReverseScan(ctx, []byte(key), []byte(config.RawKVCommandDefaultKey), batchSize)
	}
	if err != nil && !ignoreErr {
		return fmt.Errorf("execute %s failed: %v", commandType, err)
	}
	stats.Record(commandType, time.Since(start))
	return nil
}

type WorkloadImpl struct {
	cfg     *config.RawKVConfig
	clients []*rawkv.Client

	wait sync.WaitGroup

	stats *statistics.PerfProfile
}

func NewRawKVWorkload(cfg *config.RawKVConfig) (*WorkloadImpl, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	w := &WorkloadImpl{
		cfg:   cfg,
		stats: statistics.NewPerfProfile(),
	}

	w.clients = make([]*rawkv.Client, 0, w.cfg.Global.Threads)
	for i := 0; i < w.cfg.Global.Threads; i++ {
		client, err := rawkv.NewClient(workloads.GlobalContext, w.cfg.Global.Targets, w.cfg.Global.Security)
		if err != nil {
			return nil, err
		}
		w.clients = append(w.clients, client)
	}
	return w, nil
}

func (w *WorkloadImpl) Name() string {
	return config.WorkloadTypeRawKV
}

func (w *WorkloadImpl) isValid() bool {
	return w.cfg != nil && w.cfg.Global != nil && len(w.clients) > 0
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
	client.SetAtomicForCAS(w.cfg.CommandType == config.RawKVCommandTypeCAS)
	client.SetColumnFamily(w.cfg.ColumnFamily)
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
	return RunRawKVCommand(ctx, w.clients[threadID], w.cfg.CommandType, w.cfg.KeySize, w.cfg.ValueSize, w.cfg.BatchSize, w.cfg.Randomize, w.stats, w.cfg.Global.IgnoreError)
}

// Check implements WorkloadInterface
func (w *WorkloadImpl) Check(ctx context.Context, threadID int) error {
	if !w.isValidThread(threadID) {
		return fmt.Errorf("no valid RawKV clients")
	}
	if threadID == 0 {
		client := w.clients[threadID]
		checksum, err := client.Checksum(ctx, []byte(config.RawKVCommandDefaultKey), []byte(config.RawKVCommandDefaultEndKey))
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
		client.DeleteRange(ctx, []byte(config.RawKVCommandDefaultKey), []byte(config.RawKVCommandDefaultEndKey)) // delete all keys
	}
	return nil
}

func (w *WorkloadImpl) OutputStats(ifSummaryReport bool) {
	w.stats.PrintFmt(ifSummaryReport, w.cfg.Global.OutputStyle, statistics.HistogramOutputFunc)
}

func (w *WorkloadImpl) Execute(cmd string) {
	w.wait.Add(w.cfg.Global.Threads)

	ctx, cancel := context.WithCancel(workloads.GlobalContext)
	ch := make(chan struct{}, 1)
	go func() {
		ticker := time.NewTicker(w.cfg.Global.OutputInterval)
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

	count := w.cfg.Global.TotalCount / w.cfg.Global.Threads
	for i := 0; i < w.cfg.Global.Threads; i++ {
		go func(index int) {
			defer w.wait.Done()
			if err := workloads.DispatchExecution(ctx, w, cmd, count, index, w.cfg.Global.Silence, w.cfg.Global.IgnoreError); err != nil {
				fmt.Printf("[%s] execute %s failed, err %v\n", time.Now().Format("2006-01-02 15:04:05"), cmd, err)
				return
			}
		}(i)
	}

	w.wait.Wait()
	cancel()
	<-ch
}
