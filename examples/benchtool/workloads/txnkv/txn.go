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

package txnkv

import (
	"benchtool/config"
	"benchtool/utils"
	"benchtool/utils/statistics"
	"benchtool/workloads"
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/spf13/cobra"
	clientConfig "github.com/tikv/client-go/v2/config"
	tikverr "github.com/tikv/client-go/v2/error"
	clientTxnKV "github.com/tikv/client-go/v2/txnkv"
)

const (
	WorkloadImplName = "txnkv"

	WorkloadTypeWrite = "write"
	WorkloadTypeRead  = "read"

	WorkloadDefaultKey    = "txnkv_key"
	WorkloadDefaultEndKey = "txnkv_key`"
	WorkloadDefaultValue  = "txnkv_value"

	WorkloadTxnModeDefault     = "2PC"
	WorkloadTxnMode1PC         = "1PC"
	WorkloadTxnModeAsyncCommit = "async-commit"
)

// Register registers the workload to the command line parser
func Register(command *config.CommandLineParser) *config.TxnKVConfig {
	if command == nil {
		return nil
	}
	txnKVConfig := &config.TxnKVConfig{
		Global:         command.GetConfig(),
		ReadWriteRatio: utils.NewReadWriteRatio("1:1"), // TODO: generate workloads meeting the read-write ratio
	}

	cmd := &cobra.Command{
		Use: WorkloadImplName,
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			workloads.GlobalContext = context.WithValue(workloads.GlobalContext, WorkloadImplName, txnKVConfig)
		},
	}
	cmd.PersistentFlags().StringVar(&txnKVConfig.ReadWriteRatio.Ratio, "read-write-ratio", "1:1", "Read write ratio")
	cmd.PersistentFlags().IntVar(&txnKVConfig.KeySize, "key-size", 1, "Size of key in bytes")
	cmd.PersistentFlags().IntVar(&txnKVConfig.ValueSize, "value-size", 1, "Size of value in bytes")
	cmd.PersistentFlags().IntVar(&txnKVConfig.ColumnSize, "column-size", 1, "Size of column")
	cmd.PersistentFlags().IntVar(&txnKVConfig.TxnSize, "txn-size", 1, "Size of transaction (normally, the lines of kv pairs)")
	cmd.PersistentFlags().StringVar(&txnKVConfig.TxnMode, "txn-mode", "2PC", "Mode of transaction (2PC/1PC/async-commit)")
	cmd.PersistentFlags().IntVar(&txnKVConfig.LockTimeout, "lock-timeout", 0, "Lock timeout for each key in txn (>0 means pessimistic mode, 0 means optimistic mode)")
	// TODO: add more flags on txn, such as pessimistic/optimistic lock, etc.

	var cmdPrepare = &cobra.Command{
		Use:   "prepare",
		Short: "Prepare data for TxnKV workload",
		Run: func(cmd *cobra.Command, _ []string) {
			execTxnKV("prepare")
		},
	}
	cmdPrepare.PersistentFlags().IntVar(&txnKVConfig.PrepareRetryCount, "retry-count", 50, "Retry count when errors occur")
	cmdPrepare.PersistentFlags().DurationVar(&txnKVConfig.PrepareRetryInterval, "retry-interval", 10*time.Millisecond, "The interval for each retry")

	var cmdRun = &cobra.Command{
		Use:   "run",
		Short: "Run workload",
		Run: func(cmd *cobra.Command, _ []string) {
			execTxnKV("run")
		},
	}

	var cmdCleanup = &cobra.Command{
		Use:   "cleanup",
		Short: "Cleanup data for the workload",
		Run: func(cmd *cobra.Command, _ []string) {
			execTxnKV("cleanup")
		},
	}

	var cmdCheck = &cobra.Command{
		Use:   "check",
		Short: "Check data consistency for the workload",
		Run: func(cmd *cobra.Command, _ []string) {
			execTxnKV("check")
		},
	}

	cmd.AddCommand(cmdRun, cmdPrepare, cmdCleanup, cmdCheck)

	command.GetCommand().AddCommand(cmd)

	return txnKVConfig
}

func getTxnKVConfig(ctx context.Context) *config.TxnKVConfig {
	c := ctx.Value(WorkloadImplName).(*config.TxnKVConfig)
	return c
}

// Assistants for TxnKV workload
func prepareLockKeyWithTimeout(ctx context.Context, txn *clientTxnKV.KVTxn, key []byte, timeout int64) error {
	if timeout > 0 {
		return txn.LockKeysWithWaitTime(ctx, timeout, key)
	}
	return nil
}

// Workload is the implementation of WorkloadInterface
type WorkloadImpl struct {
	cfg     *config.TxnKVConfig
	clients []*clientTxnKV.Client

	wait sync.WaitGroup

	stats *statistics.PerfProfile
}

func NewTxnKVWorkload(cfg *config.TxnKVConfig) (*WorkloadImpl, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	w := &WorkloadImpl{
		cfg:   cfg,
		stats: statistics.NewPerfProfile(),
	}

	clientConfig.UpdateGlobal(func(conf *clientConfig.Config) {
		conf.TiKVClient.MaxBatchSize = (uint)(cfg.TxnSize + 10)
	})
	// TODO: setting batch.
	// defer config.UpdateGlobal(func(conf *config.Config) {
	// 	conf.TiKVClient.MaxBatchSize = 0
	// 	conf.TiKVClient.GrpcConnectionCount = 1
	// })()

	w.clients = make([]*clientTxnKV.Client, 0, w.cfg.Global.Threads)
	for i := 0; i < w.cfg.Global.Threads; i++ {
		client, err := clientTxnKV.NewClient(w.cfg.Global.Targets)
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
	return w.cfg != nil && w.cfg.Global != nil && len(w.clients) > 0
}

func (w *WorkloadImpl) isValidThread(threadID int) bool {
	return w.isValid() && threadID < len(w.clients)
}

// InitThread implements WorkloadInterface
func (w *WorkloadImpl) InitThread(ctx context.Context, threadID int) error {
	// Nothing to do
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
		return fmt.Errorf("no valid TxnKV clients")
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
		return fmt.Errorf("no valid TxnKV clients")
	}

	client := w.clients[threadID]
	key := WorkloadDefaultKey
	val := utils.GenRandomStr(WorkloadDefaultValue, w.cfg.ValueSize)
	lockTimeout := int64(w.cfg.LockTimeout)

	// Constructs the txn client and sets the txn mode
	txn, err := client.Begin()
	if err != nil {
		return fmt.Errorf("txn begin failed, err %v", err)
	}
	switch w.cfg.TxnMode {
	case WorkloadTxnMode1PC:
		txn.SetEnable1PC(true)
	case WorkloadTxnModeAsyncCommit:
		txn.SetEnableAsyncCommit(true)
	}
	// Default is optimistic lock mode.

	txn.SetPessimistic(lockTimeout > 0)

	sum := w.cfg.TxnSize * w.cfg.ColumnSize
	readCount := sum * w.cfg.ReadWriteRatio.GetPercent(utils.ReadPercent) / 100
	writeCount := sum - readCount
	canRead := func(sum, readCount, writeCount int) bool {
		return readCount > 0 && (writeCount <= 0 || rand.Intn(sum)/2 == 0)
	}

	for row := 0; row < w.cfg.TxnSize; row++ {
		key = fmt.Sprintf("%s@col_", utils.GenRandomStr(key, w.cfg.KeySize))
		// Lock the key with timeout if necessary.
		if err = prepareLockKeyWithTimeout(ctx, txn, []byte(key), lockTimeout); err != nil {
			fmt.Printf("txn lock key failed, err %v", err)
			continue
		}
		for col := 0; col < w.cfg.ColumnSize; col++ {
			colKey := fmt.Sprintf("%s%d", key, col)
			if canRead(sum, readCount, writeCount) {
				_, err = txn.Get(ctx, []byte(colKey))
				if tikverr.IsErrNotFound(err) {
					err = txn.Set([]byte(colKey), []byte(val))
					writeCount -= 1
				}
				readCount -= 1
			} else {
				err = txn.Set([]byte(colKey), []byte(val))
				writeCount -= 1
			}
			if err != nil {
				return fmt.Errorf("txn set / get failed, err %v", err)
			}
		}
	}
	start := time.Now()
	err = txn.Commit(ctx)
	if err != nil {
		return fmt.Errorf("txn commit failed, err %v", err)
	}
	w.stats.Record(w.cfg.TxnMode, time.Since(start))
	return nil
}

// Check implements WorkloadInterface
func (w *WorkloadImpl) Check(ctx context.Context, threadID int) error {
	return nil
}

// Cleanup implements WorkloadInterface
func (w *WorkloadImpl) Cleanup(ctx context.Context, threadID int) error {
	if !w.isValidThread(threadID) {
		return fmt.Errorf("no valid TxnKV clients")
	}
	if threadID == 0 {
		client := w.clients[threadID]
		client.DeleteRange(ctx, []byte(WorkloadDefaultKey), []byte(WorkloadDefaultEndKey), w.cfg.Global.Threads) // delete all keys
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

func execTxnKV(cmd string) {
	if cmd == "" {
		return
	}
	TxnKVConfig := getTxnKVConfig(workloads.GlobalContext)

	var workload *WorkloadImpl
	var err error
	if workload, err = NewTxnKVWorkload(TxnKVConfig); err != nil {
		fmt.Printf("create TxnKV workload failed: %v\n", err)
		return
	}

	timeoutCtx, cancel := context.WithTimeout(workloads.GlobalContext, TxnKVConfig.Global.TotalTime)
	workloads.GlobalContext = timeoutCtx
	defer cancel()

	workload.Execute(cmd)
	fmt.Println("TxnKV workload finished")
	workload.OutputStats(true)
}
