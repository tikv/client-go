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
	"sync"
	"time"

	"github.com/spf13/cobra"
	clientConfig "github.com/tikv/client-go/v2/config"
	clientTxnKV "github.com/tikv/client-go/v2/txnkv"
)

const (
	WorkloadImplName = "txnkv"

	WorkloadTypeWrite = "write"
	WorkloadTypeRead  = "read"

	WorkloadDefaultKey   = "txnkv_key"
	WorkloadDefaultValue = "txnkv_value"

	WorkloadTxnModeDefault     = "2pc"
	WorkloadTxnMode1PC         = "1pc"
	WorkloadTxnModeAsyncCommit = "async-commit"
)

type TxnKVConfig struct {
	keySize    int
	valueSize  int
	columnSize int
	txnSize    int

	prepareRetryCount    int
	prepareRetryInterval time.Duration
	readWriteRatio       *utils.ReadWriteRatio
	txnMode              string

	global *config.GlobalConfig
}

func (c *TxnKVConfig) Validate() error {
	if c.keySize <= 0 || c.valueSize <= 0 {
		return fmt.Errorf("key size or value size must be greater than 0")
	}
	if err := c.readWriteRatio.ParseRatio(); err != nil {
		return fmt.Errorf("parse read-write-ratio failed: %v", err)
	}
	return c.global.ParsePdAddrs()
}

// Register registers the workload to the command line parser
func Register(command *config.CommandLineParser) *TxnKVConfig {
	if command == nil {
		return nil
	}
	txnKVConfig := &TxnKVConfig{
		global:         command.GetConfig(),
		readWriteRatio: utils.NewReadWriteRatio("1:1"), // TODO: generate workloads meeting the read-write ratio
	}

	cmd := &cobra.Command{
		Use: WorkloadImplName,
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			workloads.GlobalContext = context.WithValue(workloads.GlobalContext, WorkloadImplName, txnKVConfig)
		},
	}
	cmd.PersistentFlags().StringVar(&txnKVConfig.readWriteRatio.Ratio, "read-write-ratio", "1:1", "Read write ratio")
	cmd.PersistentFlags().IntVar(&txnKVConfig.keySize, "key-size", 1, "Size of key in bytes")
	cmd.PersistentFlags().IntVar(&txnKVConfig.valueSize, "value-size", 1, "Size of value in bytes")
	cmd.PersistentFlags().IntVar(&txnKVConfig.columnSize, "column-size", 1, "Size of column")
	cmd.PersistentFlags().IntVar(&txnKVConfig.txnSize, "txn-size", 1, "Size of transaction (normally, the lines of kv pairs)")
	cmd.PersistentFlags().StringVar(&txnKVConfig.txnMode, "txn-mode", "2pc", "Mode of transaction (2pc/1pc/async-commit), default: 2pc")

	var cmdPrepare = &cobra.Command{
		Use:   "prepare",
		Short: "Prepare data for TxnKV workload",
		Run: func(cmd *cobra.Command, _ []string) {
			execTxnKV("prepare")
		},
	}
	cmdPrepare.PersistentFlags().IntVar(&txnKVConfig.prepareRetryCount, "retry-count", 50, "Retry count when errors occur")
	cmdPrepare.PersistentFlags().DurationVar(&txnKVConfig.prepareRetryInterval, "retry-interval", 10*time.Millisecond, "The interval for each retry")

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

func getTxnKVConfig(ctx context.Context) *TxnKVConfig {
	c := ctx.Value(WorkloadImplName).(*TxnKVConfig)
	return c
}

// Workload is the implementation of WorkloadInterface
type WorkloadImpl struct {
	cfg     *TxnKVConfig
	clients []*clientTxnKV.Client

	wait sync.WaitGroup

	stats *statistics.PerfProfile
}

func NewTxnKVWorkload(cfg *TxnKVConfig) (*WorkloadImpl, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	w := &WorkloadImpl{
		cfg:   cfg,
		stats: statistics.NewPerfProfile(),
	}

	clientConfig.UpdateGlobal(func(conf *clientConfig.Config) {
		conf.TiKVClient.MaxBatchSize = (uint)(cfg.txnSize + 10)
	})
	// TODO: setting batch.
	// defer config.UpdateGlobal(func(conf *config.Config) {
	// 	conf.TiKVClient.MaxBatchSize = 0
	// 	conf.TiKVClient.GrpcConnectionCount = 1
	// })()

	w.clients = make([]*clientTxnKV.Client, 0, w.cfg.global.Threads)
	for i := 0; i < w.cfg.global.Threads; i++ {
		client, err := clientTxnKV.NewClient(w.cfg.global.Targets)
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
	val := utils.GenRandomStr(WorkloadDefaultValue, w.cfg.valueSize)

	// Constructs the txn client and sets the txn mode
	txn, err := client.Begin()
	if err != nil {
		return fmt.Errorf("txn begin failed, err %v", err)
	}
	switch w.cfg.txnMode {
	case WorkloadTxnMode1PC:
		txn.SetEnable1PC(true)
	case WorkloadTxnModeAsyncCommit:
		txn.SetEnableAsyncCommit(true)
	}

	for row := 0; row < w.cfg.txnSize; row++ {
		key = fmt.Sprintf("%s@col_", utils.GenRandomStr(key, w.cfg.keySize))
		for col := 0; col < w.cfg.columnSize; col++ {
			colKey := fmt.Sprintf("%s%d", key, col)
			err = txn.Set([]byte(colKey), []byte(val))
			if err != nil {
				return fmt.Errorf("txn set failed, err %v", err)
			}
			_, err = txn.Get(ctx, []byte(colKey))
			if err != nil {
				return fmt.Errorf("txn get failed, err %v", err)
			}
		}
	}
	start := time.Now()
	err = txn.Commit(ctx)
	if err != nil {
		return fmt.Errorf("txn commit failed, err %v", err)
	}
	w.stats.Record(w.cfg.txnMode, time.Since(start))
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
		client.DeleteRange(ctx, []byte(WorkloadDefaultKey), []byte(WorkloadDefaultKey), w.cfg.global.Threads) // delete all keys
	}
	return nil
}

func (w *WorkloadImpl) OutputStats(ifSummaryReport bool) {
	w.stats.PrintFmt(ifSummaryReport, w.cfg.global.OutputStyle, statistics.HistogramOutputFunc)
}

// DBName returns the name of test db.
func (w *WorkloadImpl) DBName() string {
	return w.cfg.global.DbName
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

	timeoutCtx, cancel := context.WithTimeout(workloads.GlobalContext, TxnKVConfig.global.TotalTime)
	workloads.GlobalContext = timeoutCtx
	defer cancel()

	workload.Execute(cmd)
	fmt.Println("TxnKV workload finished")
	workload.OutputStats(true)
}
