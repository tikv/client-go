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

package patterns

import (
	"benchtool/config"
	"benchtool/utils/statistics"
	"benchtool/workloads"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/spf13/cobra"
	clientConfig "github.com/tikv/client-go/v2/config"
	clientRawKV "github.com/tikv/client-go/v2/rawkv"
	clientTxnKV "github.com/tikv/client-go/v2/txnkv"
)

// Register registers the workload to the command line parser
func Register(command *config.CommandLineParser) *config.PatternsConfig {
	if command == nil {
		return nil
	}
	patternsConfig := &config.PatternsConfig{
		Global: command.GetConfig(),
	}

	cmd := &cobra.Command{
		Use: config.WorkloadTypeHybrid,
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			workloads.GlobalContext = context.WithValue(workloads.GlobalContext, config.WorkloadTypeHybrid, patternsConfig)
		},
	}
	cmd.PersistentFlags().StringVar(&patternsConfig.FilePath, "file-path", "", "The path of the patterns file")

	var cmdPrepare = &cobra.Command{
		Use:   "prepare",
		Short: "Prepare data for TxnKV workload",
		Run: func(cmd *cobra.Command, _ []string) {
			execTxnKV("prepare")
		},
	}
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

	return patternsConfig
}

func getPatternsConfig(ctx context.Context) *config.PatternsConfig {
	c := ctx.Value(config.WorkloadTypeHybrid).(*config.PatternsConfig)
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
	// Pointer to the next execution plan
	patternIdx int
	// workload pattern
	config *config.PatternsConfig

	rawClients []*clientRawKV.Client
	txnClients []*clientTxnKV.Client

	stats *statistics.PerfProfile

	wait sync.WaitGroup
}

func NewPatternWorkload(cfg *config.PatternsConfig) (*WorkloadImpl, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	w := &WorkloadImpl{
		patternIdx: 0, // start from 0
		config:     cfg,
		stats:      statistics.NewPerfProfile(),
	}
	return w, nil
}

func (w *WorkloadImpl) Name() string {
	return config.WorkloadTypeHybrid
}

func (w *WorkloadImpl) isValid() bool {
	return w.config != nil && w.config.Global != nil && (len(w.rawClients) > 0 || len(w.txnClients) > 0)
}

func (w *WorkloadImpl) isValidThread(threadID int) bool {
	return w.isValid() && threadID < max(len(w.rawClients), len(w.txnClients))
}

// InitThread implements WorkloadInterface
func (w *WorkloadImpl) InitThread(ctx context.Context, threadID int) error {
	// Nothing to do
	return nil
}

// CleanupThread implements WorkloadInterface
func (w *WorkloadImpl) CleanupThread(ctx context.Context, threadID int) {
	if w.isValidThread(threadID) {
		if len(w.rawClients) > 0 {
			client := w.rawClients[threadID]
			if client != nil {
				client.Close()
			}
		} else {
			client := w.txnClients[threadID]
			if client != nil {
				client.Close()
			}
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
	// delete all keys
	if threadID == 0 {
		if len(w.rawClients) > 0 {
			client := w.rawClients[threadID]
			client.DeleteRange(ctx, []byte(config.RawKVCommandDefaultKey), []byte(config.RawKVCommandDefaultEndKey))
		} else {
			client := w.txnClients[threadID]
			client.DeleteRange(ctx, []byte(config.TxnKVCommandDefaultKey), []byte(config.TxnKVCommandDefaultEndKey), len(w.txnClients))
		}
	}
	return nil
}

func (w *WorkloadImpl) OutputStats(ifSummaryReport bool) {
	w.stats.PrintFmt(ifSummaryReport, w.config.Global.OutputStyle, statistics.HistogramOutputFunc)
}

func (w *WorkloadImpl) IsTxnKVPattern() bool {
	plan := w.config.Plans[w.patternIdx]
	return plan.GetTxnKVConfig() != nil
}

func (w *WorkloadImpl) ContinueToExecute() bool {
	return w.patternIdx < len(w.config.Plans)
}

func (w *WorkloadImpl) BeforeExecute() error {
	plan := w.config.Plans[w.patternIdx]
	txnConfig := plan.GetTxnKVConfig()
	rawConfig := plan.GetRawKVConfig()
	if txnConfig != nil {
		clientConfig.UpdateGlobal(func(conf *clientConfig.Config) {
			conf.TiKVClient.MaxBatchSize = (uint)(txnConfig.TxnSize + 10)
		})
		w.txnClients = make([]*clientTxnKV.Client, 0, txnConfig.Global.Threads)
		for i := 0; i < txnConfig.Global.Threads; i++ {
			client, err := clientTxnKV.NewClient(txnConfig.Global.Targets)
			if err != nil {
				return err
			}
			w.txnClients = append(w.txnClients, client)
		}
	} else if rawConfig != nil {
		w.rawClients = make([]*clientRawKV.Client, 0, rawConfig.Global.Threads)
		for i := 0; i < txnConfig.Global.Threads; i++ {
			client, err := clientRawKV.NewClient(workloads.GlobalContext, rawConfig.Global.Targets, rawConfig.Global.Security)
			if err != nil {
				return err
			}
			w.rawClients = append(w.rawClients, client)
		}
	}
	fmt.Println("Start to execute pattern", plan.GetName())
	return nil
}

func (w *WorkloadImpl) AfterExecute() {
	plan := w.config.Plans[w.patternIdx]
	w.OutputStats(true)
	fmt.Println("Finish executing pattern", plan.GetName())
	// Release the resources
	w.rawClients = nil
	w.txnClients = nil
	w.patternIdx += 1
	w.stats.Clear()
}

func (w *WorkloadImpl) Execute(cmd string) {
	plan := w.config.Plans[w.patternIdx]
	txnConfig := plan.GetTxnKVConfig()
	rawConfig := plan.GetRawKVConfig()
	var globalConfig *config.GlobalConfig
	if txnConfig != nil {
		globalConfig = txnConfig.Global
	} else {
		globalConfig = rawConfig.Global
	}

	w.wait.Add(globalConfig.Threads)

	ctx, cancel := context.WithCancel(workloads.GlobalContext)
	ch := make(chan struct{}, 1)
	go func() {
		ticker := time.NewTicker(globalConfig.OutputInterval)
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

	count := globalConfig.TotalCount / globalConfig.Threads
	for i := 0; i < globalConfig.Threads; i++ {
		go func(index int) {
			defer w.wait.Done()
			if err := workloads.DispatchExecution(ctx, w, cmd, count, index, globalConfig.Silence, globalConfig.IgnoreError); err != nil {
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
	patternsConfig := getPatternsConfig(workloads.GlobalContext)

	var workload *WorkloadImpl
	var err error
	if workload, err = NewPatternWorkload(patternsConfig); err != nil {
		fmt.Printf("create Patterns workload failed: %v\n", err)
		return
	}

	timeoutCtx, cancel := context.WithTimeout(workloads.GlobalContext, patternsConfig.Global.TotalTime)
	workloads.GlobalContext = timeoutCtx
	defer cancel()

	for {
		if !workload.ContinueToExecute() {
			break
		}
		if err = workload.BeforeExecute(); err != nil {
			fmt.Println("BeforeExecute failed:", err)
			break
		}
		workload.Execute(cmd)
		workload.AfterExecute()
	}
}
