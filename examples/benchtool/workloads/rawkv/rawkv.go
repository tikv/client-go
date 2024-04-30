package rawkv

import (
	"benchtool/config"
	"benchtool/utils"
	"benchtool/utils/statistics"
	"benchtool/workloads"
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
	"github.com/tikv/client-go/v2/rawkv"
)

const (
	WorkloadImplName = "rawkv"

	WorkloadTypePut = "put"
	WorkloadTypeGet = "get"
)

// Format: "Elapsed" - "Sum" - "Count" - "Ops" - "Avg" - "P50" - "P90" - "P95" - "P99" - "P999" - "P9999" - "Min" - "Max
var workloadFormat = []string{"Prefix", "Operation", "Takes(s)", "Count", "Ops", "Avg(ms)", "50th(ms)", "90th(ms)", "95th(ms)", "99th(ms)", "99.9th(ms)", "99.99th(ms)", "Min(ms)", "Max(ms)"}

type RawKVConfig struct {
	keySize   int
	valueSize int

	prepareRetryCount    int
	prepareRetryInterval time.Duration
	randomize            bool
	readWriteRatio       *utils.ReadWriteRatio

	global *config.GlobalConfig
}

func (c *RawKVConfig) Validate() error {
	if c.keySize <= 0 || c.valueSize <= 0 {
		return fmt.Errorf("key size or value size must be greater than 0")
	}
	if err := c.readWriteRatio.ParseRatio(); err != nil {
		return fmt.Errorf("parse read-write-ratio failed: %v", err)
	}
	return nil
}

// Register registers the workload to the command line parser
func Register(command *config.CommandLineParser) *RawKVConfig {
	if command == nil {
		return nil
	}
	rawKVConfig := &RawKVConfig{
		global:         command.GetConfig(),
		readWriteRatio: utils.NewReadWriteRatio("1:1"),
	}

	cmd := &cobra.Command{
		Use: WorkloadImplName,
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			workloads.GlobalContext = context.WithValue(workloads.GlobalContext, WorkloadImplName, rawKVConfig)
		},
	}
	cmd.PersistentFlags().IntVar(&rawKVConfig.keySize, "key-size", 1, "Size of key in bytes")
	cmd.PersistentFlags().IntVar(&rawKVConfig.valueSize, "value-size", 1, "Size of value in bytes")
	cmd.PersistentFlags().BoolVar(&rawKVConfig.randomize, "random", false, "Whether to randomize each value")
	cmd.PersistentFlags().StringVar(&rawKVConfig.readWriteRatio.Ratio, "read-write-ratio", "1:1", "Read write ratio")

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

	for i := 0; i < 10; i++ {
		start := time.Now()
		// Put
		client.Put(ctx, []byte("Company"), []byte("PingCAP"))
		w.stats.Record(WorkloadTypePut, time.Since(start))
		// Get
		start = time.Now()
		client.Get(ctx, []byte("Company"))
		w.stats.Record(WorkloadTypeGet, time.Since(start))
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
		return fmt.Errorf("no valid RawKV clients")
	}
	if threadID == 0 {
		client := w.clients[threadID]
		client.DeleteRange(ctx, []byte(""), []byte("")) // delete all keys
	}
	return nil
}

func outputFunc(outputStyle string, prefix string, perfHist map[string]*statistics.PerfHistogram) {
	keys := make([]string, 0, len(perfHist))
	for k := range perfHist {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	lines := [][]string{}
	for _, op := range keys {
		hist := perfHist[op]
		if !hist.Empty() {
			op = strings.ToUpper(op)
			line := []string{prefix, op}
			line = append(line, hist.Format()...)
			lines = append(lines, line)
		}
	}
	switch outputStyle {
	case utils.OutputStylePlain:
		utils.RenderString("%s%-6s - %s\n", workloadFormat, lines)
	case utils.OutputStyleTable:
		utils.RenderTable(workloadFormat, lines)
	case utils.OutputStyleJson:
		utils.RenderJson(workloadFormat, lines)
	}
}

func (w *WorkloadImpl) OutputStats(ifSummaryReport bool) {
	w.stats.PrintFmt(ifSummaryReport, w.cfg.global.OutputStyle, outputFunc)
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
	workload.Execute(cmd)
}
