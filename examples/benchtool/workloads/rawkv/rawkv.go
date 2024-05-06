package rawkv

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
	"github.com/tikv/client-go/v2/rawkv"
)

const (
	WorkloadImplName = "rawkv"

	WorkloadTypePut = "put"
	WorkloadTypeGet = "get"

	WorkloadDefaultKey   = "rawkv_key"
	WorkloadDefaultValue = "rawkv_value"

	WorkloadMaxInt64 = 1<<63 - 1
)

type RawKVConfig struct {
	keySize   int
	valueSize int

	prepareRetryCount    int
	prepareRetryInterval time.Duration
	randomize            bool
	readWriteRatio       *utils.ReadWriteRatio
	commandType          string

	global *config.GlobalConfig
}

func (c *RawKVConfig) Validate() error {
	if c.keySize <= 0 || c.valueSize <= 0 {
		return fmt.Errorf("key size or value size must be greater than 0")
	}
	if err := c.readWriteRatio.ParseRatio(); err != nil {
		return fmt.Errorf("parse read-write-ratio failed: %v", err)
	}
	return c.global.ParsePdAddrs()
}

// Register registers the workload to the command line parser
func Register(command *config.CommandLineParser) *RawKVConfig {
	if command == nil {
		return nil
	}
	rawKVConfig := &RawKVConfig{
		global:         command.GetConfig(),
		readWriteRatio: utils.NewReadWriteRatio("1:1"), // TODO: generate workloads meeting the read-write ratio
	}

	cmd := &cobra.Command{
		Use: WorkloadImplName,
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			workloads.GlobalContext = context.WithValue(workloads.GlobalContext, WorkloadImplName, rawKVConfig)
		},
	}
	cmd.PersistentFlags().StringVar(&rawKVConfig.commandType, "cmd", "put", "Type of command to execute (put/get)")
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

func genRandomStr(prefix string, keySize int) string {
	return fmt.Sprintf("%s_%0*d", prefix, keySize, rand.Intn(WorkloadMaxInt64))
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
	key := WorkloadDefaultKey
	val := WorkloadDefaultValue
	if w.cfg.randomize {
		key = genRandomStr(WorkloadDefaultKey, w.cfg.keySize)
		val = genRandomStr(WorkloadDefaultValue, w.cfg.valueSize)
	}

	start := time.Now()
	switch w.cfg.commandType {
	case WorkloadTypePut:
		client.Put(ctx, []byte(key), []byte(val))
	case WorkloadTypeGet:
		client.Get(ctx, []byte(key))
		// TODO: add BatchGet and BatchPut
	}
	w.stats.Record(w.cfg.commandType, time.Since(start))
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
