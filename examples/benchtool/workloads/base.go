package workloads

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

const (
	DefaultDriver = "mysql"
)

// WorkloadInterface is the interface for running customized workload
type WorkloadInterface interface {
	Name() string
	InitThread(ctx context.Context, threadID int) error
	CleanupThread(ctx context.Context, threadID int)
	Prepare(ctx context.Context, threadID int) error
	CheckPrepare(ctx context.Context, threadID int) error
	Run(ctx context.Context, threadID int) error
	Cleanup(ctx context.Context, threadID int) error
	Check(ctx context.Context, threadID int) error
	OutputStats(ifSummaryReport bool)
	DBName() string
}

var GlobalContext context.Context
var GlobalDB *sql.DB // Maybe useless, as the tikv.Client is the only enter to access the TiKV.

func DispatchExecution(timeoutCtx context.Context, w WorkloadInterface, action string, count int, threadIdx int, silence bool, ignoreError bool) error {
	if err := w.InitThread(context.Background(), threadIdx); err != nil {
		return err
	}
	defer w.CleanupThread(timeoutCtx, threadIdx)

	switch action {
	case "prepare":
		return w.Prepare(timeoutCtx, threadIdx)
	case "cleanup":
		return w.Cleanup(timeoutCtx, threadIdx)
	case "check":
		return w.Check(timeoutCtx, threadIdx)
	}

	if count > 0 {
		for i := 0; i < count || count <= 0; i++ {
			err := w.Run(timeoutCtx, threadIdx)
			if err != nil {
				if !silence {
					fmt.Printf("[%s] execute %s failed, err %v\n", time.Now().Format("2006-01-02 15:04:05"), action, err)
				}
				if !ignoreError {
					return err
				}
			}
			select {
			case <-timeoutCtx.Done():
				return nil
			default:
			}
		}
	}
	return nil
}
