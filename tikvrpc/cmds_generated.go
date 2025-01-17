// Code generated by gen.sh. DO NOT EDIT.

package tikvrpc

import (
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
)

func patchCmdCtx(req *Request, cmd CmdType, ctx *kvrpcpb.Context) bool {
	switch cmd {
	case CmdGet:
		if req.rev == 0 {
			req.Get().Context = ctx
		} else {
			cmd := *req.Get()
			cmd.Context = ctx
			req.Req = &cmd
		}
		req.rev++
	case CmdScan:
		if req.rev == 0 {
			req.Scan().Context = ctx
		} else {
			cmd := *req.Scan()
			cmd.Context = ctx
			req.Req = &cmd
		}
		req.rev++
	case CmdPrewrite:
		if req.rev == 0 {
			req.Prewrite().Context = ctx
		} else {
			cmd := *req.Prewrite()
			cmd.Context = ctx
			req.Req = &cmd
		}
		req.rev++
	case CmdPessimisticLock:
		if req.rev == 0 {
			req.PessimisticLock().Context = ctx
		} else {
			cmd := *req.PessimisticLock()
			cmd.Context = ctx
			req.Req = &cmd
		}
		req.rev++
	case CmdPessimisticRollback:
		if req.rev == 0 {
			req.PessimisticRollback().Context = ctx
		} else {
			cmd := *req.PessimisticRollback()
			cmd.Context = ctx
			req.Req = &cmd
		}
		req.rev++
	case CmdCommit:
		if req.rev == 0 {
			req.Commit().Context = ctx
		} else {
			cmd := *req.Commit()
			cmd.Context = ctx
			req.Req = &cmd
		}
		req.rev++
	case CmdCleanup:
		if req.rev == 0 {
			req.Cleanup().Context = ctx
		} else {
			cmd := *req.Cleanup()
			cmd.Context = ctx
			req.Req = &cmd
		}
		req.rev++
	case CmdBatchGet:
		if req.rev == 0 {
			req.BatchGet().Context = ctx
		} else {
			cmd := *req.BatchGet()
			cmd.Context = ctx
			req.Req = &cmd
		}
		req.rev++
	case CmdBatchRollback:
		if req.rev == 0 {
			req.BatchRollback().Context = ctx
		} else {
			cmd := *req.BatchRollback()
			cmd.Context = ctx
			req.Req = &cmd
		}
		req.rev++
	case CmdScanLock:
		if req.rev == 0 {
			req.ScanLock().Context = ctx
		} else {
			cmd := *req.ScanLock()
			cmd.Context = ctx
			req.Req = &cmd
		}
		req.rev++
	case CmdResolveLock:
		if req.rev == 0 {
			req.ResolveLock().Context = ctx
		} else {
			cmd := *req.ResolveLock()
			cmd.Context = ctx
			req.Req = &cmd
		}
		req.rev++
	case CmdGC:
		if req.rev == 0 {
			req.GC().Context = ctx
		} else {
			cmd := *req.GC()
			cmd.Context = ctx
			req.Req = &cmd
		}
		req.rev++
	case CmdDeleteRange:
		if req.rev == 0 {
			req.DeleteRange().Context = ctx
		} else {
			cmd := *req.DeleteRange()
			cmd.Context = ctx
			req.Req = &cmd
		}
		req.rev++
	case CmdRawGet:
		if req.rev == 0 {
			req.RawGet().Context = ctx
		} else {
			cmd := *req.RawGet()
			cmd.Context = ctx
			req.Req = &cmd
		}
		req.rev++
	case CmdRawBatchGet:
		if req.rev == 0 {
			req.RawBatchGet().Context = ctx
		} else {
			cmd := *req.RawBatchGet()
			cmd.Context = ctx
			req.Req = &cmd
		}
		req.rev++
	case CmdRawPut:
		if req.rev == 0 {
			req.RawPut().Context = ctx
		} else {
			cmd := *req.RawPut()
			cmd.Context = ctx
			req.Req = &cmd
		}
		req.rev++
	case CmdRawBatchPut:
		if req.rev == 0 {
			req.RawBatchPut().Context = ctx
		} else {
			cmd := *req.RawBatchPut()
			cmd.Context = ctx
			req.Req = &cmd
		}
		req.rev++
	case CmdRawDelete:
		if req.rev == 0 {
			req.RawDelete().Context = ctx
		} else {
			cmd := *req.RawDelete()
			cmd.Context = ctx
			req.Req = &cmd
		}
		req.rev++
	case CmdRawBatchDelete:
		if req.rev == 0 {
			req.RawBatchDelete().Context = ctx
		} else {
			cmd := *req.RawBatchDelete()
			cmd.Context = ctx
			req.Req = &cmd
		}
		req.rev++
	case CmdRawDeleteRange:
		if req.rev == 0 {
			req.RawDeleteRange().Context = ctx
		} else {
			cmd := *req.RawDeleteRange()
			cmd.Context = ctx
			req.Req = &cmd
		}
		req.rev++
	case CmdRawScan:
		if req.rev == 0 {
			req.RawScan().Context = ctx
		} else {
			cmd := *req.RawScan()
			cmd.Context = ctx
			req.Req = &cmd
		}
		req.rev++
	case CmdRawGetKeyTTL:
		if req.rev == 0 {
			req.RawGetKeyTTL().Context = ctx
		} else {
			cmd := *req.RawGetKeyTTL()
			cmd.Context = ctx
			req.Req = &cmd
		}
		req.rev++
	case CmdRawCompareAndSwap:
		if req.rev == 0 {
			req.RawCompareAndSwap().Context = ctx
		} else {
			cmd := *req.RawCompareAndSwap()
			cmd.Context = ctx
			req.Req = &cmd
		}
		req.rev++
	case CmdRawChecksum:
		if req.rev == 0 {
			req.RawChecksum().Context = ctx
		} else {
			cmd := *req.RawChecksum()
			cmd.Context = ctx
			req.Req = &cmd
		}
		req.rev++
	case CmdUnsafeDestroyRange:
		if req.rev == 0 {
			req.UnsafeDestroyRange().Context = ctx
		} else {
			cmd := *req.UnsafeDestroyRange()
			cmd.Context = ctx
			req.Req = &cmd
		}
		req.rev++
	case CmdRegisterLockObserver:
		if req.rev == 0 {
			req.RegisterLockObserver().Context = ctx
		} else {
			cmd := *req.RegisterLockObserver()
			cmd.Context = ctx
			req.Req = &cmd
		}
		req.rev++
	case CmdCheckLockObserver:
		if req.rev == 0 {
			req.CheckLockObserver().Context = ctx
		} else {
			cmd := *req.CheckLockObserver()
			cmd.Context = ctx
			req.Req = &cmd
		}
		req.rev++
	case CmdRemoveLockObserver:
		if req.rev == 0 {
			req.RemoveLockObserver().Context = ctx
		} else {
			cmd := *req.RemoveLockObserver()
			cmd.Context = ctx
			req.Req = &cmd
		}
		req.rev++
	case CmdPhysicalScanLock:
		if req.rev == 0 {
			req.PhysicalScanLock().Context = ctx
		} else {
			cmd := *req.PhysicalScanLock()
			cmd.Context = ctx
			req.Req = &cmd
		}
		req.rev++
	case CmdCop:
		if req.rev == 0 {
			req.Cop().Context = ctx
		} else {
			cmd := *req.Cop()
			cmd.Context = ctx
			req.Req = &cmd
		}
		req.rev++
	case CmdBatchCop:
		if req.rev == 0 {
			req.BatchCop().Context = ctx
		} else {
			cmd := *req.BatchCop()
			cmd.Context = ctx
			req.Req = &cmd
		}
		req.rev++
	case CmdMvccGetByKey:
		if req.rev == 0 {
			req.MvccGetByKey().Context = ctx
		} else {
			cmd := *req.MvccGetByKey()
			cmd.Context = ctx
			req.Req = &cmd
		}
		req.rev++
	case CmdMvccGetByStartTs:
		if req.rev == 0 {
			req.MvccGetByStartTs().Context = ctx
		} else {
			cmd := *req.MvccGetByStartTs()
			cmd.Context = ctx
			req.Req = &cmd
		}
		req.rev++
	case CmdSplitRegion:
		if req.rev == 0 {
			req.SplitRegion().Context = ctx
		} else {
			cmd := *req.SplitRegion()
			cmd.Context = ctx
			req.Req = &cmd
		}
		req.rev++
	case CmdTxnHeartBeat:
		if req.rev == 0 {
			req.TxnHeartBeat().Context = ctx
		} else {
			cmd := *req.TxnHeartBeat()
			cmd.Context = ctx
			req.Req = &cmd
		}
		req.rev++
	case CmdCheckTxnStatus:
		if req.rev == 0 {
			req.CheckTxnStatus().Context = ctx
		} else {
			cmd := *req.CheckTxnStatus()
			cmd.Context = ctx
			req.Req = &cmd
		}
		req.rev++
	case CmdCheckSecondaryLocks:
		if req.rev == 0 {
			req.CheckSecondaryLocks().Context = ctx
		} else {
			cmd := *req.CheckSecondaryLocks()
			cmd.Context = ctx
			req.Req = &cmd
		}
		req.rev++
	case CmdFlashbackToVersion:
		if req.rev == 0 {
			req.FlashbackToVersion().Context = ctx
		} else {
			cmd := *req.FlashbackToVersion()
			cmd.Context = ctx
			req.Req = &cmd
		}
		req.rev++
	case CmdPrepareFlashbackToVersion:
		if req.rev == 0 {
			req.PrepareFlashbackToVersion().Context = ctx
		} else {
			cmd := *req.PrepareFlashbackToVersion()
			cmd.Context = ctx
			req.Req = &cmd
		}
		req.rev++
	case CmdFlush:
		if req.rev == 0 {
			req.Flush().Context = ctx
		} else {
			cmd := *req.Flush()
			cmd.Context = ctx
			req.Req = &cmd
		}
		req.rev++
	case CmdBufferBatchGet:
		if req.rev == 0 {
			req.BufferBatchGet().Context = ctx
		} else {
			cmd := *req.BufferBatchGet()
			cmd.Context = ctx
			req.Req = &cmd
		}
		req.rev++
	default:
		return false
	}
	return true
}

func isValidReqType(cmd CmdType) bool {
	switch cmd {
	case CmdGet:
		return true
	case CmdScan:
		return true
	case CmdPrewrite:
		return true
	case CmdPessimisticLock:
		return true
	case CmdPessimisticRollback:
		return true
	case CmdCommit:
		return true
	case CmdCleanup:
		return true
	case CmdBatchGet:
		return true
	case CmdBatchRollback:
		return true
	case CmdScanLock:
		return true
	case CmdResolveLock:
		return true
	case CmdGC:
		return true
	case CmdDeleteRange:
		return true
	case CmdRawGet:
		return true
	case CmdRawBatchGet:
		return true
	case CmdRawPut:
		return true
	case CmdRawBatchPut:
		return true
	case CmdRawDelete:
		return true
	case CmdRawBatchDelete:
		return true
	case CmdRawDeleteRange:
		return true
	case CmdRawScan:
		return true
	case CmdRawGetKeyTTL:
		return true
	case CmdRawCompareAndSwap:
		return true
	case CmdRawChecksum:
		return true
	case CmdUnsafeDestroyRange:
		return true
	case CmdRegisterLockObserver:
		return true
	case CmdCheckLockObserver:
		return true
	case CmdRemoveLockObserver:
		return true
	case CmdPhysicalScanLock:
		return true
	case CmdCop:
		return true
	case CmdBatchCop:
		return true
	case CmdMvccGetByKey:
		return true
	case CmdMvccGetByStartTs:
		return true
	case CmdSplitRegion:
		return true
	case CmdTxnHeartBeat:
		return true
	case CmdCheckTxnStatus:
		return true
	case CmdCheckSecondaryLocks:
		return true
	case CmdFlashbackToVersion:
		return true
	case CmdPrepareFlashbackToVersion:
		return true
	case CmdFlush:
		return true
	case CmdBufferBatchGet:
		return true
	case CmdCopStream, CmdMPPTask, CmdMPPConn, CmdMPPCancel, CmdMPPAlive, CmdEmpty:
		return true
	default:
		return false
	}
}
