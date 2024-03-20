#!/bin/bash

output="cmds_generated.go"

cat <<EOF > $output
// Code generated gen.sh. DO NOT EDIT.

package tikvrpc

import (
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
)
EOF

cmds=(
  Get
  Scan
  Prewrite
  PessimisticLock
  PessimisticRollback
  Commit
  Cleanup
  BatchGet
  BatchRollback
  ScanLock
  ResolveLock
  GC
  DeleteRange
  RawGet
  RawBatchGet
  RawPut
  RawBatchPut
  RawDelete
  RawBatchDelete
  RawDeleteRange
  RawScan
  RawGetKeyTTL
  RawCompareAndSwap
  RawChecksum
  UnsafeDestroyRange
  RegisterLockObserver
  CheckLockObserver
  RemoveLockObserver
  PhysicalScanLock
  Cop
  BatchCop
  MvccGetByKey
  MvccGetByStartTs
  SplitRegion
  TxnHeartBeat
  CheckTxnStatus
  CheckSecondaryLocks
  FlashbackToVersion
  PrepareFlashbackToVersion
  Flush
  BufferBatchGet
)

cat <<EOF >> $output

func patchCmdCtx(req *Request, cmd CmdType, ctx *kvrpcpb.Context) bool {
	switch cmd {
EOF

for cmd in "${cmds[@]}"; do
cat <<EOF >> $output
	case Cmd${cmd}:
		if req.rev == 0 {
			req.${cmd}().Context = ctx
		} else {
			cmd := *req.${cmd}()
			cmd.Context = ctx
			req.Req = &cmd
		}
		req.rev++
EOF
done

cat <<EOF >> $output
	default:
		return false
	}
	return true
}
EOF
