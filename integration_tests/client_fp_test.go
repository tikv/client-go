// Copyright 2021 TiKV Authors
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

package tikv_test

import (
	"context"
	"time"

	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pkg/errors"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util"
)

// mock TiKV RPC client that hooks message by failpoint
type fpClient struct {
	tikv.Client
}

func (c fpClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	switch req.Type {
	case tikvrpc.CmdPrewrite:
		if val, err := util.EvalFailpoint("rpcPrewriteResult"); err == nil && val != nil {
			switch val.(string) {
			case "timeout":
				return nil, errors.New("timeout")
			case "writeConflict":
				return &tikvrpc.Response{
					Resp: &kvrpcpb.PrewriteResponse{Errors: []*kvrpcpb.KeyError{{Conflict: &kvrpcpb.WriteConflict{}}}},
				}, nil
			case "undeterminedResult":
				return &tikvrpc.Response{
					Resp: &kvrpcpb.PrewriteResponse{RegionError: &errorpb.Error{
						UndeterminedResult: &errorpb.UndeterminedResult{},
					}},
				}, nil
			}
		}
	case tikvrpc.CmdBatchGet:
		batchGetReq := req.BatchGet()
		if val, err := util.EvalFailpoint("rpcBatchGetResult"); err == nil {
			switch val.(string) {
			case "keyError":
				return &tikvrpc.Response{
					Resp: &kvrpcpb.BatchGetResponse{Error: &kvrpcpb.KeyError{
						Locked: &kvrpcpb.LockInfo{
							PrimaryLock: batchGetReq.Keys[0],
							LockVersion: batchGetReq.Version - 1,
							Key:         batchGetReq.Keys[0],
							LockTtl:     50,
							TxnSize:     1,
							LockType:    kvrpcpb.Op_Put,
						},
					}},
				}, nil
			}
		}
	case tikvrpc.CmdScan:
		kvScanReq := req.Scan()
		if val, err := util.EvalFailpoint("rpcScanResult"); err == nil {
			switch val.(string) {
			case "keyError":
				return &tikvrpc.Response{
					Resp: &kvrpcpb.ScanResponse{Error: &kvrpcpb.KeyError{
						Locked: &kvrpcpb.LockInfo{
							PrimaryLock: kvScanReq.StartKey,
							LockVersion: kvScanReq.Version - 1,
							Key:         kvScanReq.StartKey,
							LockTtl:     50,
							TxnSize:     1,
							LockType:    kvrpcpb.Op_Put,
						},
					}},
				}, nil
			}
		}
	case tikvrpc.CmdCommit:
		if val, err := util.EvalFailpoint("rpcCommitResult"); err == nil && val != nil {
			switch val.(string) {
			case "undeterminedResult":
				return &tikvrpc.Response{
					Resp: &kvrpcpb.CommitResponse{RegionError: &errorpb.Error{
						UndeterminedResult: &errorpb.UndeterminedResult{},
					}},
				}, nil
			}
		}
	}

	res, err := c.Client.SendRequest(ctx, addr, req, timeout)

	switch req.Type {
	case tikvrpc.CmdPrewrite:
		if val, err := util.EvalFailpoint("rpcPrewriteTimeout"); err == nil {
			if val.(bool) {
				return nil, tikverr.ErrResultUndetermined
			}
		}
	}
	return res, err
}
