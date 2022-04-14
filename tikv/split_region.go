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

// NOTE: The code in this file is based on code from the
// TiDB project, licensed under the Apache License v 2.0
//
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/split_region.go
//

// Copyright 2017 PingCAP, Inc.
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

package tikv

import (
	"bytes"
	"context"
	"fmt"
	"math"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pkg/errors"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/kvrpc"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/internal/retry"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/txnkv/rangetask"
	"github.com/tikv/client-go/v2/util"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

const splitBatchRegionLimit = 2048

func equalRegionStartKey(key, regionStartKey []byte) bool {
	return bytes.Equal(key, regionStartKey)
}

func (s *KVStore) splitBatchRegionsReq(bo *Backoffer, keys [][]byte, scatter bool, tableID *int64) (*pdpb.SplitRegionsResponse, error) {
	// equalRegionStartKey is used to filter split keys.
	// If the split key is equal to the start key of the region, then the key has been split, we need to skip the split key.
	groups, _, err := s.regionCache.GroupKeysByRegion(bo, keys, equalRegionStartKey)
	if err != nil {
		return nil, err
	}

	var batches []kvrpc.Batch
	for regionID, groupKeys := range groups {
		batches = kvrpc.AppendKeyBatches(batches, regionID, groupKeys, splitBatchRegionLimit)
	}

	if len(batches) == 0 {
		return nil, nil
	}
	// The first time it enters this function.
	if bo.GetTotalSleep() == 0 {
		logutil.BgLogger().Info("split batch regions request",
			zap.Int("split key count", len(keys)),
			zap.Int("batch count", len(batches)),
			zap.Uint64("first batch, region ID", batches[0].RegionID.GetID()),
			zap.String("first split key", kv.StrKey(batches[0].Keys[0])))
	}
	if len(batches) == 1 {
		resp := s.batchSendSingleRegion(bo, batches[0], scatter, tableID)
		return resp, err
	}
	ch := make(chan *pdpb.SplitRegionsResponse, len(batches))
	for _, batch1 := range batches {
		go func(b kvrpc.Batch) {
			backoffer, cancel := bo.Fork()
			defer cancel()

			util.WithRecovery(func() {
				select {
				case ch <- s.batchSendSingleRegion(backoffer, b, scatter, tableID):
				case <-bo.GetCtx().Done():
					resp := &pdpb.SplitRegionsResponse{}
					resp.Header.Error = &pdpb.Error{Message: err.Error()}
					ch <- resp
				}
			}, func(r interface{}) {
				if r != nil {
					resp := &pdpb.SplitRegionsResponse{}
					resp.Header.Error = &pdpb.Error{Message: err.Error()}
					ch <- resp
				}
			})
		}(batch1)
	}

	srResp := &pdpb.SplitRegionsResponse{RegionsId: make([]uint64, len(keys)*2)}
	for i := 0; i < len(batches); i++ {
		batchResp := <-ch
		if batchResp.Header.Error != nil {
			respErr := errors.New(batchResp.Header.Error.GetMessage())
			logutil.BgLogger().Info("batch split regions failed", zap.Error(respErr))
			if err == nil {
				err = respErr
			}
		}

		// If the split succeeds and the scatter fails, we also need to add the region IDs.
		srResp.RegionsId = append(srResp.RegionsId, batchResp.RegionsId...)
	}
	return srResp, err
}

func (s *KVStore) batchSendSingleRegion(bo *Backoffer, batch kvrpc.Batch, scatter bool, tableID *int64) *pdpb.SplitRegionsResponse {
	if val, err := util.EvalFailpoint("mockSplitRegionTimeout"); err == nil {
		if val.(bool) {
			if _, ok := bo.GetCtx().Deadline(); ok {
				<-bo.GetCtx().Done()
			}
		}
	}

	resp, err := s.pdClient.SplitRegions(bo.GetCtx(), batch.Keys)
	if err != nil {
		resp := &pdpb.SplitRegionsResponse{}
		resp.Header = &pdpb.ResponseHeader{}
		resp.Header.Error = &pdpb.Error{Message: err.Error()}
		return resp
	}
	if resp == nil {
		return &pdpb.SplitRegionsResponse{
			Header: &pdpb.ResponseHeader{
				Error: &pdpb.Error{Message: "empty response"},
			},
		}
	}
	regionIDs := resp.GetRegionsId()
	if len(regionIDs) > 0 {
		// Divide a region into n, one of them may not need to be scattered,
		// so n-1 needs to be scattered to other stores.
		regionIDs = regionIDs[:len(regionIDs)-1]
	}

	logutil.BgLogger().Info("batch split regions complete",
		zap.Uint64("batch region ID", batch.RegionID.GetID()),
		zap.String("first at", kv.StrKey(batch.Keys[0])),
		zap.Int("new region count", len(regionIDs)))
	if resp.FinishedPercentage != 100 {
		err = errors.Errorf("Fail to batch split regions, finishedPercentage : %d, batch region ID : %d",
			resp.FinishedPercentage, batch.RegionID.GetID())
		resp.Header.Error = &pdpb.Error{Message: err.Error()}
	}
	if !scatter {
		return resp
	}
	for i, id := range regionIDs {
		if err = s.scatterRegion(bo, id, tableID); err == nil {
			logutil.BgLogger().Info("batch split regions, scatter region complete",
				zap.Uint64("batch region ID", batch.RegionID.GetID()),
				zap.String("at", kv.StrKey(batch.Keys[i])))
			continue
		}

		logutil.BgLogger().Info("batch split regions, scatter region failed",
			zap.Uint64("batch region ID", batch.RegionID.GetID()),
			zap.String("at", kv.StrKey(batch.Keys[i])),
			zap.Error(err))
		if _, ok := err.(*tikverr.ErrPDServerTimeout); ok {
			break
		}
	}
	return resp
}

const (
	splitRegionBackoff     = 20000
	maxSplitRegionsBackoff = 120000
)

// SplitRegions splits regions by splitKeys.
func (s *KVStore) SplitRegions(ctx context.Context, splitKeys [][]byte, scatter bool, tableID *int64) (regionIDs []uint64, err error) {
	bo := retry.NewBackofferWithVars(ctx, int(math.Min(float64(len(splitKeys))*splitRegionBackoff, maxSplitRegionsBackoff)), nil)
	resp, err := s.splitBatchRegionsReq(bo, splitKeys, scatter, tableID)
	regionIDs = make([]uint64, 0, len(splitKeys))
	if resp != nil {
		regionIDs = append(regionIDs, resp.GetRegionsId()...)
		logutil.BgLogger().Info("split regions complete", zap.Int("region count", len(regionIDs)), zap.Uint64s("region IDs", regionIDs))
	}
	return regionIDs, err
}

func (s *KVStore) scatterRegion(bo *Backoffer, regionID uint64, tableID *int64) error {
	logutil.BgLogger().Info("start scatter region",
		zap.Uint64("regionID", regionID))
	for {
		opts := make([]pd.RegionsOption, 0, 1)
		if tableID != nil {
			opts = append(opts, pd.WithGroup(fmt.Sprintf("%v", *tableID)))
		}
		_, err := s.pdClient.ScatterRegions(bo.GetCtx(), []uint64{regionID}, opts...)

		if val, err2 := util.EvalFailpoint("mockScatterRegionTimeout"); err2 == nil {
			if val.(bool) {
				err = tikverr.NewErrPDServerTimeout("")
			}
		}

		if err == nil {
			break
		}
		err = bo.Backoff(retry.BoPDRPC, errors.New(err.Error()))
		if err != nil {
			return err
		}
	}
	logutil.BgLogger().Debug("scatter region complete",
		zap.Uint64("regionID", regionID))
	return nil
}

const waitScatterRegionFinishBackoff = 120000

// WaitScatterRegionFinish implements SplittableStore interface.
// backOff is the back off time of the wait scatter region.(Milliseconds)
// if backOff <= 0, the default wait scatter back off time will be used.
func (s *KVStore) WaitScatterRegionFinish(ctx context.Context, regionID uint64, backOff int) error {
	if backOff <= 0 {
		backOff = waitScatterRegionFinishBackoff
	}
	logutil.BgLogger().Info("wait scatter region",
		zap.Uint64("regionID", regionID), zap.Int("backoff(ms)", backOff))

	bo := retry.NewBackofferWithVars(ctx, backOff, nil)
	logFreq := 0
	for {
		resp, err := s.pdClient.GetOperator(ctx, regionID)
		if err == nil && resp != nil {
			if !bytes.Equal(resp.Desc, []byte("scatter-region")) || resp.Status != pdpb.OperatorStatus_RUNNING {
				logutil.BgLogger().Info("wait scatter region finished",
					zap.Uint64("regionID", regionID))
				return nil
			}
			if resp.GetHeader().GetError() != nil {
				err = errors.WithStack(&tikverr.PDError{
					Err: resp.Header.Error,
				})
				logutil.BgLogger().Warn("wait scatter region error",
					zap.Uint64("regionID", regionID), zap.Error(err))
				return err
			}
			if logFreq%10 == 0 {
				logutil.BgLogger().Info("wait scatter region",
					zap.Uint64("regionID", regionID),
					zap.String("reverse", string(resp.Desc)),
					zap.String("status", pdpb.OperatorStatus_name[int32(resp.Status)]))
			}
			logFreq++
		}
		if err != nil {
			err = bo.Backoff(retry.BoRegionMiss, errors.New(err.Error()))
		} else {
			err = bo.Backoff(retry.BoRegionMiss, errors.New("wait scatter region timeout"))
		}
		if err != nil {
			return err
		}
	}
}

// CheckRegionInScattering uses to check whether scatter region finished.
func (s *KVStore) CheckRegionInScattering(regionID uint64) (bool, error) {
	bo := rangetask.NewLocateRegionBackoffer(context.Background())
	for {
		resp, err := s.pdClient.GetOperator(context.Background(), regionID)
		if err == nil && resp != nil {
			if !bytes.Equal(resp.Desc, []byte("scatter-region")) || resp.Status != pdpb.OperatorStatus_RUNNING {
				return false, nil
			}
		}
		if err != nil {
			err = bo.Backoff(retry.BoRegionMiss, errors.New(err.Error()))
		} else {
			return true, nil
		}
		if err != nil {
			return true, err
		}
	}
}
