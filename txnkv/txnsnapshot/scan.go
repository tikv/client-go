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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/scan.go
//

// Copyright 2016 PingCAP, Inc.
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

package txnsnapshot

import (
	"bytes"
	"context"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pkg/errors"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/client"
	"github.com/tikv/client-go/v2/internal/locate"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/internal/retry"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/tikvrpc/interceptor"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
	"go.uber.org/zap"
)

// Scanner support tikv scan
type Scanner struct {
	snapshot     *KVSnapshot
	batchSize    int
	cache        []*kvrpcpb.KvPair
	idx          int
	nextStartKey []byte
	endKey       []byte

	// Use for reverse scan.
	nextEndKey []byte
	reverse    bool

	valid bool
	eof   bool
}

func newScanner(snapshot *KVSnapshot, startKey []byte, endKey []byte, batchSize int, reverse bool) (*Scanner, error) {
	// It must be > 1. Otherwise scanner won't skipFirst.
	if batchSize <= 1 {
		batchSize = DefaultScanBatchSize
	}
	scanner := &Scanner{
		snapshot:     snapshot,
		batchSize:    batchSize,
		valid:        true,
		nextStartKey: startKey,
		endKey:       endKey,
		reverse:      reverse,
		nextEndKey:   endKey,
	}
	err := scanner.Next()
	if tikverr.IsErrNotFound(err) {
		return scanner, nil
	}
	return scanner, err
}

// Valid return valid.
func (s *Scanner) Valid() bool {
	return s.valid
}

// Key return key.
func (s *Scanner) Key() []byte {
	if s.valid {
		return s.cache[s.idx].Key
	}
	return nil
}

// Value return value.
func (s *Scanner) Value() []byte {
	if s.valid {
		return s.cache[s.idx].Value
	}
	return nil
}

const scannerNextMaxBackoff = 20000

// Next return next element.
func (s *Scanner) Next() error {
	bo := retry.NewBackofferWithVars(context.WithValue(context.Background(), retry.TxnStartKey, s.snapshot.version), scannerNextMaxBackoff, s.snapshot.vars)
	if !s.valid {
		return errors.New("scanner iterator is invalid")
	}
	s.snapshot.mu.RLock()
	if s.snapshot.mu.interceptor != nil {
		// User has called snapshot.SetRPCInterceptor() to explicitly set an interceptor, we
		// need to bind it to ctx so that the internal client can perceive and execute
		// it before initiating an RPC request.
		bo.SetCtx(interceptor.WithRPCInterceptor(bo.GetCtx(), s.snapshot.mu.interceptor))
	}
	s.snapshot.mu.RUnlock()
	var err error
	for {
		s.idx++
		if s.idx >= len(s.cache) {
			if s.eof {
				s.Close()
				return nil
			}
			err = s.getData(bo)
			if err != nil {
				s.Close()
				return err
			}
			if s.idx >= len(s.cache) {
				continue
			}
		}

		current := s.cache[s.idx]
		if (!s.reverse && len(s.endKey) > 0 && kv.CmpKey(current.Key, s.endKey) >= 0) ||
			(s.reverse && len(s.nextStartKey) > 0 && kv.CmpKey(current.Key, s.nextStartKey) < 0) {
			s.eof = true
			s.Close()
			return nil
		}
		// Try to resolve the lock
		if current.GetError() != nil {
			// 'current' would be modified if the lock being resolved
			if err := s.resolveCurrentLock(bo, current); err != nil {
				s.Close()
				return err
			}

			// The check here does not violate the KeyOnly semantic, because current's value
			// is filled by resolveCurrentLock which fetches the value by snapshot.get, so an empty
			// value stands for NotExist
			if len(current.Value) == 0 {
				continue
			}
		}
		return nil
	}
}

// Close close iterator.
func (s *Scanner) Close() {
	s.valid = false
}

func (s *Scanner) startTS() uint64 {
	return s.snapshot.version
}

func (s *Scanner) resolveCurrentLock(bo *retry.Backoffer, current *kvrpcpb.KvPair) error {
	ctx := context.Background()
	val, err := s.snapshot.get(ctx, bo, current.Key)
	if err != nil {
		return err
	}
	current.Error = nil
	current.Value = val
	return nil
}

func (s *Scanner) getData(bo *retry.Backoffer) error {
	logutil.BgLogger().Debug("txn getData",
		zap.String("nextStartKey", kv.StrKey(s.nextStartKey)),
		zap.String("nextEndKey", kv.StrKey(s.nextEndKey)),
		zap.Bool("reverse", s.reverse),
		zap.Uint64("txnStartTS", s.startTS()))
	sender := locate.NewRegionRequestSender(s.snapshot.store.GetRegionCache(), s.snapshot.store.GetTiKVClient())
	var reqEndKey, reqStartKey []byte
	var loc *locate.KeyLocation
	var resolvingRecordToken *int
	var err error
	// the states in request need to keep when retry request.
	var readType string
	for {
		if !s.reverse {
			loc, err = s.snapshot.store.GetRegionCache().LocateKey(bo, s.nextStartKey)
		} else {
			loc, err = s.snapshot.store.GetRegionCache().LocateEndKey(bo, s.nextEndKey)
		}
		if err != nil {
			return err
		}

		if !s.reverse {
			reqEndKey = s.endKey
			if len(reqEndKey) == 0 ||
				(len(loc.EndKey) > 0 && bytes.Compare(loc.EndKey, reqEndKey) < 0) {
				reqEndKey = loc.EndKey
			}
		} else {
			reqStartKey = s.nextStartKey
			if len(reqStartKey) == 0 ||
				(len(loc.StartKey) > 0 && bytes.Compare(loc.StartKey, reqStartKey) > 0) {
				reqStartKey = loc.StartKey
			}
		}
		sreq := &kvrpcpb.ScanRequest{
			StartKey:   s.nextStartKey,
			EndKey:     reqEndKey,
			Limit:      uint32(s.batchSize),
			Version:    s.startTS(),
			KeyOnly:    s.snapshot.keyOnly,
			SampleStep: s.snapshot.sampleStep,
		}
		if s.reverse {
			sreq.StartKey = s.nextEndKey
			sreq.EndKey = reqStartKey
			sreq.Reverse = true
		}
		s.snapshot.mu.RLock()
		req := tikvrpc.NewReplicaReadRequest(tikvrpc.CmdScan, sreq, s.snapshot.mu.replicaRead, &s.snapshot.replicaReadSeed, kvrpcpb.Context{
			Priority:         s.snapshot.priority.ToPB(),
			NotFillCache:     s.snapshot.notFillCache,
			TaskId:           s.snapshot.mu.taskID,
			ResourceGroupTag: s.snapshot.mu.resourceGroupTag,
			IsolationLevel:   s.snapshot.isolationLevel.ToPB(),
			ResourceControlContext: &kvrpcpb.ResourceControlContext{
				ResourceGroupName: s.snapshot.mu.resourceGroupName,
			},
			BusyThresholdMs: uint32(s.snapshot.mu.busyThreshold.Milliseconds()),
		})
		if readType != "" {
			req.ReadType = readType
			req.IsRetryRequest = true
		}
		req.InputRequestSource = s.snapshot.GetRequestSource()
		if s.snapshot.mu.resourceGroupTag == nil && s.snapshot.mu.resourceGroupTagger != nil {
			s.snapshot.mu.resourceGroupTagger(req)
		}
		s.snapshot.mu.RUnlock()
		resp, _, err := sender.SendReq(bo, req, loc.Region, client.ReadTimeoutMedium)
		if err != nil {
			return err
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return err
		}
		readType = req.ReadType
		if regionErr != nil {
			logutil.BgLogger().Debug("scanner getData failed",
				zap.Stringer("regionErr", regionErr))
			// For other region error and the fake region error, backoff because
			// there's something wrong.
			// For the real EpochNotMatch error, don't backoff.
			if regionErr.GetEpochNotMatch() == nil || locate.IsFakeRegionError(regionErr) {
				err = bo.Backoff(retry.BoRegionMiss, errors.New(regionErr.String()))
				if err != nil {
					return err
				}
			}
			continue
		}
		if resp.Resp == nil {
			return errors.WithStack(tikverr.ErrBodyMissing)
		}
		cmdScanResp := resp.Resp.(*kvrpcpb.ScanResponse)

		err = s.snapshot.store.CheckVisibility(s.startTS())
		if err != nil {
			return err
		}

		// When there is a response-level key error, the returned pairs are incomplete.
		// We should resolve the lock first and then retry the same request.
		if keyErr := cmdScanResp.GetError(); keyErr != nil {
			lock, err := txnlock.ExtractLockFromKeyErr(keyErr)
			if err != nil {
				return err
			}
			locks := []*txnlock.Lock{lock}
			if resolvingRecordToken == nil {
				token := s.snapshot.store.GetLockResolver().RecordResolvingLocks(locks, s.snapshot.version)
				resolvingRecordToken = &token
				defer s.snapshot.store.GetLockResolver().ResolveLocksDone(s.snapshot.version, *resolvingRecordToken)
			} else {
				s.snapshot.store.GetLockResolver().UpdateResolvingLocks(locks, s.snapshot.version, *resolvingRecordToken)
			}
			msBeforeExpired, err := s.snapshot.store.GetLockResolver().ResolveLocks(bo, s.snapshot.version, locks)
			if err != nil {
				return err
			}
			if msBeforeExpired > 0 {
				err = bo.BackoffWithMaxSleepTxnLockFast(int(msBeforeExpired), errors.Errorf("key is locked during scanning"))
				if err != nil {
					return err
				}
			}
			continue
		}

		kvPairs := cmdScanResp.Pairs
		// Check if kvPair contains error, it should be a Lock.
		for _, pair := range kvPairs {
			if keyErr := pair.GetError(); keyErr != nil && len(pair.Key) == 0 {
				lock, err := txnlock.ExtractLockFromKeyErr(keyErr)
				if err != nil {
					return err
				}
				pair.Key = lock.Key
			}
		}

		s.cache, s.idx = kvPairs, 0
		if len(kvPairs) < s.batchSize {
			// No more data in current Region. Next getData() starts
			// from current Region's endKey.
			if !s.reverse {
				s.nextStartKey = loc.EndKey
			} else {
				s.nextEndKey = reqStartKey
			}
			if (!s.reverse && (len(loc.EndKey) == 0 || (len(s.endKey) > 0 && kv.CmpKey(s.nextStartKey, s.endKey) >= 0))) ||
				(s.reverse && (len(loc.StartKey) == 0 || (len(s.nextStartKey) > 0 && kv.CmpKey(s.nextStartKey, s.nextEndKey) >= 0))) {
				// Current Region is the last one.
				s.eof = true
			}
			return nil
		}
		// next getData() starts from the last key in kvPairs (but skip
		// it by appending a '\x00' to the key). Note that next getData()
		// may get an empty response if the Region in fact does not have
		// more data.
		lastKey := kvPairs[len(kvPairs)-1].GetKey()
		if !s.reverse {
			s.nextStartKey = kv.NextKey(lastKey)
		} else {
			s.nextEndKey = lastKey
		}
		return nil
	}
}
