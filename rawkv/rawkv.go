// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package rawkv

import (
	"bytes"
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	pd "github.com/pingcap/pd/client"
	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/locate"
	"github.com/tikv/client-go/metrics"
	"github.com/tikv/client-go/retry"
	"github.com/tikv/client-go/rpc"
)

var (
	// MaxRawKVScanLimit is the maximum scan limit for rawkv Scan.
	MaxRawKVScanLimit = 10240
	// ErrMaxScanLimitExceeded is returned when the limit for rawkv Scan is to large.
	ErrMaxScanLimitExceeded = errors.New("limit should be less than MaxRawKVScanLimit")
)

const (
	// rawBatchPutSize is the maximum size limit for rawkv each batch put request.
	rawBatchPutSize = 16 * 1024
	// rawBatchPairCount is the maximum limit for rawkv each batch get/delete request.
	rawBatchPairCount = 512
)

// RawKVClient is a client of TiKV server which is used as a key-value storage,
// only GET/PUT/DELETE commands are supported.
type RawKVClient struct {
	clusterID   uint64
	regionCache *locate.RegionCache
	pdClient    pd.Client
	rpcClient   rpc.Client
}

// NewRawKVClient creates a client with PD cluster addrs.
func NewRawKVClient(pdAddrs []string, security config.Security) (*RawKVClient, error) {
	pdCli, err := pd.NewClient(pdAddrs, pd.SecurityOption{
		CAPath:   security.SSLCA,
		CertPath: security.SSLCert,
		KeyPath:  security.SSLKey,
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &RawKVClient{
		clusterID:   pdCli.GetClusterID(context.TODO()),
		regionCache: locate.NewRegionCache(pdCli),
		pdClient:    pdCli,
		rpcClient:   rpc.NewRPCClient(security),
	}, nil
}

// Close closes the client.
func (c *RawKVClient) Close() error {
	c.pdClient.Close()
	return c.rpcClient.Close()
}

// ClusterID returns the TiKV cluster ID.
func (c *RawKVClient) ClusterID() uint64 {
	return c.clusterID
}

// Get queries value with the key. When the key does not exist, it returns `nil, nil`.
func (c *RawKVClient) Get(key []byte) ([]byte, error) {
	start := time.Now()
	defer func() { metrics.RawkvCmdHistogram.WithLabelValues("get").Observe(time.Since(start).Seconds()) }()

	req := &rpc.Request{
		Type: rpc.CmdRawGet,
		RawGet: &kvrpcpb.RawGetRequest{
			Key: key,
		},
	}
	resp, _, err := c.sendReq(key, req)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	cmdResp := resp.RawGet
	if cmdResp == nil {
		return nil, errors.WithStack(ErrBodyMissing)
	}
	if cmdResp.GetError() != "" {
		return nil, errors.New(cmdResp.GetError())
	}
	if len(cmdResp.Value) == 0 {
		return nil, nil
	}
	return cmdResp.Value, nil
}

// BatchGet queries values with the keys.
func (c *RawKVClient) BatchGet(keys [][]byte) ([][]byte, error) {
	start := time.Now()
	defer func() {
		metrics.RawkvCmdHistogram.WithLabelValues("batch_get").Observe(time.Since(start).Seconds())
	}()

	bo := retry.NewBackoffer(context.Background(), retry.RawkvMaxBackoff)
	resp, err := c.sendBatchReq(bo, keys, rpc.CmdRawBatchGet)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	cmdResp := resp.RawBatchGet
	if cmdResp == nil {
		return nil, errors.WithStack(ErrBodyMissing)
	}

	keyToValue := make(map[string][]byte, len(keys))
	for _, pair := range cmdResp.Pairs {
		keyToValue[string(pair.Key)] = pair.Value
	}

	values := make([][]byte, len(keys))
	for i, key := range keys {
		values[i] = keyToValue[string(key)]
	}
	return values, nil
}

// Put stores a key-value pair to TiKV.
func (c *RawKVClient) Put(key, value []byte) error {
	start := time.Now()
	defer func() { metrics.RawkvCmdHistogram.WithLabelValues("put").Observe(time.Since(start).Seconds()) }()
	metrics.RawkvSizeHistogram.WithLabelValues("key").Observe(float64(len(key)))
	metrics.RawkvSizeHistogram.WithLabelValues("value").Observe(float64(len(value)))

	if len(value) == 0 {
		return errors.New("empty value is not supported")
	}

	req := &rpc.Request{
		Type: rpc.CmdRawPut,
		RawPut: &kvrpcpb.RawPutRequest{
			Key:   key,
			Value: value,
		},
	}
	resp, _, err := c.sendReq(key, req)
	if err != nil {
		return errors.WithStack(err)
	}
	cmdResp := resp.RawPut
	if cmdResp == nil {
		return errors.WithStack(ErrBodyMissing)
	}
	if cmdResp.GetError() != "" {
		return errors.New(cmdResp.GetError())
	}
	return nil
}

// BatchPut stores key-value pairs to TiKV.
func (c *RawKVClient) BatchPut(keys, values [][]byte) error {
	start := time.Now()
	defer func() {
		metrics.RawkvCmdHistogram.WithLabelValues("batch_put").Observe(time.Since(start).Seconds())
	}()

	if len(keys) != len(values) {
		return errors.New("the len of keys is not equal to the len of values")
	}
	for _, value := range values {
		if len(value) == 0 {
			return errors.New("empty value is not supported")
		}
	}
	bo := retry.NewBackoffer(context.Background(), retry.RawkvMaxBackoff)
	err := c.sendBatchPut(bo, keys, values)
	return errors.WithStack(err)
}

// Delete deletes a key-value pair from TiKV.
func (c *RawKVClient) Delete(key []byte) error {
	start := time.Now()
	defer func() { metrics.RawkvCmdHistogram.WithLabelValues("delete").Observe(time.Since(start).Seconds()) }()

	req := &rpc.Request{
		Type: rpc.CmdRawDelete,
		RawDelete: &kvrpcpb.RawDeleteRequest{
			Key: key,
		},
	}
	resp, _, err := c.sendReq(key, req)
	if err != nil {
		return errors.WithStack(err)
	}
	cmdResp := resp.RawDelete
	if cmdResp == nil {
		return errors.WithStack(ErrBodyMissing)
	}
	if cmdResp.GetError() != "" {
		return errors.New(cmdResp.GetError())
	}
	return nil
}

// BatchDelete deletes key-value pairs from TiKV
func (c *RawKVClient) BatchDelete(keys [][]byte) error {
	start := time.Now()
	defer func() {
		metrics.RawkvCmdHistogram.WithLabelValues("batch_delete").Observe(time.Since(start).Seconds())
	}()

	bo := retry.NewBackoffer(context.Background(), retry.RawkvMaxBackoff)
	resp, err := c.sendBatchReq(bo, keys, rpc.CmdRawBatchDelete)
	if err != nil {
		return errors.WithStack(err)
	}
	cmdResp := resp.RawBatchDelete
	if cmdResp == nil {
		return errors.WithStack(ErrBodyMissing)
	}
	if cmdResp.GetError() != "" {
		return errors.New(cmdResp.GetError())
	}
	return nil
}

// DeleteRange deletes all key-value pairs in a range from TiKV
func (c *RawKVClient) DeleteRange(startKey []byte, endKey []byte) error {
	start := time.Now()
	var err error
	defer func() {
		var label = "delete_range"
		if err != nil {
			label += "_error"
		}
		metrics.RawkvCmdHistogram.WithLabelValues(label).Observe(time.Since(start).Seconds())
	}()

	// Process each affected region respectively
	for !bytes.Equal(startKey, endKey) {
		var resp *rpc.Response
		var actualEndKey []byte
		resp, actualEndKey, err = c.sendDeleteRangeReq(startKey, endKey)
		if err != nil {
			return errors.WithStack(err)
		}
		cmdResp := resp.RawDeleteRange
		if cmdResp == nil {
			return errors.WithStack(ErrBodyMissing)
		}
		if cmdResp.GetError() != "" {
			return errors.New(cmdResp.GetError())
		}
		startKey = actualEndKey
	}

	return nil
}

// Scan queries continuous kv pairs in range [startKey, endKey), up to limit pairs.
// If endKey is empty, it means unbounded.
// If you want to exclude the startKey or include the endKey, append a '\0' to the key. For example, to scan
// (startKey, endKey], you can write:
// `Scan(append(startKey, '\0'), append(endKey, '\0'), limit)`.
func (c *RawKVClient) Scan(startKey, endKey []byte, limit int) (keys [][]byte, values [][]byte, err error) {
	start := time.Now()
	defer func() { metrics.RawkvCmdHistogram.WithLabelValues("raw_scan").Observe(time.Since(start).Seconds()) }()

	if limit > MaxRawKVScanLimit {
		return nil, nil, errors.WithStack(ErrMaxScanLimitExceeded)
	}

	for len(keys) < limit {
		req := &rpc.Request{
			Type: rpc.CmdRawScan,
			RawScan: &kvrpcpb.RawScanRequest{
				StartKey: startKey,
				EndKey:   endKey,
				Limit:    uint32(limit - len(keys)),
			},
		}
		resp, loc, err := c.sendReq(startKey, req)
		if err != nil {
			return nil, nil, errors.WithStack(err)
		}
		cmdResp := resp.RawScan
		if cmdResp == nil {
			return nil, nil, errors.WithStack(ErrBodyMissing)
		}
		for _, pair := range cmdResp.Kvs {
			keys = append(keys, pair.Key)
			values = append(values, pair.Value)
		}
		startKey = loc.EndKey
		if len(startKey) == 0 {
			break
		}
	}
	return
}

func (c *RawKVClient) sendReq(key []byte, req *rpc.Request) (*rpc.Response, *locate.KeyLocation, error) {
	bo := retry.NewBackoffer(context.Background(), retry.RawkvMaxBackoff)
	sender := rpc.NewRegionRequestSender(c.regionCache, c.rpcClient)
	for {
		loc, err := c.regionCache.LocateKey(bo, key)
		if err != nil {
			return nil, nil, errors.WithStack(err)
		}
		resp, err := sender.SendReq(bo, req, loc.Region, rpc.ReadTimeoutShort)
		if err != nil {
			return nil, nil, errors.WithStack(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return nil, nil, errors.WithStack(err)
		}
		if regionErr != nil {
			err := bo.Backoff(retry.BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return nil, nil, errors.WithStack(err)
			}
			continue
		}
		return resp, loc, nil
	}
}

func (c *RawKVClient) sendBatchReq(bo *retry.Backoffer, keys [][]byte, cmdType rpc.CmdType) (*rpc.Response, error) { // split the keys
	groups, _, err := c.regionCache.GroupKeysByRegion(bo, keys)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	var batches []batch
	for regionID, groupKeys := range groups {
		batches = appendKeyBatches(batches, regionID, groupKeys, rawBatchPairCount)
	}
	bo, cancel := bo.Fork()
	ches := make(chan singleBatchResp, len(batches))
	for _, batch := range batches {
		batch1 := batch
		go func() {
			singleBatchBackoffer, singleBatchCancel := bo.Fork()
			defer singleBatchCancel()
			ches <- c.doBatchReq(singleBatchBackoffer, batch1, cmdType)
		}()
	}

	var firstError error
	var resp *rpc.Response
	switch cmdType {
	case rpc.CmdRawBatchGet:
		resp = &rpc.Response{Type: rpc.CmdRawBatchGet, RawBatchGet: &kvrpcpb.RawBatchGetResponse{}}
	case rpc.CmdRawBatchDelete:
		resp = &rpc.Response{Type: rpc.CmdRawBatchDelete, RawBatchDelete: &kvrpcpb.RawBatchDeleteResponse{}}
	}
	for i := 0; i < len(batches); i++ {
		singleResp, ok := <-ches
		if ok {
			if singleResp.err != nil {
				cancel()
				if firstError == nil {
					firstError = singleResp.err
				}
			} else if cmdType == rpc.CmdRawBatchGet {
				cmdResp := singleResp.resp.RawBatchGet
				resp.RawBatchGet.Pairs = append(resp.RawBatchGet.Pairs, cmdResp.Pairs...)
			}
		}
	}

	return resp, firstError
}

func (c *RawKVClient) doBatchReq(bo *retry.Backoffer, batch batch, cmdType rpc.CmdType) singleBatchResp {
	var req *rpc.Request
	switch cmdType {
	case rpc.CmdRawBatchGet:
		req = &rpc.Request{
			Type: cmdType,
			RawBatchGet: &kvrpcpb.RawBatchGetRequest{
				Keys: batch.keys,
			},
		}
	case rpc.CmdRawBatchDelete:
		req = &rpc.Request{
			Type: cmdType,
			RawBatchDelete: &kvrpcpb.RawBatchDeleteRequest{
				Keys: batch.keys,
			},
		}
	}

	sender := rpc.NewRegionRequestSender(c.regionCache, c.rpcClient)
	resp, err := sender.SendReq(bo, req, batch.regionID, rpc.ReadTimeoutShort)

	batchResp := singleBatchResp{}
	if err != nil {
		batchResp.err = errors.WithStack(err)
		return batchResp
	}
	regionErr, err := resp.GetRegionError()
	if err != nil {
		batchResp.err = errors.WithStack(err)
		return batchResp
	}
	if regionErr != nil {
		err := bo.Backoff(retry.BoRegionMiss, errors.New(regionErr.String()))
		if err != nil {
			batchResp.err = errors.WithStack(err)
			return batchResp
		}
		resp, err = c.sendBatchReq(bo, batch.keys, cmdType)
		batchResp.resp = resp
		batchResp.err = err
		return batchResp
	}

	switch cmdType {
	case rpc.CmdRawBatchGet:
		batchResp.resp = resp
	case rpc.CmdRawBatchDelete:
		cmdResp := resp.RawBatchDelete
		if cmdResp == nil {
			batchResp.err = errors.WithStack(ErrBodyMissing)
			return batchResp
		}
		if cmdResp.GetError() != "" {
			batchResp.err = errors.New(cmdResp.GetError())
			return batchResp
		}
		batchResp.resp = resp
	}
	return batchResp
}

// sendDeleteRangeReq sends a raw delete range request and returns the response and the actual endKey.
// If the given range spans over more than one regions, the actual endKey is the end of the first region.
// We can't use sendReq directly, because we need to know the end of the region before we send the request
// TODO: Is there any better way to avoid duplicating code with func `sendReq` ?
func (c *RawKVClient) sendDeleteRangeReq(startKey []byte, endKey []byte) (*rpc.Response, []byte, error) {
	bo := retry.NewBackoffer(context.Background(), retry.RawkvMaxBackoff)
	sender := rpc.NewRegionRequestSender(c.regionCache, c.rpcClient)
	for {
		loc, err := c.regionCache.LocateKey(bo, startKey)
		if err != nil {
			return nil, nil, errors.WithStack(err)
		}

		actualEndKey := endKey
		if len(loc.EndKey) > 0 && bytes.Compare(loc.EndKey, endKey) < 0 {
			actualEndKey = loc.EndKey
		}

		req := &rpc.Request{
			Type: rpc.CmdRawDeleteRange,
			RawDeleteRange: &kvrpcpb.RawDeleteRangeRequest{
				StartKey: startKey,
				EndKey:   actualEndKey,
			},
		}

		resp, err := sender.SendReq(bo, req, loc.Region, rpc.ReadTimeoutShort)
		if err != nil {
			return nil, nil, errors.WithStack(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return nil, nil, errors.WithStack(err)
		}
		if regionErr != nil {
			err := bo.Backoff(retry.BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return nil, nil, errors.WithStack(err)
			}
			continue
		}
		return resp, actualEndKey, nil
	}
}

func (c *RawKVClient) sendBatchPut(bo *retry.Backoffer, keys, values [][]byte) error {
	keyToValue := make(map[string][]byte)
	for i, key := range keys {
		keyToValue[string(key)] = values[i]
	}
	groups, _, err := c.regionCache.GroupKeysByRegion(bo, keys)
	if err != nil {
		return errors.WithStack(err)
	}
	var batches []batch
	// split the keys by size and RegionVerID
	for regionID, groupKeys := range groups {
		batches = appendBatches(batches, regionID, groupKeys, keyToValue, rawBatchPutSize)
	}
	bo, cancel := bo.Fork()
	ch := make(chan error, len(batches))
	for _, batch := range batches {
		batch1 := batch
		go func() {
			singleBatchBackoffer, singleBatchCancel := bo.Fork()
			defer singleBatchCancel()
			ch <- c.doBatchPut(singleBatchBackoffer, batch1)
		}()
	}

	for i := 0; i < len(batches); i++ {
		if e := <-ch; e != nil {
			cancel()
			// catch the first error
			if err == nil {
				err = e
			}
		}
	}
	return errors.WithStack(err)
}

func appendKeyBatches(batches []batch, regionID locate.RegionVerID, groupKeys [][]byte, limit int) []batch {
	var keys [][]byte
	for start, count := 0, 0; start < len(groupKeys); start++ {
		if count > limit {
			batches = append(batches, batch{regionID: regionID, keys: keys})
			keys = make([][]byte, 0, limit)
			count = 0
		}
		keys = append(keys, groupKeys[start])
		count++
	}
	if len(keys) != 0 {
		batches = append(batches, batch{regionID: regionID, keys: keys})
	}
	return batches
}

func appendBatches(batches []batch, regionID locate.RegionVerID, groupKeys [][]byte, keyToValue map[string][]byte, limit int) []batch {
	var start, size int
	var keys, values [][]byte
	for start = 0; start < len(groupKeys); start++ {
		if size >= limit {
			batches = append(batches, batch{regionID: regionID, keys: keys, values: values})
			keys = make([][]byte, 0)
			values = make([][]byte, 0)
			size = 0
		}
		key := groupKeys[start]
		value := keyToValue[string(key)]
		keys = append(keys, key)
		values = append(values, value)
		size += len(key)
		size += len(value)
	}
	if len(keys) != 0 {
		batches = append(batches, batch{regionID: regionID, keys: keys, values: values})
	}
	return batches
}

func (c *RawKVClient) doBatchPut(bo *retry.Backoffer, batch batch) error {
	kvPair := make([]*kvrpcpb.KvPair, 0, len(batch.keys))
	for i, key := range batch.keys {
		kvPair = append(kvPair, &kvrpcpb.KvPair{Key: key, Value: batch.values[i]})
	}

	req := &rpc.Request{
		Type: rpc.CmdRawBatchPut,
		RawBatchPut: &kvrpcpb.RawBatchPutRequest{
			Pairs: kvPair,
		},
	}

	sender := rpc.NewRegionRequestSender(c.regionCache, c.rpcClient)
	resp, err := sender.SendReq(bo, req, batch.regionID, rpc.ReadTimeoutShort)
	if err != nil {
		return errors.WithStack(err)
	}
	regionErr, err := resp.GetRegionError()
	if err != nil {
		return errors.WithStack(err)
	}
	if regionErr != nil {
		err := bo.Backoff(retry.BoRegionMiss, errors.New(regionErr.String()))
		if err != nil {
			return errors.WithStack(err)
		}
		// recursive call
		return c.sendBatchPut(bo, batch.keys, batch.values)
	}

	cmdResp := resp.RawBatchPut
	if cmdResp == nil {
		return errors.WithStack(ErrBodyMissing)
	}
	if cmdResp.GetError() != "" {
		return errors.New(cmdResp.GetError())
	}
	return nil
}

type batch struct {
	regionID locate.RegionVerID
	keys     [][]byte
	values   [][]byte
}

type singleBatchResp struct {
	resp *rpc.Response
	err  error
}
