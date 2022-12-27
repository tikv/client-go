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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/rawkv.go
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

package rawkv

import (
	"bytes"
	"context"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/config"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/client"
	"github.com/tikv/client-go/v2/internal/kvrpc"
	"github.com/tikv/client-go/v2/internal/locate"
	"github.com/tikv/client-go/v2/internal/retry"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	pd "github.com/tikv/pd/client"
	"google.golang.org/grpc"
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

type rawOptions struct {
	// ColumnFamily filed is used for manipulate kv in specified column family
	ColumnFamily string

	// This field is used for Scan()/ReverseScan().
	KeyOnly bool
}

// RawChecksum represents the checksum result of raw kv pairs in TiKV cluster.
type RawChecksum struct {
	// Crc64Xor is the checksum result with crc64 algorithm
	Crc64Xor uint64
	// TotalKvs is the total number of kvpairs
	TotalKvs uint64
	// TotalBytes is the total bytes of kvpairs, including prefix in APIV2
	TotalBytes uint64
}

// RawOption represents possible options that can be cotrolled by the user
// to tweak the API behavior.
//
// Available options are:
// - ScanColumnFamily
// - ScanKeyOnly
type RawOption interface {
	apply(opts *rawOptions)
}

type rawOptionFunc func(opts *rawOptions)

func (f rawOptionFunc) apply(opts *rawOptions) {
	f(opts)
}

// SetColumnFamily is a RawkvOption to only manipulate the k-v in specified column family
func SetColumnFamily(cf string) RawOption {
	return rawOptionFunc(func(opts *rawOptions) {
		opts.ColumnFamily = cf
	})
}

// ScanKeyOnly is a rawkvOptions that tells the scanner to only returns
// keys and omit the values.
// It can work only in API scan().
func ScanKeyOnly() RawOption {
	return rawOptionFunc(func(opts *rawOptions) {
		opts.KeyOnly = true
	})
}

// Client is a client of TiKV server which is used as a key-value storage,
// only GET/PUT/DELETE commands are supported.
type Client struct {
	apiVersion  kvrpcpb.APIVersion
	clusterID   uint64
	regionCache *locate.RegionCache
	pdClient    pd.Client
	rpcClient   client.Client
	cf          string
	atomic      bool
}

type option struct {
	apiVersion      kvrpcpb.APIVersion
	security        config.Security
	gRPCDialOptions []grpc.DialOption
	pdOptions       []pd.ClientOption
	keyspace        string
}

// ClientOpt is factory to set the client options.
type ClientOpt func(*option)

// WithPDOptions is used to set the pd.ClientOption
func WithPDOptions(opts ...pd.ClientOption) ClientOpt {
	return func(o *option) {
		o.pdOptions = append(o.pdOptions, opts...)
	}
}

// WithSecurity is used to set the config.Security
func WithSecurity(security config.Security) ClientOpt {
	return func(o *option) {
		o.security = security
	}
}

// WithGRPCDialOptions is used to set the grpc.DialOption.
func WithGRPCDialOptions(opts ...grpc.DialOption) ClientOpt {
	return func(o *option) {
		o.gRPCDialOptions = append(o.gRPCDialOptions, opts...)
	}
}

// WithAPIVersion is used to set the api version.
func WithAPIVersion(apiVersion kvrpcpb.APIVersion) ClientOpt {
	return func(o *option) {
		o.apiVersion = apiVersion
	}
}

// WithKeyspace is used to set the keyspace Name.
func WithKeyspace(name string) ClientOpt {
	return func(o *option) {
		o.keyspace = name
	}
}

// SetAtomicForCAS sets atomic mode for CompareAndSwap
func (c *Client) SetAtomicForCAS(b bool) *Client {
	c.atomic = b
	return c
}

// SetColumnFamily sets columnFamily for client
func (c *Client) SetColumnFamily(columnFamily string) *Client {
	c.cf = columnFamily
	return c
}

// NewClient creates a client with PD cluster addrs.
func NewClient(ctx context.Context, pdAddrs []string, security config.Security, opts ...pd.ClientOption) (*Client, error) {
	return NewClientWithOpts(ctx, pdAddrs, WithSecurity(security), WithPDOptions(opts...))
}

// NewClientWithOpts creates a client with PD cluster addrs and client options.
func NewClientWithOpts(ctx context.Context, pdAddrs []string, opts ...ClientOpt) (*Client, error) {
	opt := &option{}
	for _, o := range opts {
		o(opt)
	}

	// Use an unwrapped PDClient to obtain keyspace meta.
	pdCli, err := pd.NewClient(pdAddrs, pd.SecurityOption{
		CAPath:   opt.security.ClusterSSLCA,
		CertPath: opt.security.ClusterSSLCert,
		KeyPath:  opt.security.ClusterSSLKey,
	}, opt.pdOptions...)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// Build a CodecPDClient
	var codecCli *tikv.CodecPDClient

	switch opt.apiVersion {
	case kvrpcpb.APIVersion_V1, kvrpcpb.APIVersion_V1TTL:
		codecCli = locate.NewCodecPDClient(tikv.ModeRaw, pdCli)
	case kvrpcpb.APIVersion_V2:
		codecCli, err = tikv.NewCodecPDClientWithKeyspace(tikv.ModeRaw, pdCli, opt.keyspace)
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.Errorf("unknown api version: %d", opt.apiVersion)
	}

	pdCli = codecCli

	rpcCli := client.NewRPCClient(
		client.WithSecurity(opt.security),
		client.WithGRPCDialOptions(opt.gRPCDialOptions...),
		client.WithCodec(codecCli.GetCodec()),
	)

	return &Client{
		apiVersion:  opt.apiVersion,
		clusterID:   pdCli.GetClusterID(ctx),
		regionCache: locate.NewRegionCache(pdCli),
		pdClient:    pdCli,
		rpcClient:   rpcCli,
	}, nil
}

// Close closes the client.
func (c *Client) Close() error {
	if c.pdClient != nil {
		c.pdClient.Close()
	}
	if c.regionCache != nil {
		c.regionCache.Close()
	}
	if c.rpcClient == nil {
		return nil
	}
	return c.rpcClient.Close()
}

// ClusterID returns the TiKV cluster ID.
func (c *Client) ClusterID() uint64 {
	return c.clusterID
}

// Get queries value with the key. When the key does not exist, it returns `nil, nil`.
func (c *Client) Get(ctx context.Context, key []byte, options ...RawOption) ([]byte, error) {
	start := time.Now()
	defer func() { metrics.RawkvCmdHistogramWithGet.Observe(time.Since(start).Seconds()) }()

	opts := c.getRawKVOptions(options...)
	req := tikvrpc.NewRequest(
		tikvrpc.CmdRawGet,
		&kvrpcpb.RawGetRequest{
			Key: key,
			Cf:  c.getColumnFamily(opts),
		})
	resp, _, err := c.sendReq(ctx, key, req, false)
	if err != nil {
		return nil, err
	}
	if resp.Resp == nil {
		return nil, errors.WithStack(tikverr.ErrBodyMissing)
	}
	cmdResp := resp.Resp.(*kvrpcpb.RawGetResponse)
	if cmdResp.GetError() != "" {
		return nil, errors.New(cmdResp.GetError())
	}
	if cmdResp.NotFound {
		return nil, nil
	}
	return convertNilToEmptySlice(cmdResp.Value), nil
}

const rawkvMaxBackoff = 20000

// BatchGet queries values with the keys.
func (c *Client) BatchGet(ctx context.Context, keys [][]byte, options ...RawOption) ([][]byte, error) {
	start := time.Now()
	defer func() {
		metrics.RawkvCmdHistogramWithBatchGet.Observe(time.Since(start).Seconds())
	}()

	opts := c.getRawKVOptions(options...)
	bo := retry.NewBackofferWithVars(ctx, rawkvMaxBackoff, nil)
	resp, err := c.sendBatchReq(bo, keys, opts, tikvrpc.CmdRawBatchGet)
	if err != nil {
		return nil, err
	}

	if resp.Resp == nil {
		return nil, errors.WithStack(tikverr.ErrBodyMissing)
	}
	cmdResp := resp.Resp.(*kvrpcpb.RawBatchGetResponse)

	keyToValue := make(map[string][]byte, len(keys))
	for _, pair := range cmdResp.Pairs {
		keyToValue[string(pair.Key)] = pair.Value
	}

	values := make([][]byte, len(keys))
	for i, key := range keys {
		v, ok := keyToValue[string(key)]
		if ok {
			v = convertNilToEmptySlice(v)
		}
		values[i] = v
	}
	return values, nil
}

// PutWithTTL stores a key-value pair to TiKV with a time-to-live duration.
func (c *Client) PutWithTTL(ctx context.Context, key, value []byte, ttl uint64, options ...RawOption) error {
	start := time.Now()
	defer func() { metrics.RawkvCmdHistogramWithBatchPut.Observe(time.Since(start).Seconds()) }()
	metrics.RawkvSizeHistogramWithKey.Observe(float64(len(key)))
	metrics.RawkvSizeHistogramWithValue.Observe(float64(len(value)))

	opts := c.getRawKVOptions(options...)
	req := tikvrpc.NewRequest(tikvrpc.CmdRawPut, &kvrpcpb.RawPutRequest{
		Key:    key,
		Value:  value,
		Ttl:    ttl,
		Cf:     c.getColumnFamily(opts),
		ForCas: c.atomic,
	})
	resp, _, err := c.sendReq(ctx, key, req, false)
	if err != nil {
		return err
	}
	if resp.Resp == nil {
		return errors.WithStack(tikverr.ErrBodyMissing)
	}
	cmdResp := resp.Resp.(*kvrpcpb.RawPutResponse)
	if cmdResp.GetError() != "" {
		return errors.New(cmdResp.GetError())
	}
	return nil
}

// GetKeyTTL get the TTL of a raw key from TiKV if key exists
func (c *Client) GetKeyTTL(ctx context.Context, key []byte, options ...RawOption) (*uint64, error) {
	var ttl uint64
	metrics.RawkvSizeHistogramWithKey.Observe(float64(len(key)))

	opts := c.getRawKVOptions(options...)
	req := tikvrpc.NewRequest(tikvrpc.CmdGetKeyTTL, &kvrpcpb.RawGetKeyTTLRequest{
		Key: key,
		Cf:  c.getColumnFamily(opts),
	})
	resp, _, err := c.sendReq(ctx, key, req, false)

	if err != nil {
		return nil, err
	}
	if resp.Resp == nil {
		return nil, errors.WithStack(tikverr.ErrBodyMissing)
	}

	cmdResp := resp.Resp.(*kvrpcpb.RawGetKeyTTLResponse)
	if cmdResp.GetError() != "" {
		return nil, errors.New(cmdResp.GetError())
	}

	if cmdResp.GetNotFound() {
		return nil, nil
	}

	ttl = cmdResp.GetTtl()
	return &ttl, nil
}

// GetPDClient returns the PD client.
func (c *Client) GetPDClient() pd.Client {
	return c.pdClient
}

// Put stores a key-value pair to TiKV.
func (c *Client) Put(ctx context.Context, key, value []byte, options ...RawOption) error {
	return c.PutWithTTL(ctx, key, value, 0, options...)
}

// BatchPut stores key-value pairs to TiKV.
func (c *Client) BatchPut(ctx context.Context, keys, values [][]byte, options ...RawOption) error {
	return c.BatchPutWithTTL(ctx, keys, values, nil, options...)
}

// BatchPutWithTTL stores key-values pairs to TiKV with time-to-live durations.
func (c *Client) BatchPutWithTTL(ctx context.Context, keys, values [][]byte, ttls []uint64, options ...RawOption) error {
	start := time.Now()
	defer func() {
		metrics.RawkvCmdHistogramWithBatchPut.Observe(time.Since(start).Seconds())
	}()

	if len(keys) != len(values) {
		return errors.New("the len of keys is not equal to the len of values")
	}
	if len(ttls) > 0 && len(keys) != len(ttls) {
		return errors.New("the len of ttls is not equal to the len of values")
	}
	bo := retry.NewBackofferWithVars(ctx, rawkvMaxBackoff, nil)
	opts := c.getRawKVOptions(options...)
	err := c.sendBatchPut(bo, keys, values, ttls, opts)
	return err
}

// Delete deletes a key-value pair from TiKV.
func (c *Client) Delete(ctx context.Context, key []byte, options ...RawOption) error {
	start := time.Now()
	defer func() { metrics.RawkvCmdHistogramWithDelete.Observe(time.Since(start).Seconds()) }()

	opts := c.getRawKVOptions(options...)
	req := tikvrpc.NewRequest(tikvrpc.CmdRawDelete, &kvrpcpb.RawDeleteRequest{
		Key:    key,
		Cf:     c.getColumnFamily(opts),
		ForCas: c.atomic,
	})
	req.MaxExecutionDurationMs = uint64(client.MaxWriteExecutionTime.Milliseconds())
	resp, _, err := c.sendReq(ctx, key, req, false)
	if err != nil {
		return err
	}
	if resp.Resp == nil {
		return errors.WithStack(tikverr.ErrBodyMissing)
	}
	cmdResp := resp.Resp.(*kvrpcpb.RawDeleteResponse)
	if cmdResp.GetError() != "" {
		return errors.New(cmdResp.GetError())
	}
	return nil
}

// BatchDelete deletes key-value pairs from TiKV.
func (c *Client) BatchDelete(ctx context.Context, keys [][]byte, options ...RawOption) error {
	start := time.Now()
	defer func() {
		metrics.RawkvCmdHistogramWithBatchDelete.Observe(time.Since(start).Seconds())
	}()

	bo := retry.NewBackofferWithVars(ctx, rawkvMaxBackoff, nil)
	opts := c.getRawKVOptions(options...)
	resp, err := c.sendBatchReq(bo, keys, opts, tikvrpc.CmdRawBatchDelete)
	if err != nil {
		return err
	}
	if resp.Resp == nil {
		return errors.WithStack(tikverr.ErrBodyMissing)
	}
	cmdResp := resp.Resp.(*kvrpcpb.RawBatchDeleteResponse)
	if cmdResp.GetError() != "" {
		return errors.New(cmdResp.GetError())
	}
	return nil
}

// DeleteRange deletes all key-value pairs in the [startKey, endKey) range from TiKV.
func (c *Client) DeleteRange(ctx context.Context, startKey []byte, endKey []byte, options ...RawOption) error {
	start := time.Now()
	var err error
	defer func() {
		var label = "delete_range"
		if err != nil {
			label += "_error"
		}
		metrics.TiKVRawkvCmdHistogram.WithLabelValues(label).Observe(time.Since(start).Seconds())
	}()

	// Process each affected region respectively
	for !bytes.Equal(startKey, endKey) {
		opts := c.getRawKVOptions(options...)
		var resp *tikvrpc.Response
		var actualEndKey []byte
		resp, actualEndKey, err = c.sendDeleteRangeReq(ctx, startKey, endKey, opts)
		if err != nil {
			return err
		}
		if resp.Resp == nil {
			return errors.WithStack(tikverr.ErrBodyMissing)
		}
		cmdResp := resp.Resp.(*kvrpcpb.RawDeleteRangeResponse)
		if cmdResp.GetError() != "" {
			return errors.New(cmdResp.GetError())
		}
		startKey = actualEndKey
	}

	return nil
}

// Scan queries continuous kv pairs in range [startKey, endKey), up to limit pairs.
// The returned keys are in lexicographical order.
// If endKey is empty, it means unbounded.
// If you want to exclude the startKey or include the endKey, push a '\0' to the key. For example, to scan
// (startKey, endKey], you can write:
// `Scan(ctx, push(startKey, '\0'), push(endKey, '\0'), limit)`.
func (c *Client) Scan(ctx context.Context, startKey, endKey []byte, limit int, options ...RawOption,
) (keys [][]byte, values [][]byte, err error) {
	start := time.Now()
	defer func() { metrics.RawkvCmdHistogramWithRawScan.Observe(time.Since(start).Seconds()) }()

	if limit > MaxRawKVScanLimit {
		return nil, nil, errors.WithStack(ErrMaxScanLimitExceeded)
	}

	opts := c.getRawKVOptions(options...)

	for len(keys) < limit && (len(endKey) == 0 || bytes.Compare(startKey, endKey) < 0) {
		req := tikvrpc.NewRequest(tikvrpc.CmdRawScan, &kvrpcpb.RawScanRequest{
			StartKey: startKey,
			EndKey:   endKey,
			Limit:    uint32(limit - len(keys)),
			KeyOnly:  opts.KeyOnly,
			Cf:       c.getColumnFamily(opts),
		})
		resp, loc, err := c.sendReq(ctx, startKey, req, false)
		if err != nil {
			return nil, nil, err
		}
		if resp.Resp == nil {
			return nil, nil, errors.WithStack(tikverr.ErrBodyMissing)
		}
		cmdResp := resp.Resp.(*kvrpcpb.RawScanResponse)
		for _, pair := range cmdResp.Kvs {
			keys = append(keys, pair.Key)
			values = append(values, convertNilToEmptySlice(pair.Value))
		}
		startKey = loc.EndKey
		if len(startKey) == 0 {
			break
		}
	}
	return
}

// ReverseScan queries continuous kv pairs in range [endKey, startKey), up to limit pairs.
// The returned keys are in reversed lexicographical order.
// If endKey is empty, it means unbounded.
// If you want to include the startKey or exclude the endKey, push a '\0' to the key. For example, to scan
// (endKey, startKey], you can write:
// `ReverseScan(ctx, push(startKey, '\0'), push(endKey, '\0'), limit)`.
// It doesn't support Scanning from "", because locating the last Region is not yet implemented.
func (c *Client) ReverseScan(ctx context.Context, startKey, endKey []byte, limit int, options ...RawOption) (keys [][]byte, values [][]byte, err error) {
	start := time.Now()
	defer func() {
		metrics.RawkvCmdHistogramWithRawReversScan.Observe(time.Since(start).Seconds())
	}()

	if limit > MaxRawKVScanLimit {
		return nil, nil, errors.WithStack(ErrMaxScanLimitExceeded)
	}

	opts := c.getRawKVOptions(options...)

	for len(keys) < limit && bytes.Compare(startKey, endKey) > 0 {
		req := tikvrpc.NewRequest(tikvrpc.CmdRawScan, &kvrpcpb.RawScanRequest{
			StartKey: startKey,
			EndKey:   endKey,
			Limit:    uint32(limit - len(keys)),
			Reverse:  true,
			KeyOnly:  opts.KeyOnly,
			Cf:       c.getColumnFamily(opts),
		})
		resp, loc, err := c.sendReq(ctx, startKey, req, true)
		if err != nil {
			return nil, nil, err
		}
		if resp.Resp == nil {
			return nil, nil, errors.WithStack(tikverr.ErrBodyMissing)
		}
		cmdResp := resp.Resp.(*kvrpcpb.RawScanResponse)
		for _, pair := range cmdResp.Kvs {
			keys = append(keys, pair.Key)
			values = append(values, convertNilToEmptySlice(pair.Value))
		}
		startKey = loc.StartKey
		if len(startKey) == 0 {
			break
		}
	}
	return
}

// Checksum do checksum of continuous kv pairs in range [startKey, endKey).
// If endKey is empty, it means unbounded.
// If you want to exclude the startKey or include the endKey, push a '\0' to the key. For example, to scan
// (startKey, endKey], you can write:
// `Checksum(ctx, push(startKey, '\0'), push(endKey, '\0'))`.
func (c *Client) Checksum(ctx context.Context, startKey, endKey []byte, options ...RawOption,
) (check RawChecksum, err error) {

	start := time.Now()
	defer func() { metrics.RawkvCmdHistogramWithRawChecksum.Observe(time.Since(start).Seconds()) }()

	for len(endKey) == 0 || bytes.Compare(startKey, endKey) < 0 {
		req := tikvrpc.NewRequest(tikvrpc.CmdRawChecksum, &kvrpcpb.RawChecksumRequest{
			Algorithm: kvrpcpb.ChecksumAlgorithm_Crc64_Xor,
			Ranges: []*kvrpcpb.KeyRange{{
				StartKey: startKey,
				EndKey:   endKey,
			}},
		})
		resp, loc, err := c.sendReq(ctx, startKey, req, false)
		if err != nil {
			return RawChecksum{0, 0, 0}, err
		}
		if resp.Resp == nil {
			return RawChecksum{0, 0, 0}, errors.WithStack(tikverr.ErrBodyMissing)
		}
		cmdResp := resp.Resp.(*kvrpcpb.RawChecksumResponse)
		check.Crc64Xor ^= cmdResp.GetChecksum()
		check.TotalKvs += cmdResp.GetTotalKvs()
		check.TotalBytes += cmdResp.GetTotalBytes()
		startKey = loc.EndKey
		if len(startKey) == 0 {
			break
		}
	}
	return
}

// CompareAndSwap results in an atomic compare-and-set operation for the given key while SetAtomicForCAS(true)
// If the value retrieved is equal to previousValue, newValue is written.
// It returns the previous value and whether the value is successfully swapped.
//
// If SetAtomicForCAS(false), it will returns an error because
// CAS operations enforce the client should operate in atomic mode.
//
// NOTE: This feature is experimental. It depends on the single-row transaction mechanism of TiKV which is conflict
// with the normal write operation in rawkv mode. If multiple clients exist, it's up to the clients the sync the atomic mode flag.
// If some clients write in atomic mode but the other don't, the linearizability of TiKV will be violated.
func (c *Client) CompareAndSwap(ctx context.Context, key, previousValue, newValue []byte, options ...RawOption) ([]byte, bool, error) {
	if !c.atomic {
		return nil, false, errors.New("using CompareAndSwap without enable atomic mode")
	}

	opts := c.getRawKVOptions(options...)
	reqArgs := kvrpcpb.RawCASRequest{
		Key:   key,
		Value: newValue,
		Cf:    c.getColumnFamily(opts),
	}
	if previousValue == nil {
		reqArgs.PreviousNotExist = true
	} else {
		reqArgs.PreviousValue = previousValue
	}

	req := tikvrpc.NewRequest(tikvrpc.CmdRawCompareAndSwap, &reqArgs)
	req.MaxExecutionDurationMs = uint64(client.MaxWriteExecutionTime.Milliseconds())
	resp, _, err := c.sendReq(ctx, key, req, false)
	if err != nil {
		return nil, false, err
	}
	if resp.Resp == nil {
		return nil, false, errors.WithStack(tikverr.ErrBodyMissing)
	}

	cmdResp := resp.Resp.(*kvrpcpb.RawCASResponse)
	if cmdResp.GetError() != "" {
		return nil, false, errors.New(cmdResp.GetError())
	}

	if cmdResp.PreviousNotExist {
		return nil, cmdResp.Succeed, nil
	}
	return convertNilToEmptySlice(cmdResp.PreviousValue), cmdResp.Succeed, nil
}

func (c *Client) sendReq(ctx context.Context, key []byte, req *tikvrpc.Request, reverse bool) (*tikvrpc.Response, *locate.KeyLocation, error) {
	bo := retry.NewBackofferWithVars(ctx, rawkvMaxBackoff, nil)
	sender := locate.NewRegionRequestSender(c.regionCache, c.rpcClient)
	for {
		var loc *locate.KeyLocation
		var err error
		if reverse {
			loc, err = c.regionCache.LocateEndKey(bo, key)
		} else {
			loc, err = c.regionCache.LocateKey(bo, key)
		}
		if err != nil {
			return nil, nil, err
		}
		resp, err := sender.SendReq(bo, req, loc.Region, client.ReadTimeoutShort)
		if err != nil {
			return nil, nil, err
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return nil, nil, err
		}
		if regionErr != nil {
			err := bo.Backoff(retry.BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return nil, nil, err
			}
			continue
		}
		return resp, loc, nil
	}
}

func (c *Client) sendBatchReq(bo *retry.Backoffer, keys [][]byte, options *rawOptions, cmdType tikvrpc.CmdType) (*tikvrpc.Response, error) { // split the keys
	groups, _, err := c.regionCache.GroupKeysByRegion(bo, keys, nil)
	if err != nil {
		return nil, err
	}

	var batches []kvrpc.Batch
	for regionID, groupKeys := range groups {
		batches = kvrpc.AppendKeyBatches(batches, regionID, groupKeys, rawBatchPairCount)
	}
	bo, cancel := bo.Fork()
	ches := make(chan kvrpc.BatchResult, len(batches))
	for _, batch := range batches {
		batch1 := batch
		go func() {
			singleBatchBackoffer, singleBatchCancel := bo.Fork()
			defer singleBatchCancel()
			ches <- c.doBatchReq(singleBatchBackoffer, batch1, options, cmdType)
		}()
	}

	var firstError error
	var resp *tikvrpc.Response
	switch cmdType {
	case tikvrpc.CmdRawBatchGet:
		resp = &tikvrpc.Response{Resp: &kvrpcpb.RawBatchGetResponse{}}
	case tikvrpc.CmdRawBatchDelete:
		resp = &tikvrpc.Response{Resp: &kvrpcpb.RawBatchDeleteResponse{}}
	}
	for i := 0; i < len(batches); i++ {
		singleResp, ok := <-ches
		if ok {
			if singleResp.Error != nil {
				cancel()
				if firstError == nil {
					firstError = errors.WithStack(singleResp.Error)
				}
			} else if cmdType == tikvrpc.CmdRawBatchGet {
				cmdResp := singleResp.Resp.(*kvrpcpb.RawBatchGetResponse)
				resp.Resp.(*kvrpcpb.RawBatchGetResponse).Pairs = append(resp.Resp.(*kvrpcpb.RawBatchGetResponse).Pairs, cmdResp.Pairs...)
			}
		}
	}

	return resp, firstError
}

func (c *Client) doBatchReq(bo *retry.Backoffer, batch kvrpc.Batch, options *rawOptions, cmdType tikvrpc.CmdType) kvrpc.BatchResult {
	var req *tikvrpc.Request
	switch cmdType {
	case tikvrpc.CmdRawBatchGet:
		req = tikvrpc.NewRequest(cmdType, &kvrpcpb.RawBatchGetRequest{
			Keys: batch.Keys,
			Cf:   c.getColumnFamily(options),
		})
	case tikvrpc.CmdRawBatchDelete:
		req = tikvrpc.NewRequest(cmdType, &kvrpcpb.RawBatchDeleteRequest{
			Keys:   batch.Keys,
			Cf:     c.getColumnFamily(options),
			ForCas: c.atomic,
		})
	}

	sender := locate.NewRegionRequestSender(c.regionCache, c.rpcClient)
	req.MaxExecutionDurationMs = uint64(client.MaxWriteExecutionTime.Milliseconds())
	resp, err := sender.SendReq(bo, req, batch.RegionID, client.ReadTimeoutShort)

	batchResp := kvrpc.BatchResult{}
	if err != nil {
		batchResp.Error = err
		return batchResp
	}
	regionErr, err := resp.GetRegionError()
	if err != nil {
		batchResp.Error = err
		return batchResp
	}
	if regionErr != nil {
		err := bo.Backoff(retry.BoRegionMiss, errors.New(regionErr.String()))
		if err != nil {
			batchResp.Error = err
			return batchResp
		}
		resp, err = c.sendBatchReq(bo, batch.Keys, options, cmdType)
		batchResp.Response = resp
		batchResp.Error = err
		return batchResp
	}

	switch cmdType {
	case tikvrpc.CmdRawBatchGet:
		batchResp.Response = resp
	case tikvrpc.CmdRawBatchDelete:
		if resp.Resp == nil {
			batchResp.Error = errors.WithStack(tikverr.ErrBodyMissing)
			return batchResp
		}
		cmdResp := resp.Resp.(*kvrpcpb.RawBatchDeleteResponse)
		if cmdResp.GetError() != "" {
			batchResp.Error = errors.New(cmdResp.GetError())
			return batchResp
		}
		batchResp.Response = resp
	}
	return batchResp
}

// sendDeleteRangeReq sends a raw delete range request and returns the response and the actual endKey.
// If the given range spans over more than one regions, the actual endKey is the end of the first region.
// We can't use sendReq directly, because we need to know the end of the region before we send the request
// TODO: Is there any better way to avoid duplicating code with func `sendReq` ?
func (c *Client) sendDeleteRangeReq(ctx context.Context, startKey []byte, endKey []byte, opts *rawOptions) (*tikvrpc.Response, []byte, error) {
	bo := retry.NewBackofferWithVars(ctx, rawkvMaxBackoff, nil)
	sender := locate.NewRegionRequestSender(c.regionCache, c.rpcClient)
	for {
		loc, err := c.regionCache.LocateKey(bo, startKey)
		if err != nil {
			return nil, nil, err
		}

		actualEndKey := endKey
		if len(loc.EndKey) > 0 && bytes.Compare(loc.EndKey, endKey) < 0 {
			actualEndKey = loc.EndKey
		}

		req := tikvrpc.NewRequest(tikvrpc.CmdRawDeleteRange, &kvrpcpb.RawDeleteRangeRequest{
			StartKey: startKey,
			EndKey:   actualEndKey,
			Cf:       c.getColumnFamily(opts),
		})

		req.MaxExecutionDurationMs = uint64(client.MaxWriteExecutionTime.Milliseconds())
		resp, err := sender.SendReq(bo, req, loc.Region, client.ReadTimeoutShort)
		if err != nil {
			return nil, nil, err
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return nil, nil, err
		}
		if regionErr != nil {
			err := bo.Backoff(retry.BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return nil, nil, err
			}
			continue
		}
		return resp, actualEndKey, nil
	}
}

func (c *Client) sendBatchPut(bo *retry.Backoffer, keys, values [][]byte, ttls []uint64, opts *rawOptions) error {
	keyToValue := make(map[string][]byte, len(keys))
	keyToTTL := make(map[string]uint64, len(keys))
	for i, key := range keys {
		keyToValue[string(key)] = values[i]
		if len(ttls) > 0 {
			keyToTTL[string(key)] = ttls[i]
		}
	}
	groups, _, err := c.regionCache.GroupKeysByRegion(bo, keys, nil)
	if err != nil {
		return err
	}
	var batches []kvrpc.Batch
	// split the keys by size and RegionVerID
	for regionID, groupKeys := range groups {
		batches = kvrpc.AppendBatches(batches, regionID, groupKeys, keyToValue, keyToTTL, rawBatchPutSize)
	}
	bo, cancel := bo.Fork()
	ch := make(chan error, len(batches))
	for _, batch := range batches {
		batch1 := batch
		go func() {
			singleBatchBackoffer, singleBatchCancel := bo.Fork()
			defer singleBatchCancel()
			ch <- c.doBatchPut(singleBatchBackoffer, batch1, opts)
		}()
	}

	for i := 0; i < len(batches); i++ {
		if e := <-ch; e != nil {
			cancel()
			// catch the first error
			if err == nil {
				err = errors.WithStack(e)
			}
		}
	}
	return err
}

func (c *Client) doBatchPut(bo *retry.Backoffer, batch kvrpc.Batch, opts *rawOptions) error {
	kvPair := make([]*kvrpcpb.KvPair, 0, len(batch.Keys))
	for i, key := range batch.Keys {
		kvPair = append(kvPair, &kvrpcpb.KvPair{Key: key, Value: batch.Values[i]})
	}

	var ttl uint64
	if len(batch.TTLs) > 0 {
		ttl = batch.TTLs[0]
	}
	req := tikvrpc.NewRequest(tikvrpc.CmdRawBatchPut,
		&kvrpcpb.RawBatchPutRequest{
			Pairs:  kvPair,
			Cf:     c.getColumnFamily(opts),
			ForCas: c.atomic,
			Ttls:   batch.TTLs,
			Ttl:    ttl,
		})

	sender := locate.NewRegionRequestSender(c.regionCache, c.rpcClient)
	req.MaxExecutionDurationMs = uint64(client.MaxWriteExecutionTime.Milliseconds())
	req.ApiVersion = c.apiVersion
	resp, err := sender.SendReq(bo, req, batch.RegionID, client.ReadTimeoutShort)
	if err != nil {
		return err
	}
	regionErr, err := resp.GetRegionError()
	if err != nil {
		return err
	}
	if regionErr != nil {
		err := bo.Backoff(retry.BoRegionMiss, errors.New(regionErr.String()))
		if err != nil {
			return err
		}
		// recursive call
		return c.sendBatchPut(bo, batch.Keys, batch.Values, batch.TTLs, opts)
	}

	if resp.Resp == nil {
		return errors.WithStack(tikverr.ErrBodyMissing)
	}
	cmdResp := resp.Resp.(*kvrpcpb.RawBatchPutResponse)
	if cmdResp.GetError() != "" {
		return errors.New(cmdResp.GetError())
	}
	return nil
}

func (c *Client) getColumnFamily(options *rawOptions) string {
	if options.ColumnFamily == "" {
		return c.cf
	}
	return options.ColumnFamily
}

func (c *Client) getRawKVOptions(options ...RawOption) *rawOptions {
	opts := rawOptions{}
	for _, op := range options {
		op.apply(&opts)
	}
	return &opts
}

// convertNilToEmptySlice is used to convert value of existed key return from TiKV.
// Convert nil to `[]byte{}` for indicating an empty value, and distinguishing from "not found",
// which is necessary when putting empty value is permitted.
// Also note that gRPC will always transfer empty byte slice as nil.
func convertNilToEmptySlice(value []byte) []byte {
	if value == nil {
		return []byte{}
	}
	return value
}
