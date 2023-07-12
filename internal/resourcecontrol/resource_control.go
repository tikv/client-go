// Copyright 2023 TiKV Authors
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

package resourcecontrol

import (
	"reflect"
	"strings"
	"time"

	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

// RequestInfo contains information about a request that is able to calculate the RU cost
// before the request is sent. Specifically, the write bytes RU cost of a write request
// could be calculated by its key size to write.
type RequestInfo struct {
	// writeBytes is the actual write size if the request is a write request,
	// or -1 if it's a read request.
	writeBytes    int64
	storeID       uint64
	replicaNumber int64
	// bypass indicates whether the request should be bypassed.
	// some internal request should be bypassed, such as Privilege request.
	bypass bool
}

// MakeRequestInfo extracts the relevant information from a BatchRequest.
func MakeRequestInfo(req *tikvrpc.Request) *RequestInfo {
	if !req.IsTxnWriteRequest() && !req.IsRawWriteRequest() {
		return &RequestInfo{writeBytes: -1}
	}

	var writeBytes int64
	var bypass bool
	switch r := req.Req.(type) {
	case *kvrpcpb.PrewriteRequest:
		for _, m := range r.Mutations {
			writeBytes += int64(len(m.Key)) + int64(len(m.Value))
		}
		writeBytes += int64(len(r.PrimaryLock))
		for _, l := range r.Secondaries {
			writeBytes += int64(len(l))
		}
	case *kvrpcpb.CommitRequest:
		for _, k := range r.Keys {
			writeBytes += int64(len(k))
		}
	}
	requestSource := req.Context.GetRequestSource()
	if len(requestSource) > 0 {
		if strings.Contains(requestSource, util.InternalRequest+"_"+util.InternalTxnOthers) {
			bypass = true
		}
	}
	return &RequestInfo{writeBytes: writeBytes, storeID: req.Context.Peer.StoreId, replicaNumber: req.ReplicaNumber, bypass: bypass}
}

// IsWrite returns whether the request is a write request.
func (req *RequestInfo) IsWrite() bool {
	return req.writeBytes > -1
}

// WriteBytes returns the actual write size of the request,
// -1 will be returned if it's not a write request.
func (req *RequestInfo) WriteBytes() uint64 {
	return uint64(req.writeBytes)
}

func (req *RequestInfo) ReplicaNumber() int64 {
	return req.replicaNumber
}

// Bypass returns whether the request should be bypassed.
func (req *RequestInfo) Bypass() bool {
	return req.bypass
}

func (req *RequestInfo) StoreID() uint64 {
	return req.storeID
}

// ResponseInfo contains information about a response that is able to calculate the RU cost
// after the response is received. Specifically, the read bytes RU cost of a read request
// could be calculated by its response size, and the KV CPU time RU cost of a request could
// be calculated by its execution details info.
type ResponseInfo struct {
	readBytes uint64
	kvCPU     time.Duration
}

// MakeResponseInfo extracts the relevant information from a BatchResponse.
func MakeResponseInfo(resp *tikvrpc.Response) *ResponseInfo {
	if resp.Resp == nil {
		return &ResponseInfo{}
	}
	// Parse the response to extract the info.
	var (
		readBytes uint64
		detailsV2 *kvrpcpb.ExecDetailsV2
		details   *kvrpcpb.ExecDetails
	)
	switch r := resp.Resp.(type) {
	case *coprocessor.Response:
		detailsV2 = r.GetExecDetailsV2()
		details = r.GetExecDetails()
		readBytes = uint64(r.Data.Size())
	case *tikvrpc.CopStreamResponse:
		// Streaming request returns `io.EOF``, so the first `CopStreamResponse.Response`` may be nil.
		if r.Response != nil {
			detailsV2 = r.Response.GetExecDetailsV2()
			details = r.Response.GetExecDetails()
		}
		readBytes = uint64(r.Data.Size())
	case *kvrpcpb.GetResponse:
		detailsV2 = r.GetExecDetailsV2()
	case *kvrpcpb.BatchGetResponse:
		detailsV2 = r.GetExecDetailsV2()
	case *kvrpcpb.ScanResponse:
		// TODO: using a more accurate size rather than using the whole response size as the read bytes.
		readBytes = uint64(r.Size())
	default:
		logutil.BgLogger().Debug("[kv resource] unknown response type to collect the info", zap.Any("type", reflect.TypeOf(r)))
		return &ResponseInfo{}
	}
	// Try to get read bytes from the `detailsV2`.
	// TODO: clarify whether we should count the underlying storage engine read bytes or not.
	if scanDetail := detailsV2.GetScanDetailV2(); scanDetail != nil {
		readBytes = scanDetail.GetProcessedVersionsSize()
	}
	// Get the KV CPU time in milliseconds from the execution time details.
	kvCPU := getKVCPU(detailsV2, details)
	return &ResponseInfo{readBytes: readBytes, kvCPU: kvCPU}
}

// TODO: find out a more accurate way to get the actual KV CPU time.
func getKVCPU(detailsV2 *kvrpcpb.ExecDetailsV2, details *kvrpcpb.ExecDetails) time.Duration {
	if timeDetail := detailsV2.GetTimeDetailV2(); timeDetail != nil {
		return time.Duration(timeDetail.GetProcessWallTimeNs())
	}
	if timeDetail := detailsV2.GetTimeDetail(); timeDetail != nil {
		return time.Duration(timeDetail.GetProcessWallTimeMs()) * time.Millisecond
	}
	if timeDetail := details.GetTimeDetail(); timeDetail != nil {
		return time.Duration(timeDetail.GetProcessWallTimeMs()) * time.Millisecond
	}
	return time.Duration(0)
}

// ReadBytes returns the read bytes of the response.
func (res *ResponseInfo) ReadBytes() uint64 {
	return res.readBytes
}

// KVCPU returns the KV CPU time of the response.
func (res *ResponseInfo) KVCPU() time.Duration {
	return res.kvCPU
}

// Succeed returns whether the KV request is successful.
// Todo: to fit https://github.com/tikv/pd/pull/5941
func (res *ResponseInfo) Succeed() bool {
	return true
}
