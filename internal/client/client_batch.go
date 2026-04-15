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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/client/client_batch.go
//

// Copyright 2019 PingCAP, Inc.
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

// Package client provides tcp connection to kvserver.
package client

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"runtime/trace"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/config/retry"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util"
	"github.com/tikv/client-go/v2/util/async"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/metadata"
)

// globalEncodedMsgDataPool is used to pool pre-encoded message data for batch commands.
var globalEncodedMsgDataPool = mem.NewTieredBufferPool(
	256, 4<<10, 16<<10, 32<<10, 1<<20, // copied from defaultBufferPoolSizes
)

type encodedBatchCmd struct {
	// implement isBatchCommandsRequest_Request_Cmd
	tikvpb.BatchCommandsRequest_Request_Empty
	// pre-encoded message data
	data *[]byte
}

func (p *encodedBatchCmd) MarshalTo(data []byte) (int, error) {
	if p.data == nil {
		return 0, errors.New("message data has been released")
	} else if len(data) < len(*p.data) {
		return 0, errors.New("no enough space to marshal message")
	}
	n := copy(data, *p.data)
	return n, nil
}

func (p *encodedBatchCmd) Size() int {
	if p.data == nil {
		return 0
	}
	return len(*p.data)
}

// encodeRequestCmd encodes the `req.Cmd` into a `encodedBatchCmd` and updates the `req.Cmd` to it in place.
//
// SAFETY: This function is called just after `tikvrpc.Request.ToBatchCommandsRequest()` and before constructing
// `batchCommandsEntry`. Note that the `req` argument is the output of `ToBatchCommandsRequest`, which is a new object
// of `tikvpb.BatchCommandsRequest_Request`. So `req.Cmd` can be only modified inside the batch client by
// `reuseRequestData` after the `req` has been sent. So there should be no data race on `req.Cmd` via public API
// (`SendRequest` and `SendRequestAsync`). When writing unit tests, please make sure it's only called once for one `req`
// (do NOT reuse `req`).
func encodeRequestCmd(req *tikvpb.BatchCommandsRequest_Request) error {
	if req.Cmd == nil {
		// req.Cmd might be nil in unit tests.
		return nil
	}
	if encoded, ok := req.Cmd.(*encodedBatchCmd); ok {
		if encoded.data == nil {
			return errors.New("request has already been encoded and sent")
		}
		return nil
	}
	data := globalEncodedMsgDataPool.Get(req.Cmd.Size())
	n, err := req.Cmd.MarshalTo(*data)
	if err != nil {
		globalEncodedMsgDataPool.Put(data)
		return errors.WithStack(err)
	} else if n != len(*data) {
		globalEncodedMsgDataPool.Put(data)
		return errors.Errorf("unexpected marshaled size: got %d, want %d", n, len(*data))
	}
	req.Cmd = &encodedBatchCmd{data: data}
	return nil
}

// reuseRequestData puts back all pre-encoded message data in the request to the `globalEncodedMsgDataPool`. The
// returned count is used for testing.
func reuseRequestData(req *tikvpb.BatchCommandsRequest) int {
	count := 0
	for _, r := range req.Requests {
		if cmd, ok := r.Cmd.(*encodedBatchCmd); ok && cmd.data != nil {
			globalEncodedMsgDataPool.Put(cmd.data)
			cmd.data = nil
			count++
		}
	}
	return count
}

type batchCommandsEntry struct {
	ctx context.Context
	req *tikvpb.BatchCommandsRequest_Request
	res chan *tikvpb.BatchCommandsResponse_Response
	cb  async.Callback[*tikvrpc.Response]
	// requestID is assigned in the send loop before the entry is published to batched.
	// Invariant: requestID > 0 means the entry has been assigned a concrete batch request ID.
	requestID atomic.Uint64
	// batchState is shared by all requests contained in the same BatchCommandsRequest.
	// It is published atomically because timeout/metrics paths may observe an entry
	// concurrently with the send loop assigning it a concrete send start.
	// Invariant: any entry published to batchCommandsClient.batched must already have a non-nil batchState.
	batchState atomic.Pointer[batchCommandsRequestState]
	// batchRequestMetrics is cached by store in connPool.metrics.
	batchRequestMetrics *batchRequestStageMetrics
	// forwardedHost is the address of a store which will handle the request.
	// It's different from the address the request sent to.
	forwardedHost string
	// canceled indicated the request is canceled or not.
	canceled int32
	err      error
	pri      uint64

	// reqArriveAt indicates when the batch commands entry is generated and sent to the batch conn channel.
	reqArriveAt time.Time

	// batchSelectedAfterReqArriveNS is published when the entry is assigned to a concrete batch.
	// It stays on the entry so timeout/metrics paths can observe batch selection before send()
	// publishes the shared batch state.
	batchSelectedAfterReqArriveNS atomic.Int64
	// recvAfterReqArriveNS is the elapsed time in nanoseconds from reqArriveAt until the response is received.
	recvAfterReqArriveNS atomic.Int64
}

type batchCommandsRequestState struct {
	// stream is the concrete batch stream selected for this batch request.
	stream *batchCommandsStream
	// batchSize is the number of requests in the batch.
	batchSize int
	// sendStartAt is when the client is ready to send the batch request, and matches ClientSendTimeNs.
	sendStartAt time.Time
	// sentAfterSendStartNS is the elapsed time in nanoseconds from sendStartAt until sending the batch request finishes.
	sentAfterSendStartNS atomic.Int64
	// firstRespAfterSendStartNS is the elapsed time in nanoseconds from sendStartAt until the first response of the batch arrives.
	firstRespAfterSendStartNS atomic.Int64
}

func (b *batchCommandsEntry) isCanceled() bool {
	return atomic.LoadInt32(&b.canceled) == 1
}

func (b *batchCommandsEntry) priority() uint64 {
	return b.pri
}

func (b *batchCommandsEntry) async() bool {
	return b.cb != nil
}

func (b *batchCommandsEntry) response(resp *tikvpb.BatchCommandsResponse_Response) {
	if b.async() {
		b.cb.Schedule(tikvrpc.FromBatchCommandsResponse(resp))
	} else {
		b.res <- resp
	}
}

func (b *batchCommandsEntry) error(err error) {
	b.err = err
	if b.async() {
		b.cb.Schedule(nil, err)
	} else {
		close(b.res)
	}
}

type batchRequestStage string

const (
	batchRequestStageBatchWait batchRequestStage = "batch_wait"
	batchRequestStageSendWait  batchRequestStage = "send_wait"
	batchRequestStageRecvWait  batchRequestStage = "recv_wait"
	batchRequestStageDone      batchRequestStage = "done"
)

type batchRequestOutcome string

const (
	batchRequestOutcomeOK       batchRequestOutcome = "ok"
	batchRequestOutcomeTimeout  batchRequestOutcome = "timeout"
	batchRequestOutcomeCanceled batchRequestOutcome = "canceled"
	batchRequestOutcomeFailed   batchRequestOutcome = "failed"
	batchRequestOutcomeClosed   batchRequestOutcome = "closed"
)

func batchRequestTerminalOutcome(err error) batchRequestOutcome {
	if err == nil {
		return batchRequestOutcomeOK
	}

	cause := errors.Cause(err)
	switch cause {
	case context.DeadlineExceeded:
		return batchRequestOutcomeTimeout
	case context.Canceled:
		return batchRequestOutcomeCanceled
	}

	switch cause.Error() {
	case "batchConn closed", "batch client closed":
		return batchRequestOutcomeClosed
	default:
		return batchRequestOutcomeFailed
	}
}

// normalizeObservedSentNS clamps the observed send boundary when sentNS is read concurrently with a later receive
// event. The send loop stamps sentAfterSendStartNS asynchronously, so a reader can observe
// firstRespAfterSendStartNS/recvAfterReqArriveNS first or see a stale sentAfterSendStartNS that lands after them.
func normalizeObservedSentNS(batchedNS, sentNS, firstRespNS, recvNS int64) int64 {
	boundaryNS := recvNS
	if boundaryNS == 0 {
		boundaryNS = firstRespNS
	}
	if boundaryNS > 0 {
		if sentNS == 0 {
			sentNS = batchedNS + 1
		} else if sentNS > boundaryNS {
			sentNS = boundaryNS - 1
		}
	}
	return sentNS
}

func loadBatchRequestProgress(entry *batchCommandsEntry) (batchedNS, sentNS, firstRespNS, recvNS int64) {
	if batchSelectedAfterReqArriveNS := entry.batchSelectedAfterReqArriveNS.Load(); batchSelectedAfterReqArriveNS > 0 {
		batchedNS = batchSelectedAfterReqArriveNS
	}
	if batchState := entry.batchState.Load(); batchState != nil && !batchState.sendStartAt.IsZero() {
		sendStartNS := max(batchState.sendStartAt.Sub(entry.reqArriveAt).Nanoseconds(), int64(1))
		if sentAfterSendStartNS := batchState.sentAfterSendStartNS.Load(); sentAfterSendStartNS > 0 {
			sentNS = sendStartNS + sentAfterSendStartNS
		}
		if firstRespAfterSendStartNS := batchState.firstRespAfterSendStartNS.Load(); firstRespAfterSendStartNS > 0 {
			firstRespNS = sendStartNS + firstRespAfterSendStartNS
		}
	}
	recvNS = entry.recvAfterReqArriveNS.Load()
	sentNS = normalizeObservedSentNS(batchedNS, sentNS, firstRespNS, recvNS)
	return
}

func formatBatchRequestTimeoutReason(entry *batchCommandsEntry, timeout time.Duration, now time.Time) string {
	var builder strings.Builder
	builder.WriteString("wait recvLoop timeout, timeout:")
	builder.WriteString(timeout.String())
	builder.WriteString(", ")
	writeBatchCommandsEntryProgress(&builder, entry, now)
	return builder.String()
}

func writeBatchCommandsEntryProgress(buf *strings.Builder, entry *batchCommandsEntry, now time.Time) *strings.Builder {
	if buf == nil {
		buf = &strings.Builder{}
	}
	buf.WriteString("EntryProgress{")

	batchedNS, sentNS, firstRespNS, recvNS := loadBatchRequestProgress(entry)
	if batchedNS == 0 {
		buf.WriteString("}")
		return buf
	}
	buf.WriteString("batch:")
	buf.WriteString(util.FormatDuration(time.Duration(batchedNS)))
	if batchState := entry.batchState.Load(); batchState != nil && batchState.batchSize > 0 {
		buf.WriteString(", size:")
		buf.WriteString(strconv.Itoa(batchState.batchSize))
	}

	if sentNS == 0 && recvNS == 0 {
		buf.WriteString(", send:")
		buf.WriteString(util.FormatDuration(time.Duration(max(now.Sub(entry.reqArriveAt).Nanoseconds()-batchedNS, int64(1)))))
		if batchRequestReceivedByTiKV(entry) {
			buf.WriteString(", ack:yes")
		}
		buf.WriteString("}")
		return buf
	}
	buf.WriteString(", send:")
	buf.WriteString(util.FormatDuration(time.Duration(max(sentNS-batchedNS, int64(1)))))

	if firstRespNS > 0 {
		buf.WriteString(", ack:")
		buf.WriteString(util.FormatDuration(time.Duration(max(firstRespNS-sentNS, int64(1)))))
	} else if batchRequestReceivedByTiKV(entry) {
		buf.WriteString(", ack:yes")
	}

	if recvNS > 0 {
		buf.WriteString(", recv:")
		buf.WriteString(util.FormatDuration(time.Duration(max(recvNS-sentNS, int64(1)))))
	}

	if entry.forwardedHost != "" {
		buf.WriteString(", fwd:")
		buf.WriteString(entry.forwardedHost)
	}
	buf.WriteString("}")
	return buf
}

func visitBatchRequestObservations(entry *batchCommandsEntry, terminal batchRequestOutcome, now time.Time, visit func(batchRequestStage, batchRequestOutcome, time.Duration)) {
	nowNS := max(now.Sub(entry.reqArriveAt).Nanoseconds(), int64(1))

	batchedNS, sentNS, firstRespNS, recvNS := loadBatchRequestProgress(entry)
	if batchedNS == 0 {
		visit(batchRequestStageBatchWait, terminal, time.Duration(nowNS))
		return
	}
	visit(batchRequestStageBatchWait, batchRequestOutcomeOK, time.Duration(batchedNS))

	if sentNS == 0 && recvNS == 0 {
		if firstRespNS > 0 {
			visit(batchRequestStageSendWait, batchRequestOutcomeOK, time.Duration(max(sentNS-batchedNS, int64(1))))
			visit(batchRequestStageRecvWait, terminal, time.Duration(max(nowNS-sentNS, int64(1))))
			return
		}
		visit(batchRequestStageSendWait, terminal, time.Duration(max(nowNS-batchedNS, int64(1))))
		return
	}
	visit(batchRequestStageSendWait, batchRequestOutcomeOK, time.Duration(max(sentNS-batchedNS, int64(1))))

	if recvNS == 0 {
		visit(batchRequestStageRecvWait, terminal, time.Duration(max(nowNS-sentNS, int64(1))))
		return
	}
	visit(batchRequestStageRecvWait, batchRequestOutcomeOK, time.Duration(max(recvNS-sentNS, int64(1))))

	if terminal == batchRequestOutcomeOK {
		visit(batchRequestStageDone, batchRequestOutcomeOK, time.Duration(nowNS))
	}
}

func observeBatchRequestCompletion(entry *batchCommandsEntry, err error) {
	if entry.batchRequestMetrics == nil {
		return
	}
	visitBatchRequestObservations(entry, batchRequestTerminalOutcome(err), time.Now(), entry.batchRequestMetrics.observe)
}

// batchCommandsBuilder collects a batch of `batchCommandsEntry`s to build
// `BatchCommandsRequest`s.
type batchCommandsBuilder struct {
	// Each BatchCommandsRequest_Request sent to a store has a unique identity to
	// distinguish its response.
	idAlloc     uint64
	entries     *PriorityQueue
	directGroup batchCommandsRequestGroup
	// In most cases, there isn't any forwardingGroups.
	forwardingGroups map[string]*batchCommandsRequestGroup

	latestReqArriveTime time.Time
}

type batchCommandsRequestGroup struct {
	req     *tikvpb.BatchCommandsRequest
	state   *batchCommandsRequestState
	entries []*batchCommandsEntry
}

func (b *batchCommandsBuilder) len() int {
	return b.entries.Len()
}

func (b *batchCommandsBuilder) push(entry *batchCommandsEntry) {
	b.entries.Push(entry)
	if entry.reqArriveAt.After(b.latestReqArriveTime) {
		b.latestReqArriveTime = entry.reqArriveAt
	}
}

const highTaskPriority = 10

func (b *batchCommandsBuilder) hasHighPriorityTask() bool {
	return b.entries.highestPriority() >= highTaskPriority
}

// buildWithLimit builds BatchCommandsRequests with the given limit.
// the highest priority tasks don't consume any limit,
// so the limit only works for normal tasks.
// The first return value is the request that doesn't need forwarding.
// The second is a map that maps forwarded hosts to requests.
func (b *batchCommandsBuilder) buildWithLimit(limit int64, collect func(id uint64, e *batchCommandsEntry),
) (*batchCommandsRequestGroup, map[string]*batchCommandsRequestGroup) {
	buildTime := time.Now()
	count := int64(0)
	build := func(reqs []Item) {
		for _, e := range reqs {
			e := e.(*batchCommandsEntry)
			if e.isCanceled() {
				continue
			}
			if e.priority() < highTaskPriority {
				count++
			}

			var (
				group *batchCommandsRequestGroup
				ok    bool
			)
			if e.forwardedHost == "" {
				group = &b.directGroup
				if group.state == nil {
					group.state = &batchCommandsRequestState{}
				}
			} else {
				group, ok = b.forwardingGroups[e.forwardedHost]
				if !ok {
					group = &batchCommandsRequestGroup{
						req:   &tikvpb.BatchCommandsRequest{},
						state: &batchCommandsRequestState{},
					}
					b.forwardingGroups[e.forwardedHost] = group
				}
			}
			e.batchSelectedAfterReqArriveNS.Store(max(buildTime.Sub(e.reqArriveAt).Nanoseconds(), int64(1)))

			b.idAlloc++
			if collect != nil {
				collect(b.idAlloc, e)
			}
			group.req.RequestIds = append(group.req.RequestIds, b.idAlloc)
			group.req.Requests = append(group.req.Requests, e.req)
			group.entries = append(group.entries, e)
			group.state.batchSize = len(group.entries)
		}
	}
	for (count < limit && b.entries.Len() > 0) || b.hasHighPriorityTask() {
		n := limit
		if limit == 0 {
			n = 1
		}
		b.entries.Take(int(n), build)
	}
	var directGroup *batchCommandsRequestGroup
	if len(b.directGroup.req.Requests) > 0 {
		directGroup = &b.directGroup
	}
	return directGroup, b.forwardingGroups
}

// cancel all requests, only used in test.
func (b *batchCommandsBuilder) cancel(e error) {
	for _, entry := range b.entries.all() {
		entry.(*batchCommandsEntry).error(e)
	}
	b.entries.reset()
}

// reset resets the builder to the initial state.
// Should call it before collecting a new batch.
func (b *batchCommandsBuilder) reset() {
	b.entries.clean()
	// NOTE: We can't simply set entries = entries[:0] here.
	// The data in the cap part of the slice would reference the prewrite keys whose
	// underlying memory is borrowed from memdb. The reference cause GC can't release
	// the memdb, leading to serious memory leak problems in the large transaction case.
	for i := 0; i < len(b.directGroup.req.Requests); i++ {
		b.directGroup.req.Requests[i] = nil
	}
	b.directGroup.req.Requests = b.directGroup.req.Requests[:0]
	b.directGroup.req.RequestIds = b.directGroup.req.RequestIds[:0]
	for i := 0; i < len(b.directGroup.entries); i++ {
		b.directGroup.entries[i] = nil
	}
	b.directGroup.entries = b.directGroup.entries[:0]
	b.directGroup.state = nil

	for k := range b.forwardingGroups {
		delete(b.forwardingGroups, k)
	}
}

func newBatchCommandsBuilder(maxBatchSize uint) *batchCommandsBuilder {
	return &batchCommandsBuilder{
		idAlloc: 0,
		entries: NewPriorityQueue(),
		directGroup: batchCommandsRequestGroup{
			req: &tikvpb.BatchCommandsRequest{
				Requests:   make([]*tikvpb.BatchCommandsRequest_Request, 0, maxBatchSize),
				RequestIds: make([]uint64, 0, maxBatchSize),
			},
			entries: make([]*batchCommandsEntry, 0, maxBatchSize),
		},
		forwardingGroups: make(map[string]*batchCommandsRequestGroup),
	}
}

const (
	batchSendTailLatThreshold   = 20 * time.Millisecond
	batchRecvTailLatThreshold   = 20 * time.Millisecond
	batchRequestInspectInterval = time.Minute
	batchRequestSlowThreshold   = 30 * time.Second
	batchRequestHangThreshold   = 90 * time.Second
)

type pendingBatchRequestStats struct {
	oldestID                uint64
	oldestEntry             *batchCommandsEntry
	oldestWait              time.Duration
	slowCount               int
	slowUnconfirmedCount    int
	hangingCount            int
	hangingUnconfirmedCount int
}

func batchRequestReceivedByTiKV(entry *batchCommandsEntry) bool {
	batchState := entry.batchState.Load()
	if batchState == nil {
		return false
	}
	stream := batchState.stream
	if stream == nil {
		return false
	}
	requestID := entry.requestID.Load()
	return requestID > 0 && requestID <= stream.maxRespReqID.Load()
}

type batchConnMetrics struct {
	pendingRequests prometheus.Observer
	batchSize       prometheus.Observer

	sendLoopWaitHeadDur prometheus.Observer
	sendLoopWaitMoreDur prometheus.Observer
	sendLoopSendDur     prometheus.Observer

	batchSendTailLat prometheus.Observer

	headArrivalInterval prometheus.Observer
	batchMoreRequests   prometheus.Observer

	bestBatchSize prometheus.Observer
}

// batchCommandsClientMetrics contains metrics for a single batchCommandsClient.
type batchCommandsClientMetrics struct {
	target  string
	connIdx string

	// Non-forwarded stream metrics

	// recvLoopRecvDur tracks time spent blocked in stream Recv.
	recvLoopRecvDur prometheus.Observer
	// recvLoopProcessDur tracks time spent handling a received batch.
	recvLoopProcessDur prometheus.Observer
	// batchRecvTailLat records unusually slow recv loop iterations.
	batchRecvTailLat prometheus.Observer
	// tikvSendTailLat records tail latency from TiKV sending a batch response until the client receives it.
	tikvSendTailLat prometheus.Observer
	// canceledEntryTailLat records canceled entries that later still receive responses.
	canceledEntryTailLat prometheus.Observer
	// trackedRequestCount counts requests registered on this stream.
	trackedRequestCount prometheus.Counter
	// retiredRequestCount counts requests removed from this stream.
	retiredRequestCount prometheus.Counter
	// completedResponseCount counts responses matched to tracked requests.
	completedResponseCount prometheus.Counter
	// outdatedResponseCount counts responses for requests no longer tracked.
	outdatedResponseCount prometheus.Counter

	// Forwarded stream metrics are initialized lazily because most clients never create
	// forwarding streams.
	forwardedMetricsInit      sync.Once
	recvLoopRecvDurFwd        prometheus.Observer
	recvLoopProcessDurFwd     prometheus.Observer
	batchRecvTailLatFwd       prometheus.Observer
	tikvSendTailLatFwd        prometheus.Observer
	canceledEntryTailLatFwd   prometheus.Observer
	trackedRequestCountFwd    prometheus.Counter
	retiredRequestCountFwd    prometheus.Counter
	completedResponseCountFwd prometheus.Counter
	outdatedResponseCountFwd  prometheus.Counter
}

// initBatchCommandsClientMetrics initializes metrics for a batchCommandsClient.
// The connIdx is the index of this client in batchCommandsClients slice.
func initBatchCommandsClientMetrics(target string, connIdx string) *batchCommandsClientMetrics {
	return &batchCommandsClientMetrics{
		target:                 target,
		connIdx:                connIdx,
		recvLoopRecvDur:        metrics.TiKVBatchStreamRecvLoopDuration.WithLabelValues(target, connIdx, "0", "recv"),
		recvLoopProcessDur:     metrics.TiKVBatchStreamRecvLoopDuration.WithLabelValues(target, connIdx, "0", "process"),
		batchRecvTailLat:       metrics.TiKVBatchStreamRecvTailLatency.WithLabelValues(target, connIdx, "0"),
		tikvSendTailLat:        metrics.TiKVBatchStreamTiKVSendTailLatency.WithLabelValues(target, connIdx, "0"),
		canceledEntryTailLat:   metrics.TiKVBatchStreamCanceledEntryTailLatency.WithLabelValues(target, connIdx, "0"),
		trackedRequestCount:    metrics.TiKVBatchStreamTrackedRequestCount.WithLabelValues(target, connIdx, "0"),
		retiredRequestCount:    metrics.TiKVBatchStreamRetiredRequestCount.WithLabelValues(target, connIdx, "0"),
		completedResponseCount: metrics.TiKVBatchStreamCompletedResponseCount.WithLabelValues(target, connIdx, "0"),
		outdatedResponseCount:  metrics.TiKVBatchStreamOutdatedResponseCount.WithLabelValues(target, connIdx, "0"),
	}
}

func (m *batchCommandsClientMetrics) recvLoopRecvDurObserver(forwarded bool) prometheus.Observer {
	if forwarded {
		m.initForwardedMetrics()
		return m.recvLoopRecvDurFwd
	}
	return m.recvLoopRecvDur
}

func (m *batchCommandsClientMetrics) recvLoopProcessDurObserver(forwarded bool) prometheus.Observer {
	if forwarded {
		m.initForwardedMetrics()
		return m.recvLoopProcessDurFwd
	}
	return m.recvLoopProcessDur
}

func (m *batchCommandsClientMetrics) batchRecvTailLatObserver(forwarded bool) prometheus.Observer {
	if forwarded {
		m.initForwardedMetrics()
		return m.batchRecvTailLatFwd
	}
	return m.batchRecvTailLat
}

func (m *batchCommandsClientMetrics) tikvSendTailLatObserver(forwarded bool) prometheus.Observer {
	if forwarded {
		m.initForwardedMetrics()
		return m.tikvSendTailLatFwd
	}
	return m.tikvSendTailLat
}

func (m *batchCommandsClientMetrics) canceledEntryTailLatObserver(forwarded bool) prometheus.Observer {
	if forwarded {
		m.initForwardedMetrics()
		return m.canceledEntryTailLatFwd
	}
	return m.canceledEntryTailLat
}

func (m *batchCommandsClientMetrics) trackedRequestCounter(forwarded bool) prometheus.Counter {
	if forwarded {
		m.initForwardedMetrics()
		return m.trackedRequestCountFwd
	}
	return m.trackedRequestCount
}

func (m *batchCommandsClientMetrics) retiredRequestCounter(forwarded bool) prometheus.Counter {
	if forwarded {
		m.initForwardedMetrics()
		return m.retiredRequestCountFwd
	}
	return m.retiredRequestCount
}

func (m *batchCommandsClientMetrics) completedResponseCounter(forwarded bool) prometheus.Counter {
	if forwarded {
		m.initForwardedMetrics()
		return m.completedResponseCountFwd
	}
	return m.completedResponseCount
}

func (m *batchCommandsClientMetrics) outdatedResponseCounter(forwarded bool) prometheus.Counter {
	if forwarded {
		m.initForwardedMetrics()
		return m.outdatedResponseCountFwd
	}
	return m.outdatedResponseCount
}

func (m *batchCommandsClientMetrics) initForwardedMetrics() {
	m.forwardedMetricsInit.Do(func() {
		m.recvLoopRecvDurFwd = metrics.TiKVBatchStreamRecvLoopDuration.WithLabelValues(m.target, m.connIdx, "1", "recv")
		m.recvLoopProcessDurFwd = metrics.TiKVBatchStreamRecvLoopDuration.WithLabelValues(m.target, m.connIdx, "1", "process")
		m.batchRecvTailLatFwd = metrics.TiKVBatchStreamRecvTailLatency.WithLabelValues(m.target, m.connIdx, "1")
		m.tikvSendTailLatFwd = metrics.TiKVBatchStreamTiKVSendTailLatency.WithLabelValues(m.target, m.connIdx, "1")
		m.canceledEntryTailLatFwd = metrics.TiKVBatchStreamCanceledEntryTailLatency.WithLabelValues(m.target, m.connIdx, "1")
		m.trackedRequestCountFwd = metrics.TiKVBatchStreamTrackedRequestCount.WithLabelValues(m.target, m.connIdx, "1")
		m.retiredRequestCountFwd = metrics.TiKVBatchStreamRetiredRequestCount.WithLabelValues(m.target, m.connIdx, "1")
		m.completedResponseCountFwd = metrics.TiKVBatchStreamCompletedResponseCount.WithLabelValues(m.target, m.connIdx, "1")
		m.outdatedResponseCountFwd = metrics.TiKVBatchStreamOutdatedResponseCount.WithLabelValues(m.target, m.connIdx, "1")
	})
}

var (
	// presetBatchPolicies defines a set of [turboBatchOptions] as batch policies.
	presetBatchPolicies = map[string]turboBatchOptions{
		config.BatchPolicyBasic:    {},
		config.BatchPolicyStandard: {V: turboBatchTimeBased, T: 0.0001, N: 5, W: 0.2, P: 0.8, Q: 0.8},
		config.BatchPolicyPositive: {V: turboBatchAlways, T: 0.0001},
	}
)

const (
	turboBatchAlways = iota
	turboBatchTimeBased
	turboBatchProbBased
)

// turboBatchOptions defines internal options for the [turboBatchTrigger].
type turboBatchOptions struct {
	// V determines the batch strategy: always(v=0), time-based(v=1), prob-based(v=2).
	V int `json:"v"`
	// N currently is used to determine the max arrival interval (n * t).
	N int `json:"n,omitempty"`
	// T is the max wait time for the batch.
	T float64 `json:"t,omitempty"`
	// W is used to adjust the `estArrivalInterval` or `estFetchMoreProb` dynamically.
	//   - time-based(v=1): estArrivalInterval = w*reqArrivalInterval + (1-w)*estArrivalInterval
	//   - prob-based(v=2): estFetchMoreProb = w*thisProb + (1-w)*estFetchMoreProb
	W float64 `json:"w,omitempty"`
	// P is used to determine whether to fetch more requests:
	//   - time-based(v=1): estArrivalInterval < p * t
	//   - prob-based(v=2): estFetchMoreProb > p
	P float64 `json:"p,omitempty"`
	// Q is used to adjust the `batchWaitSize` dynamically.
	Q float64 `json:"q,omitempty"`
}

// turboBatchTrigger is used to trigger the `fetchMorePendingRequests` dynamically according to the request arrival
// intervals. The option `v` indicates the strategy of triggering:
//
//   - turboBatchAlways: always fetch more requests.
//
//   - turboBatchTimeBased: fetch more requests if estArrivalInterval < p * t
//     where estArrivalInterval = w*reqArrivalInterval + (1-w)*estArrivalInterval
//     and reqArrivalInterval = min(reqArrivalInterval, n * t)
//
//   - turboBatchProbBased: fetch more requests if estFetchMoreProb > p
//     where estFetchMoreProb = w*thisProb + (1-w)*estFetchMoreProb
//     and thisProb = reqArrivalInterval < t ? 1 : 0
//
// The option `q` is used to adjust the `batchWaitSize` dynamically. If the fractional part of the `avgBatchWaitSize` is
// greater or equal to `q`, the `batchWaitSize` will be increased by 1.
type turboBatchTrigger struct {
	opts turboBatchOptions

	estFetchMoreProb   float64
	estArrivalInterval float64
	maxArrivalInterval float64
}

func newTurboBatchTriggerFromPolicy(policy string) (trigger turboBatchTrigger, ok bool) {
	if opts, found := presetBatchPolicies[policy]; found {
		return turboBatchTrigger{opts: opts}, true
	}
	rawOpts, _ := strings.CutPrefix(policy, config.BatchPolicyCustom)
	if err := json.Unmarshal([]byte(strings.TrimSpace(rawOpts)), &trigger.opts); err != nil {
		return turboBatchTrigger{opts: presetBatchPolicies[config.DefBatchPolicy]}, false
	}
	ok = true
	return
}

func (t *turboBatchTrigger) turboWaitSeconds() float64 {
	return t.opts.T
}

func (t *turboBatchTrigger) turboWaitTime() time.Duration {
	return time.Duration(t.opts.T * float64(time.Second))
}

func (t *turboBatchTrigger) needFetchMore(reqArrivalInterval time.Duration) bool {
	switch t.opts.V {
	case turboBatchTimeBased:
		thisArrivalInterval := reqArrivalInterval.Seconds()
		if t.maxArrivalInterval == 0 {
			t.maxArrivalInterval = t.turboWaitSeconds() * float64(t.opts.N)
		}
		if thisArrivalInterval > t.maxArrivalInterval {
			thisArrivalInterval = t.maxArrivalInterval
		}
		if t.estArrivalInterval == 0 {
			t.estArrivalInterval = thisArrivalInterval
		} else {
			t.estArrivalInterval = t.opts.W*thisArrivalInterval + (1-t.opts.W)*t.estArrivalInterval
		}
		return t.estArrivalInterval < t.turboWaitSeconds()*t.opts.P
	case turboBatchProbBased:
		thisProb := .0
		if reqArrivalInterval.Seconds() < t.turboWaitSeconds() {
			thisProb = 1
		}
		t.estFetchMoreProb = t.opts.W*thisProb + (1-t.opts.W)*t.estFetchMoreProb
		return t.estFetchMoreProb > t.opts.P
	default:
		return true
	}
}

func (t *turboBatchTrigger) preferredBatchWaitSize(avgBatchWaitSize float64, defBatchWaitSize int) int {
	if t.opts.V == turboBatchAlways {
		return defBatchWaitSize
	}
	n, m := math.Modf(avgBatchWaitSize)
	batchWaitSize := int(n)
	if m >= t.opts.Q {
		batchWaitSize++
	}
	return batchWaitSize
}

type tryLock struct {
	*sync.Cond
	reCreating bool
}

func (l *tryLock) tryLockForSend() bool {
	l.L.Lock()
	if l.reCreating {
		l.L.Unlock()
		return false
	}
	return true
}

func (l *tryLock) unlockForSend() {
	l.L.Unlock()
}

func (l *tryLock) lockForRecreate() {
	l.L.Lock()
	for l.reCreating {
		l.Wait()
	}
	l.reCreating = true
	l.L.Unlock()
}

func (l *tryLock) unlockForRecreate() {
	l.L.Lock()
	l.reCreating = false
	l.Broadcast()
	l.L.Unlock()
}

type batchCommandsStream struct {
	tikvpb.Tikv_BatchCommandsClient
	forwardedHost string
	connIdx       string
	maxRespReqID  atomic.Uint64
}

func (s *batchCommandsStream) recv() (resp *tikvpb.BatchCommandsResponse, err error) {
	defer func() {
		if r := recover(); r != nil {
			metrics.TiKVPanicCounter.WithLabelValues(metrics.LabelBatchRecvLoop).Inc()
			logutil.BgLogger().Error("batchCommandsClient.recv panic",
				zap.Any("r", r),
				zap.Stack("stack"))
			err = errors.New("batch conn recv paniced")
		}
	}()
	if _, err := util.EvalFailpoint("gotErrorInRecvLoop"); err == nil {
		return nil, errors.New("injected error in batchRecvLoop")
	}
	// When `conn.Close()` is called, `client.Recv()` will return an error.
	resp, err = s.Recv()
	return
}

// recreate creates a new BatchCommands stream. The conn should be ready for work.
func (s *batchCommandsStream) recreate(conn *grpc.ClientConn) error {
	tikvClient := tikvpb.NewTikvClient(conn)
	ctx := context.TODO()
	if s.forwardedHost != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, forwardMetadataKey, s.forwardedHost)
	}
	if s.connIdx != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, batchConnIdxMetadataKey, s.connIdx)
	}
	streamClient, err := tikvClient.BatchCommands(ctx)
	if err != nil {
		return errors.WithStack(err)
	}
	s.Tikv_BatchCommandsClient = streamClient
	s.maxRespReqID.Store(0)
	return nil
}

type batchCommandsClient struct {
	// The target host.
	target  string
	connIdx string

	conn *grpc.ClientConn
	// client and forwardedClients are protected by tryLock.
	//
	// client is the stream that needn't forwarding.
	client *batchCommandsStream
	// TiDB uses [gRPC-metadata](https://github.com/grpc/grpc-go/blob/master/Documentation/grpc-metadata.md) to
	// indicate a request needs forwarding. gRPC doesn't support setting a metadata for each request in a stream,
	// so we need to create a stream for each forwarded host.
	//
	// forwardedClients are clients that need forwarding. It's a map that maps forwarded hosts to streams
	forwardedClients map[string]*batchCommandsStream
	batched          sync.Map

	tikvClientCfg config.TiKVClient
	tikvLoad      *uint64
	dialTimeout   time.Duration

	// Increased in each reconnection.
	// It's used to prevent the connection from reconnecting multiple times
	// due to one failure because there may be more than 1 `batchRecvLoop`s.
	epoch uint64
	// closed indicates the batch client is closed explicitly or not.
	closed int32
	// tryLock protects client when re-create the streaming.
	tryLock

	// sent is the number of the requests are processed by tikv server.
	sent atomic.Int64
	// maxConcurrencyRequestLimit is the max allowed number of requests to be sent the tikv
	maxConcurrencyRequestLimit atomic.Int64

	// eventListener is the listener set by external code to observe some events in the client. It's stored in a atomic
	// pointer to make setting thread-safe.
	eventListener *atomic.Pointer[ClientEventListener]

	metrics *batchCommandsClientMetrics
}

func (c *batchCommandsClient) isStopped() bool {
	return atomic.LoadInt32(&c.closed) != 0
}

func (c *batchCommandsClient) available() int64 {
	limit := c.maxConcurrencyRequestLimit.Load()
	sent := c.sent.Load()
	//  The `sent` could be less than 0, see https://github.com/tikv/client-go/issues/1225 for details.
	if sent > 0 {
		if limit > sent {
			return limit - sent
		}
		return 0
	}
	return limit
}

func (c *batchCommandsClient) stream(forwardedHost string) *batchCommandsStream {
	if forwardedHost == "" {
		return c.client
	}
	return c.forwardedClients[forwardedHost]
}

func (c *batchCommandsClient) send(forwardedHost string, grp *batchCommandsRequestGroup) {
	defer reuseRequestData(grp.req)

	err := c.initBatchClient(forwardedHost)
	if err != nil {
		logutil.BgLogger().Warn(
			"init create streaming fail",
			zap.String("target", c.target),
			zap.String("forwardedHost", forwardedHost),
			zap.Error(err),
		)
		for _, entry := range grp.entries {
			entry.error(err)
		}
		return
	}
	client := c.stream(forwardedHost)
	sendStartAt := time.Now()
	grp.state.stream = client
	grp.state.sendStartAt = sendStartAt
	grp.req.ClientSendTimeNs = uint64(sendStartAt.UnixNano())
	for i, requestID := range grp.req.RequestIds {
		entry := grp.entries[i]
		entry.batchState.Store(grp.state)
		entry.requestID.Store(requestID)
		c.batched.Store(requestID, entry)
		c.sent.Add(1)
		c.metrics.trackedRequestCounter(entry.forwardedHost != "").Inc()
		if trace.IsEnabled() {
			trace.Log(entry.ctx, "rpc", "send")
		}
	}

	if err := client.Send(grp.req); err != nil {
		logutil.BgLogger().Info(
			"sending batch commands meets error",
			zap.String("target", c.target),
			zap.String("forwardedHost", forwardedHost),
			zap.Uint64s("requestIDs", grp.req.RequestIds),
			zap.Error(err),
		)
		c.failRequestsByIDs(err, grp.req.RequestIds) // fast fail requests.
		return
	}
	grp.state.sentAfterSendStartNS.CompareAndSwap(0, max(time.Since(sendStartAt).Nanoseconds(), int64(1)))
}

// `failPendingRequests` must be called in locked contexts in order to avoid double closing channels.
// when enable-forwarding is true, the `forwardedHost` maybe not empty.
// failPendingRequests fails all pending requests which req.forwardedHost equals to forwardedHost parameter.
// Why need check `forwardedHost`? Here is an example, when enable-forwarding is true, and this client has network issue with store1:
//   - some requests are sent to store1 with forwarding, such as forwardedHost is store2, those requests will succeed.
//   - some requests are sent to store1 without forwarding, and may fail then `failPendingRequests` would be called,
//     if we don't check `forwardedHost` and fail all pending requests, the requests with forwarding will be failed too. this may cause some issue:
//     1. data race. see https://github.com/tikv/client-go/issues/1222 and TestRandomRestartStoreAndForwarding.
//     2. panic which cause by `send on closed channel`, since failPendingRequests will close the entry.res channel,
//     but in another batchRecvLoop goroutine,  it may receive the response from forwardedHost store2 and try to send the response to entry.res channel,
//     then panic by send on closed channel.
func (c *batchCommandsClient) failPendingRequests(err error, forwardedHost string) {
	util.EvalFailpoint("panicInFailPendingRequests")
	c.batched.Range(func(key, value interface{}) bool {
		id, _ := key.(uint64)
		entry, _ := value.(*batchCommandsEntry)
		if entry.forwardedHost == forwardedHost {
			c.failRequest(err, id, entry)
		}
		return true
	})
}

// failAsyncRequestsOnClose fails all async requests when the client is closed.
func (c *batchCommandsClient) failAsyncRequestsOnClose() {
	err := errors.New("batch client closed")
	c.batched.Range(func(key, value interface{}) bool {
		id, _ := key.(uint64)
		entry, _ := value.(*batchCommandsEntry)
		if entry.async() {
			c.failRequest(err, id, entry)
		}
		return true
	})
}

// failRequestsByIDs fails requests by requestID.
func (c *batchCommandsClient) failRequestsByIDs(err error, requestIDs []uint64) {
	for _, requestID := range requestIDs {
		value, ok := c.batched.Load(requestID)
		if !ok {
			continue
		}
		c.failRequest(err, requestID, value.(*batchCommandsEntry))
	}
}

func (c *batchCommandsClient) failRequest(err error, requestID uint64, entry *batchCommandsEntry) {
	c.batched.Delete(requestID)
	c.sent.Add(-1)
	c.metrics.retiredRequestCounter(entry.forwardedHost != "").Inc()
	entry.error(err)
}

func (c *batchCommandsClient) waitConnReady() (err error) {
	state := c.conn.GetState()
	if state == connectivity.Ready {
		return
	}
	// Trigger idle connection to reconnection
	// Put it outside loop to avoid unnecessary reconnecting.
	if state == connectivity.Idle {
		c.conn.Connect()
	}
	start := time.Now()
	defer func() {
		metrics.TiKVBatchClientWaitEstablish.Observe(time.Since(start).Seconds())
	}()
	dialCtx, cancel := context.WithTimeout(context.Background(), c.dialTimeout)
	for {
		s := c.conn.GetState()
		if s == connectivity.Ready {
			cancel()
			break
		}
		if !c.conn.WaitForStateChange(dialCtx, s) {
			cancel()
			err = dialCtx.Err()
			return
		}
	}
	return
}

func (c *batchCommandsClient) recreateStreamingClientOnce(streamClient *batchCommandsStream) error {
	err := c.waitConnReady()
	// Re-establish a application layer stream. TCP layer is handled by gRPC.
	if err == nil {
		err := streamClient.recreate(c.conn)
		if err == nil {
			logutil.BgLogger().Info(
				"batchRecvLoop re-create streaming success",
				zap.String("target", c.target),
				zap.String("forwardedHost", streamClient.forwardedHost),
			)
			return nil
		}
	}
	logutil.BgLogger().Info(
		"batchRecvLoop re-create streaming fail",
		zap.String("target", c.target),
		zap.String("forwardedHost", streamClient.forwardedHost),
		zap.Error(err),
	)
	return err
}

func (c *batchCommandsClient) inspectPendingBatchRequests(checkTime time.Time) pendingBatchRequestStats {
	stats := pendingBatchRequestStats{}
	c.batched.Range(func(key, value interface{}) bool {
		entry := value.(*batchCommandsEntry)
		wait := checkTime.Sub(entry.reqArriveAt)
		if wait < batchRequestSlowThreshold {
			return true
		}
		stats.slowCount++
		requestID := key.(uint64)
		batchState := entry.batchState.Load()
		unconfirmed := batchState == nil ||
			(batchState.firstRespAfterSendStartNS.Load() == 0 && !batchRequestReceivedByTiKV(entry))
		if unconfirmed {
			stats.slowUnconfirmedCount++
		}
		if wait >= batchRequestHangThreshold {
			stats.hangingCount++
			if unconfirmed {
				stats.hangingUnconfirmedCount++
			}
		}
		if stats.oldestEntry == nil || wait > stats.oldestWait {
			stats.oldestID = requestID
			stats.oldestEntry = entry
			stats.oldestWait = wait
		}
		return true
	})
	return stats
}

func (c *batchCommandsClient) logPendingBatchRequests(checkTime time.Time) {
	stats := c.inspectPendingBatchRequests(checkTime)
	if stats.oldestEntry != nil {
		logutil.BgLogger().Warn(
			"batchRecvLoop detects slow pending request",
			zap.String("target", c.target),
			zap.String("conn", c.connIdx),
			zap.Uint64("requestID", stats.oldestID),
			zap.Duration("waitTime", stats.oldestWait),
			zap.Bool("canceled", atomic.LoadInt32(&stats.oldestEntry.canceled) == 1),
			zap.String("progress", writeBatchCommandsEntryProgress(nil, stats.oldestEntry, checkTime).String()),
			zap.Bool("receivedByTiKV", batchRequestReceivedByTiKV(stats.oldestEntry)),
			zap.Int("slowCount", stats.slowCount),
			zap.Int("slowUnconfirmedCount", stats.slowUnconfirmedCount),
			zap.Int("hangingCount", stats.hangingCount),
			zap.Int("hangingUnconfirmedCount", stats.hangingUnconfirmedCount),
		)
	}
}

func (c *batchCommandsClient) batchRecvLoop(cfg config.TiKVClient, tikvTransportLayerLoad *uint64, streamClient *batchCommandsStream) {
	defer func() {
		if r := recover(); r != nil {
			metrics.TiKVPanicCounter.WithLabelValues(metrics.LabelBatchRecvLoop).Inc()
			logutil.BgLogger().Error("batchRecvLoop",
				zap.Any("r", r),
				zap.Stack("stack"))
			logutil.BgLogger().Info("restart batchRecvLoop")
			go c.batchRecvLoop(cfg, tikvTransportLayerLoad, streamClient)
		} else {
			c.failAsyncRequestsOnClose()
		}
	}()

	forwarded := streamClient.forwardedHost != ""
	recvDur := c.metrics.recvLoopRecvDurObserver(forwarded)
	recvDurTail := c.metrics.batchRecvTailLatObserver(forwarded)
	tikvSendTailLat := c.metrics.tikvSendTailLatObserver(forwarded)
	canceledEntryTailLat := c.metrics.canceledEntryTailLatObserver(forwarded)
	processDur := c.metrics.recvLoopProcessDurObserver(forwarded)
	retiredReqCount := c.metrics.retiredRequestCounter(forwarded)
	completedRespCount := c.metrics.completedResponseCounter(forwarded)
	outdatedRespCount := c.metrics.outdatedResponseCounter(forwarded)

	epoch := atomic.LoadUint64(&c.epoch)
	for {
		recvLoopStartTime := time.Now()
		resp, err := streamClient.recv()
		respRecvTime := time.Now()
		recvDurVal := respRecvTime.Sub(recvLoopStartTime)
		recvDur.Observe(recvDurVal.Seconds())
		if recvDurVal > batchRecvTailLatThreshold {
			recvDurTail.Observe(recvDurVal.Seconds())
		}
		if tidbRecvTimeNs, tikvSendTimeNs := respRecvTime.UnixNano(), int64(resp.GetTikvSendTimeNs()); tikvSendTimeNs > 0 && tidbRecvTimeNs-tikvSendTimeNs > batchRecvTailLatThreshold.Nanoseconds()/2 {
			tikvSendTailLat.Observe(float64(tidbRecvTimeNs-tikvSendTimeNs) / 1e9)
		}
		if err != nil {
			if c.isStopped() {
				return
			}
			logutil.BgLogger().Debug(
				"batchRecvLoop fails when receiving, needs to reconnect",
				zap.String("target", c.target),
				zap.String("forwardedHost", streamClient.forwardedHost),
				zap.Error(err),
			)

			now := time.Now()
			if stopped := c.recreateStreamingClient(err, streamClient, &epoch); stopped {
				return
			}
			metrics.TiKVBatchClientUnavailable.Observe(time.Since(now).Seconds())
			continue
		}

		if resp.GetHealthFeedback() != nil {
			if val, err := util.EvalFailpoint("injectHealthFeedbackSlowScore"); err == nil {
				v, ok := val.(int)
				if !ok || v < 0 || v > 100 {
					panic(fmt.Sprintf("invalid injection in failpoint injectHealthFeedbackSlowScore: %+q", v))
				}
				resp.GetHealthFeedback().SlowScore = int32(v)
			}
			c.onHealthFeedback(resp.GetHealthFeedback())
		}

		responses := resp.GetResponses()
		var maxRespReqID uint64
		for i, requestID := range resp.GetRequestIds() {
			if requestID > maxRespReqID {
				maxRespReqID = requestID
			}
			value, ok := c.batched.Load(requestID)
			if !ok {
				// this maybe caused by batchCommandsClient#send meets ambiguous error that request has be sent to TiKV but still report a error.
				// then TiKV will send response back though stream and reach here.
				outdatedRespCount.Inc()
				logutil.BgLogger().Warn("batchRecvLoop receives outdated response", zap.Uint64("requestID", requestID), zap.String("conn", c.connIdx), zap.String("forwardedHost", streamClient.forwardedHost))
				continue
			}
			completedRespCount.Inc()
			entry := value.(*batchCommandsEntry)
			batchState := entry.batchState.Load()
			batchState.firstRespAfterSendStartNS.CompareAndSwap(0, max(respRecvTime.Sub(batchState.sendStartAt).Nanoseconds(), int64(1)))
			entry.recvAfterReqArriveNS.CompareAndSwap(0, max(respRecvTime.Sub(entry.reqArriveAt).Nanoseconds(), int64(1)))
			recvNS := entry.recvAfterReqArriveNS.Load()
			if trace.IsEnabled() {
				trace.Log(entry.ctx, "rpc", "received")
			}
			logutil.Eventf(entry.ctx, "receive %T response with other %d batched requests from %s", responses[i].GetCmd(), len(responses), c.target)
			if atomic.LoadInt32(&entry.canceled) == 0 {
				// Put the response only if the request is not canceled.
				entry.response(responses[i])
			} else {
				batchedNS, _, _, _ := loadBatchRequestProgress(entry)
				canceledEntryTailLat.Observe(float64(time.Duration(max(recvNS-batchedNS, int64(1)))) / 1e9)
			}
			c.batched.Delete(requestID)
			c.sent.Add(-1)
			retiredReqCount.Inc()
		}
		if maxRespReqID > 0 {
			for {
				prev := streamClient.maxRespReqID.Load()
				if prev >= maxRespReqID || streamClient.maxRespReqID.CompareAndSwap(prev, maxRespReqID) {
					break
				}
			}
		}

		transportLayerLoad := resp.GetTransportLayerLoad()
		if transportLayerLoad > 0 && cfg.MaxBatchWaitTime > 0 {
			// We need to consider TiKV load only if batch-wait strategy is enabled.
			atomic.StoreUint64(tikvTransportLayerLoad, transportLayerLoad)
		}
		processDur.Observe(time.Since(recvLoopStartTime).Seconds())
	}
}

func (c *batchCommandsClient) onHealthFeedback(feedback *kvrpcpb.HealthFeedback) {
	if h := c.eventListener.Load(); h != nil {
		(*h).OnHealthFeedback(feedback)
	}
}

func (c *batchCommandsClient) recreateStreamingClient(err error, streamClient *batchCommandsStream, epoch *uint64) (stopped bool) {
	// Forbids the batchSendLoop using the old client and
	// blocks other streams trying to recreate.
	c.lockForRecreate()
	defer c.unlockForRecreate()

	// Each batchCommandsStream has a batchRecvLoop. There is only one stream waiting for
	// the connection ready in every epoch to prevent the connection from reconnecting
	// multiple times due to one failure.
	//
	// Check it in the locked scope to prevent the stream which gets the token from
	// reconnecting lately, i.e.
	// goroutine 1       | goroutine 2
	// CAS success       |
	//                   | CAS failure
	//                   | lockForRecreate
	//                   | recreate error
	//                   | unlockForRecreate
	// lockForRecreate   |
	// waitConnReady     |
	// recreate          |
	// unlockForRecreate |
	waitConnReady := atomic.CompareAndSwapUint64(&c.epoch, *epoch, *epoch+1)
	if !waitConnReady {
		*epoch = atomic.LoadUint64(&c.epoch)
		if err := streamClient.recreate(c.conn); err != nil {
			logutil.BgLogger().Info(
				"batchRecvLoop re-create streaming fail",
				zap.String("target", c.target),
				zap.String("forwardedHost", streamClient.forwardedHost),
				zap.Error(err),
			)
		}
		return c.isStopped()
	}
	*epoch++

	c.failPendingRequests(err, streamClient.forwardedHost) // fail all pending requests.
	b := retry.NewBackofferWithVars(context.Background(), math.MaxInt32, nil)
	for { // try to re-create the streaming in the loop.
		if c.isStopped() {
			return true
		}
		err1 := c.recreateStreamingClientOnce(streamClient)
		if err1 == nil {
			break
		}

		err2 := b.Backoff(retry.BoTiKVRPC, err1)
		// As timeout is set to math.MaxUint32, err2 should always be nil.
		// This line is added to make the 'make errcheck' pass.
		tikverr.Log(err2)
	}
	return false
}

func (c *batchCommandsClient) newBatchStream(forwardedHost string) (*batchCommandsStream, error) {
	batchStream := &batchCommandsStream{forwardedHost: forwardedHost, connIdx: c.connIdx}
	if err := batchStream.recreate(c.conn); err != nil {
		return nil, err
	}
	return batchStream, nil
}

func (c *batchCommandsClient) initBatchClient(forwardedHost string) error {
	if forwardedHost == "" && c.client != nil {
		return nil
	}
	if _, ok := c.forwardedClients[forwardedHost]; ok {
		return nil
	}

	if err := c.waitConnReady(); err != nil {
		return err
	}

	streamClient, err := c.newBatchStream(forwardedHost)
	if err != nil {
		return err
	}
	if forwardedHost == "" {
		c.client = streamClient
	} else {
		c.forwardedClients[forwardedHost] = streamClient
	}
	go c.batchRecvLoop(c.tikvClientCfg, c.tikvLoad, streamClient)
	return nil
}

func sendBatchRequest(
	ctx context.Context,
	addr string,
	forwardedHost string,
	batchConn *batchConn,
	batchRequestMetrics *batchRequestStageMetrics,
	req *tikvpb.BatchCommandsRequest_Request,
	timeout time.Duration,
	priority uint64,
) (_ *tikvrpc.Response, err error) {
	if err := encodeRequestCmd(req); err != nil {
		return nil, err
	}
	entry := &batchCommandsEntry{
		ctx:                 ctx,
		req:                 req,
		res:                 make(chan *tikvpb.BatchCommandsResponse_Response, 1),
		batchRequestMetrics: batchRequestMetrics,
		forwardedHost:       forwardedHost,
		canceled:            0,
		err:                 nil,
		pri:                 priority,
		reqArriveAt:         time.Now(),
	}
	timer := time.NewTimer(timeout)
	defer func() {
		timer.Stop()
		observeBatchRequestCompletion(entry, err)
	}()

	select {
	case batchConn.batchCommandsCh <- entry:
	case <-ctx.Done():
		logutil.Logger(ctx).Debug("send request is cancelled",
			zap.String("to", addr), zap.String("cause", ctx.Err().Error()))
		return nil, errors.WithStack(ctx.Err())
	case <-batchConn.closed:
		logutil.Logger(ctx).Debug("send request is cancelled (batchConn closed)", zap.String("to", addr))
		return nil, errors.New("batchConn closed")
	case <-timer.C:
		return nil, errors.WithMessage(context.DeadlineExceeded, "wait sendLoop")
	}

	select {
	case res, ok := <-entry.res:
		if !ok {
			return nil, errors.WithStack(entry.err)
		}
		return tikvrpc.FromBatchCommandsResponse(res)
	case <-ctx.Done():
		atomic.StoreInt32(&entry.canceled, 1)
		logutil.Logger(ctx).Debug("wait response is cancelled",
			zap.String("to", addr), zap.String("cause", ctx.Err().Error()))
		return nil, errors.WithStack(ctx.Err())
	case <-batchConn.closed:
		atomic.StoreInt32(&entry.canceled, 1)
		logutil.Logger(ctx).Debug("wait response is cancelled (batchConn closed)", zap.String("to", addr))
		return nil, errors.New("batchConn closed")
	case <-timer.C:
		atomic.StoreInt32(&entry.canceled, 1)
		return nil, errors.WithMessage(context.DeadlineExceeded, formatBatchRequestTimeoutReason(entry, timeout, time.Now()))
	}
}
