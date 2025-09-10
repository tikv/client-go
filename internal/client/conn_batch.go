// Copyright 2025 TiKV Authors
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

package client

import (
	"runtime"
	"runtime/trace"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

type batchConn struct {
	// An atomic flag indicates whether the batch is idle or not.
	// 0 for busy, others for idle.
	idle uint32

	// batchCommandsCh used for batch commands.
	batchCommandsCh        chan *batchCommandsEntry
	batchCommandsClients   []*batchCommandsClient
	tikvTransportLayerLoad uint64
	closed                 chan struct{}

	reqBuilder *batchCommandsBuilder

	// Notify rpcClient to check the idle flag
	idleNotify *uint32
	idleDetect *time.Timer

	fetchMoreTimer *time.Timer

	index uint32

	metrics batchConnMetrics
}

func newBatchConn(connCount, maxBatchSize uint, idleNotify *uint32) *batchConn {
	return &batchConn{
		batchCommandsCh:        make(chan *batchCommandsEntry, maxBatchSize),
		batchCommandsClients:   make([]*batchCommandsClient, 0, connCount),
		tikvTransportLayerLoad: 0,
		closed:                 make(chan struct{}),
		reqBuilder:             newBatchCommandsBuilder(maxBatchSize),
		idleNotify:             idleNotify,
		idleDetect:             time.NewTimer(idleTimeout),
	}
}

func (a *batchConn) initMetrics(target string) {
	a.metrics.pendingRequests = metrics.TiKVBatchPendingRequests.WithLabelValues(target)
	a.metrics.batchSize = metrics.TiKVBatchRequests.WithLabelValues(target)
	a.metrics.sendLoopWaitHeadDur = metrics.TiKVBatchSendLoopDuration.WithLabelValues(target, "wait-head")
	a.metrics.sendLoopWaitMoreDur = metrics.TiKVBatchSendLoopDuration.WithLabelValues(target, "wait-more")
	a.metrics.sendLoopSendDur = metrics.TiKVBatchSendLoopDuration.WithLabelValues(target, "send")
	a.metrics.recvLoopRecvDur = metrics.TiKVBatchRecvLoopDuration.WithLabelValues(target, "recv")
	a.metrics.recvLoopProcessDur = metrics.TiKVBatchRecvLoopDuration.WithLabelValues(target, "process")
	a.metrics.batchSendTailLat = metrics.TiKVBatchSendTailLatency.WithLabelValues(target)
	a.metrics.batchRecvTailLat = metrics.TiKVBatchRecvTailLatency.WithLabelValues(target)
	a.metrics.headArrivalInterval = metrics.TiKVBatchHeadArrivalInterval.WithLabelValues(target)
	a.metrics.batchMoreRequests = metrics.TiKVBatchMoreRequests.WithLabelValues(target)
	a.metrics.bestBatchSize = metrics.TiKVBatchBestSize.WithLabelValues(target)
}

func (a *batchConn) isIdle() bool {
	return atomic.LoadUint32(&a.idle) != 0
}

// fetchAllPendingRequests fetches all pending requests from the channel.
func (a *batchConn) fetchAllPendingRequests(maxBatchSize int) (headRecvTime time.Time, headArrivalInterval time.Duration) {
	// Block on the first element.
	latestReqStartTime := a.reqBuilder.latestReqStartTime
	var headEntry *batchCommandsEntry
	select {
	case headEntry = <-a.batchCommandsCh:
		if !a.idleDetect.Stop() {
			<-a.idleDetect.C
		}
		a.idleDetect.Reset(idleTimeout)
	case <-a.idleDetect.C:
		a.idleDetect.Reset(idleTimeout)
		atomic.AddUint32(&a.idle, 1)
		atomic.CompareAndSwapUint32(a.idleNotify, 0, 1)
		// This batchConn to be recycled
		return time.Now(), 0
	case <-a.closed:
		return time.Now(), 0
	}
	if headEntry == nil {
		return time.Now(), 0
	}
	headRecvTime = time.Now()
	if headEntry.start.After(latestReqStartTime) && !latestReqStartTime.IsZero() {
		headArrivalInterval = headEntry.start.Sub(latestReqStartTime)
	}
	a.reqBuilder.push(headEntry)

	// This loop is for trying best to collect more requests.
	for a.reqBuilder.len() < maxBatchSize {
		select {
		case entry := <-a.batchCommandsCh:
			if entry == nil {
				return
			}
			a.reqBuilder.push(entry)
		default:
			return
		}
	}
	return
}

// fetchMorePendingRequests fetches more pending requests from the channel.
func (a *batchConn) fetchMorePendingRequests(
	maxBatchSize int,
	batchWaitSize int,
	maxWaitTime time.Duration,
) {
	// Try to collect `batchWaitSize` requests, or wait `maxWaitTime`.
	if a.fetchMoreTimer == nil {
		a.fetchMoreTimer = time.NewTimer(maxWaitTime)
	} else {
		a.fetchMoreTimer.Reset(maxWaitTime)
	}
	for a.reqBuilder.len() < batchWaitSize {
		select {
		case entry := <-a.batchCommandsCh:
			if entry == nil {
				if !a.fetchMoreTimer.Stop() {
					<-a.fetchMoreTimer.C
				}
				return
			}
			a.reqBuilder.push(entry)
		case <-a.fetchMoreTimer.C:
			return
		}
	}
	if !a.fetchMoreTimer.Stop() {
		<-a.fetchMoreTimer.C
	}

	// Do an additional non-block try. Here we test the length with `maxBatchSize` instead
	// of `batchWaitSize` because trying best to fetch more requests is necessary so that
	// we can adjust the `batchWaitSize` dynamically.
	yielded := false
	for a.reqBuilder.len() < maxBatchSize {
		select {
		case entry := <-a.batchCommandsCh:
			if entry == nil {
				return
			}
			a.reqBuilder.push(entry)
		default:
			if yielded {
				return
			}
			// yield once to batch more requests.
			runtime.Gosched()
			yielded = true
		}
	}
}

const idleTimeout = 3 * time.Minute

// BatchSendLoopPanicCounter is only used for testing.
var BatchSendLoopPanicCounter int64 = 0

var initBatchPolicyWarn sync.Once

func (a *batchConn) batchSendLoop(cfg config.TiKVClient) {
	defer func() {
		if r := recover(); r != nil {
			metrics.TiKVPanicCounter.WithLabelValues(metrics.LabelBatchSendLoop).Inc()
			logutil.BgLogger().Error("batchSendLoop",
				zap.Any("r", r),
				zap.Stack("stack"))
			atomic.AddInt64(&BatchSendLoopPanicCounter, 1)
			logutil.BgLogger().Info("restart batchSendLoop", zap.Int64("count", atomic.LoadInt64(&BatchSendLoopPanicCounter)))
			go a.batchSendLoop(cfg)
		}
	}()

	trigger, ok := newTurboBatchTriggerFromPolicy(cfg.BatchPolicy)
	if !ok {
		initBatchPolicyWarn.Do(func() {
			logutil.BgLogger().Warn("fallback to default batch policy due to invalid value", zap.String("value", cfg.BatchPolicy))
		})
	}
	turboBatchWaitTime := trigger.turboWaitTime()

	avgBatchWaitSize := float64(cfg.BatchWaitSize)
	for {
		sendLoopStartTime := time.Now()
		a.reqBuilder.reset()

		headRecvTime, headArrivalInterval := a.fetchAllPendingRequests(int(cfg.MaxBatchSize))
		if a.reqBuilder.len() == 0 {
			// the conn is closed or recycled.
			return
		}

		// curl -X PUT -d 'return(true)' http://0.0.0.0:10080/fail/tikvclient/mockBlockOnBatchClient
		if val, err := util.EvalFailpoint("mockBlockOnBatchClient"); err == nil {
			if val.(bool) {
				time.Sleep(1 * time.Hour)
			}
		}

		if batchSize := a.reqBuilder.len(); batchSize < int(cfg.MaxBatchSize) {
			if cfg.MaxBatchWaitTime > 0 && atomic.LoadUint64(&a.tikvTransportLayerLoad) > uint64(cfg.OverloadThreshold) {
				// If the target TiKV is overload, wait a while to collect more requests.
				metrics.TiKVBatchWaitOverLoad.Inc()
				a.fetchMorePendingRequests(int(cfg.MaxBatchSize), int(cfg.BatchWaitSize), cfg.MaxBatchWaitTime)
			} else if turboBatchWaitTime > 0 && headArrivalInterval > 0 && trigger.needFetchMore(headArrivalInterval) {
				batchWaitSize := trigger.preferredBatchWaitSize(avgBatchWaitSize, int(cfg.BatchWaitSize))
				a.fetchMorePendingRequests(int(cfg.MaxBatchSize), batchWaitSize, turboBatchWaitTime)
				a.metrics.batchMoreRequests.Observe(float64(a.reqBuilder.len() - batchSize))
			}
		}
		length := a.reqBuilder.len()
		avgBatchWaitSize = 0.2*float64(length) + 0.8*avgBatchWaitSize
		a.metrics.pendingRequests.Observe(float64(len(a.batchCommandsCh) + length))
		a.metrics.bestBatchSize.Observe(avgBatchWaitSize)
		a.metrics.headArrivalInterval.Observe(headArrivalInterval.Seconds())
		a.metrics.sendLoopWaitHeadDur.Observe(headRecvTime.Sub(sendLoopStartTime).Seconds())
		a.metrics.sendLoopWaitMoreDur.Observe(time.Since(sendLoopStartTime).Seconds())

		a.getClientAndSend()

		sendLoopEndTime := time.Now()
		a.metrics.sendLoopSendDur.Observe(sendLoopEndTime.Sub(sendLoopStartTime).Seconds())
		if dur := sendLoopEndTime.Sub(headRecvTime); dur > batchSendTailLatThreshold {
			a.metrics.batchSendTailLat.Observe(dur.Seconds())
		}
	}
}

const (
	SendFailedReasonNoAvailableLimit   = "concurrency limit exceeded"
	SendFailedReasonTryLockForSendFail = "tryLockForSend fail"
)

func (a *batchConn) getClientAndSend() {
	if val, err := util.EvalFailpoint("mockBatchClientSendDelay"); err == nil {
		if timeout, ok := val.(int); ok && timeout > 0 {
			time.Sleep(time.Duration(timeout * int(time.Millisecond)))
		}
	}

	// Choose a connection by round-robbin.
	var (
		cli    *batchCommandsClient
		target string
	)
	reasons := make([]string, 0)
	hasHighPriorityTask := a.reqBuilder.hasHighPriorityTask()
	for i := 0; i < len(a.batchCommandsClients); i++ {
		a.index = (a.index + 1) % uint32(len(a.batchCommandsClients))
		target = a.batchCommandsClients[a.index].target
		// The lock protects the batchCommandsClient from been closed while it's in use.
		c := a.batchCommandsClients[a.index]
		if hasHighPriorityTask || c.available() > 0 {
			if c.tryLockForSend() {
				cli = c
				break
			} else {
				reasons = append(reasons, SendFailedReasonTryLockForSendFail)
			}
		} else {
			reasons = append(reasons, SendFailedReasonNoAvailableLimit)
		}
	}
	if cli == nil {
		logutil.BgLogger().Info("no available connections", zap.String("target", target), zap.Any("reasons", reasons))
		metrics.TiKVNoAvailableConnectionCounter.Inc()
		if config.GetGlobalConfig().TiKVClient.MaxConcurrencyRequestLimit == config.DefMaxConcurrencyRequestLimit {
			// Only cancel requests when MaxConcurrencyRequestLimit feature is not enabled, to be compatible with the behavior of older versions.
			// TODO: But when MaxConcurrencyRequestLimit feature is enabled, the requests won't be canceled and will wait until timeout.
			// This behavior may not be reasonable, as the timeout is usually 40s or 60s, which is too long to retry in time.
			a.reqBuilder.cancel(errors.New("no available connections"))
		}
		return
	}
	defer cli.unlockForSend()
	available := cli.available()
	reqSendTime := time.Now()
	batch := 0
	req, forwardingReqs := a.reqBuilder.buildWithLimit(available, func(id uint64, e *batchCommandsEntry) {
		cli.batched.Store(id, e)
		cli.sent.Add(1)
		atomic.StoreInt64(&e.sendLat, int64(reqSendTime.Sub(e.start)))
		if trace.IsEnabled() {
			trace.Log(e.ctx, "rpc", "send")
		}
	})
	if req != nil {
		batch += len(req.RequestIds)
		cli.send("", req)
	}
	for forwardedHost, req := range forwardingReqs {
		batch += len(req.RequestIds)
		cli.send(forwardedHost, req)
	}
	if batch > 0 {
		a.metrics.batchSize.Observe(float64(batch))
	}
}

func (a *batchConn) Close() {
	// Close all batchRecvLoop.
	for _, c := range a.batchCommandsClients {
		// After connections are closed, `batchRecvLoop`s will check the flag.
		atomic.StoreInt32(&c.closed, 1)
	}
	// Don't close(batchCommandsCh) because when Close() is called, someone maybe
	// calling SendRequest and writing batchCommandsCh, if we close it here the
	// writing goroutine will panic.
	close(a.closed)
}
