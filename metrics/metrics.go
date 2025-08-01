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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/metrics/metrics.go
//

// Copyright 2021 PingCAP, Inc.
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

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// Client metrics.
var (
	TiKVTxnCmdHistogram                            *prometheus.HistogramVec
	TiKVBackoffHistogram                           *prometheus.HistogramVec
	TiKVSendReqHistogram                           *prometheus.HistogramVec
	TiKVSendReqSummary                             *prometheus.SummaryVec
	TiKVRPCNetLatencyHistogram                     *prometheus.HistogramVec
	TiKVLockResolverCounter                        *prometheus.CounterVec
	TiKVRegionErrorCounter                         *prometheus.CounterVec
	TiKVRPCErrorCounter                            *prometheus.CounterVec
	TiKVTxnWriteKVCountHistogram                   *prometheus.HistogramVec
	TiKVTxnWriteSizeHistogram                      *prometheus.HistogramVec
	TiKVRawkvCmdHistogram                          *prometheus.HistogramVec
	TiKVRawkvSizeHistogram                         *prometheus.HistogramVec
	TiKVTxnRegionsNumHistogram                     *prometheus.HistogramVec
	TiKVLoadTxnSafePointCounter                    *prometheus.CounterVec
	TiKVSecondaryLockCleanupFailureCounter         *prometheus.CounterVec
	TiKVRegionCacheCounter                         *prometheus.CounterVec
	TiKVLoadRegionCounter                          *prometheus.CounterVec
	TiKVLoadRegionCacheHistogram                   *prometheus.HistogramVec
	TiKVLocalLatchWaitTimeHistogram                prometheus.Histogram
	TiKVStatusDuration                             *prometheus.HistogramVec
	TiKVStatusCounter                              *prometheus.CounterVec
	TiKVBatchSendTailLatency                       *prometheus.HistogramVec
	TiKVBatchSendLoopDuration                      *prometheus.SummaryVec
	TiKVBatchRecvTailLatency                       *prometheus.HistogramVec
	TiKVBatchRecvLoopDuration                      *prometheus.SummaryVec
	TiKVBatchHeadArrivalInterval                   *prometheus.SummaryVec
	TiKVBatchBestSize                              *prometheus.SummaryVec
	TiKVBatchMoreRequests                          *prometheus.SummaryVec
	TiKVBatchWaitOverLoad                          prometheus.Counter
	TiKVBatchPendingRequests                       *prometheus.HistogramVec
	TiKVBatchRequests                              *prometheus.HistogramVec
	TiKVBatchRequestDuration                       *prometheus.SummaryVec
	TiKVBatchClientUnavailable                     prometheus.Histogram
	TiKVBatchClientWaitEstablish                   prometheus.Histogram
	TiKVBatchClientRecycle                         prometheus.Histogram
	TiKVRangeTaskStats                             *prometheus.GaugeVec
	TiKVRangeTaskPushDuration                      *prometheus.HistogramVec
	TiKVTokenWaitDuration                          prometheus.Histogram
	TiKVTxnHeartBeatHistogram                      *prometheus.HistogramVec
	TiKVTTLManagerHistogram                        prometheus.Histogram
	TiKVPessimisticLockKeysDuration                prometheus.Histogram
	TiKVTTLLifeTimeReachCounter                    prometheus.Counter
	TiKVNoAvailableConnectionCounter               prometheus.Counter
	TiKVTwoPCTxnCounter                            *prometheus.CounterVec
	TiKVAsyncCommitTxnCounter                      *prometheus.CounterVec
	TiKVOnePCTxnCounter                            *prometheus.CounterVec
	TiKVStoreLimitErrorCounter                     *prometheus.CounterVec
	TiKVGRPCConnTransientFailureCounter            *prometheus.CounterVec
	TiKVPanicCounter                               *prometheus.CounterVec
	TiKVForwardRequestCounter                      *prometheus.CounterVec
	TiKVTSFutureWaitDuration                       prometheus.Histogram
	TiKVSafeTSUpdateCounter                        *prometheus.CounterVec
	TiKVMinSafeTSGapSeconds                        *prometheus.GaugeVec
	TiKVReplicaSelectorFailureCounter              *prometheus.CounterVec
	TiKVRequestRetryTimesHistogram                 prometheus.Histogram
	TiKVTxnCommitBackoffSeconds                    prometheus.Histogram
	TiKVTxnCommitBackoffCount                      prometheus.Histogram
	TiKVSmallReadDuration                          prometheus.Histogram
	TiKVReadThroughput                             prometheus.Histogram
	TiKVUnsafeDestroyRangeFailuresCounterVec       *prometheus.CounterVec
	TiKVPrewriteAssertionUsageCounter              *prometheus.CounterVec
	TiKVGrpcConnectionState                        *prometheus.GaugeVec
	TiKVAggressiveLockedKeysCounter                *prometheus.CounterVec
	TiKVStoreSlowScoreGauge                        *prometheus.GaugeVec
	TiKVFeedbackSlowScoreGauge                     *prometheus.GaugeVec
	TiKVHealthFeedbackOpsCounter                   *prometheus.CounterVec
	TiKVPreferLeaderFlowsGauge                     *prometheus.GaugeVec
	TiKVStaleReadCounter                           *prometheus.CounterVec
	TiKVStaleReadReqCounter                        *prometheus.CounterVec
	TiKVStaleReadBytes                             *prometheus.CounterVec
	TiKVPipelinedFlushLenHistogram                 prometheus.Histogram
	TiKVPipelinedFlushSizeHistogram                prometheus.Histogram
	TiKVPipelinedFlushDuration                     prometheus.Histogram
	TiKVValidateReadTSFromPDCount                  prometheus.Counter
	TiKVLowResolutionTSOUpdateIntervalSecondsGauge prometheus.Gauge
	TiKVStaleRegionFromPDCounter                   prometheus.Counter
	TiKVPipelinedFlushThrottleSecondsHistogram     prometheus.Histogram
	TiKVTxnWriteConflictCounter                    prometheus.Counter
	TiKVAsyncSendReqCounter                        *prometheus.CounterVec
	TiKVAsyncBatchGetCounter                       *prometheus.CounterVec
)

// Label constants.
const (
	LblType            = "type"
	LblResult          = "result"
	LblStore           = "store"
	LblCommit          = "commit"
	LblAbort           = "abort"
	LblRollback        = "rollback"
	LblBatchGet        = "batch_get"
	LblGet             = "get"
	LblLockKeys        = "lock_keys"
	LabelBatchRecvLoop = "batch-recv-loop"
	LabelBatchSendLoop = "batch-send-loop"
	LblAddress         = "address"
	LblFromStore       = "from_store"
	LblToStore         = "to_store"
	LblStaleRead       = "stale_read"
	LblSource          = "source"
	LblScope           = "scope"
	LblInternal        = "internal"
	LblGeneral         = "general"
	LblDirection       = "direction"
	LblReason          = "reason"
)

func initMetrics(namespace, subsystem string, constLabels prometheus.Labels) {
	TiKVTxnCmdHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "txn_cmd_duration_seconds",
			Help:        "Bucketed histogram of processing time of txn cmds.",
			Buckets:     prometheus.ExponentialBuckets(0.0005, 2, 29), // 0.5ms ~ 1.5days
			ConstLabels: constLabels,
		}, []string{LblType, LblScope})

	TiKVBackoffHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "backoff_seconds",
			Help:        "total backoff seconds of a single backoffer.",
			Buckets:     prometheus.ExponentialBuckets(0.0005, 2, 29), // 0.5ms ~ 1.5days
			ConstLabels: constLabels,
		}, []string{LblType})

	TiKVSendReqHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "request_seconds",
			Help:        "Bucketed histogram of sending request duration.",
			Buckets:     prometheus.ExponentialBuckets(0.0005, 2, 24), // 0.5ms ~ 1.2h
			ConstLabels: constLabels,
		}, []string{LblType, LblStore, LblStaleRead, LblScope})

	TiKVSendReqSummary = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "source_request_seconds",
			Help:        "Summary of sending request with multi dimensions.",
			ConstLabels: constLabels,
		}, []string{LblType, LblStore, LblStaleRead, LblScope, LblSource})

	TiKVRPCNetLatencyHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "rpc_net_latency_seconds",
			Help:        "Bucketed histogram of time difference between TiDB and TiKV.",
			Buckets:     prometheus.ExponentialBuckets(0.0001, 2, 20), // 0.1ms ~ 52s
			ConstLabels: constLabels,
		}, []string{LblStore, LblScope})

	TiKVLockResolverCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "lock_resolver_actions_total",
			Help:        "Counter of lock resolver actions.",
			ConstLabels: constLabels,
		}, []string{LblType})

	TiKVRegionErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "region_err_total",
			Help:        "Counter of region errors.",
			ConstLabels: constLabels,
		}, []string{LblType, LblStore})

	TiKVRPCErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "rpc_err_total",
			Help:        "Counter of rpc errors.",
			ConstLabels: constLabels,
		}, []string{LblType, LblStore})

	TiKVTxnWriteKVCountHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "txn_write_kv_num",
			Help:        "Count of kv pairs to write in a transaction.",
			Buckets:     prometheus.ExponentialBuckets(1, 4, 17), // 1 ~ 4G
			ConstLabels: constLabels,
		}, []string{LblScope})

	TiKVTxnWriteSizeHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "txn_write_size_bytes",
			Help:        "Size of kv pairs to write in a transaction.",
			Buckets:     prometheus.ExponentialBuckets(16, 4, 17), // 16Bytes ~ 64GB
			ConstLabels: constLabels,
		}, []string{LblScope})

	TiKVRawkvCmdHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "rawkv_cmd_seconds",
			Help:        "Bucketed histogram of processing time of rawkv cmds.",
			Buckets:     prometheus.ExponentialBuckets(0.0005, 2, 29), // 0.5ms ~ 1.5days
			ConstLabels: constLabels,
		}, []string{LblType})

	TiKVRawkvSizeHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "rawkv_kv_size_bytes",
			Help:        "Size of key/value to put, in bytes.",
			Buckets:     prometheus.ExponentialBuckets(1, 2, 30), // 1Byte ~ 512MB
			ConstLabels: constLabels,
		}, []string{LblType})

	TiKVTxnRegionsNumHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "txn_regions_num",
			Help:        "Number of regions in a transaction.",
			Buckets:     prometheus.ExponentialBuckets(1, 2, 25), // 1 ~ 16M
			ConstLabels: constLabels,
		}, []string{LblType, LblScope})

	TiKVLoadTxnSafePointCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "load_safepoint_total",
			Help:        "Counter of load safepoint.",
			ConstLabels: constLabels,
		}, []string{LblType})

	TiKVSecondaryLockCleanupFailureCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "lock_cleanup_task_total",
			Help:        "failure statistic of secondary lock cleanup task.",
			ConstLabels: constLabels,
		}, []string{LblType})

	TiKVRegionCacheCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "region_cache_operations_total",
			Help:        "Counter of region cache.",
			ConstLabels: constLabels,
		}, []string{LblType, LblResult})

	TiKVLoadRegionCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   namespace,
		Subsystem:   subsystem,
		Name:        "load_region_total",
		Help:        "Counter of loading region.",
		ConstLabels: constLabels,
	}, []string{LblType, LblReason})

	TiKVLoadRegionCacheHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "load_region_cache_seconds",
			Help:        "Load region information duration",
			Buckets:     prometheus.ExponentialBuckets(0.0001, 2, 20), // 0.1ms ~ 52s
			ConstLabels: constLabels,
		}, []string{LblType})

	TiKVLocalLatchWaitTimeHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "local_latch_wait_seconds",
			Help:        "Wait time of a get local latch.",
			Buckets:     prometheus.ExponentialBuckets(0.0005, 2, 20), // 0.5ms ~ 262s
			ConstLabels: constLabels,
		})

	TiKVStatusDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "kv_status_api_duration",
			Help:        "duration for kv status api.",
			Buckets:     prometheus.ExponentialBuckets(0.0005, 2, 20), // 0.5ms ~ 262s
			ConstLabels: constLabels,
		}, []string{LblStore})

	TiKVStatusCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "kv_status_api_count",
			Help:        "Counter of access kv status api.",
			ConstLabels: constLabels,
		}, []string{LblResult})

	TiKVBatchSendTailLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "batch_send_tail_latency_seconds",
			Buckets:     prometheus.ExponentialBuckets(0.02, 2, 8), // 20ms ~ 2.56s
			Help:        "batch send tail latency",
			ConstLabels: constLabels,
		}, []string{LblStore})

	TiKVBatchSendLoopDuration = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "batch_send_loop_duration_seconds",
			Help:        "batch send loop duration breakdown by steps",
			ConstLabels: constLabels,
		}, []string{LblStore, "step"})

	TiKVBatchRecvTailLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "batch_recv_tail_latency_seconds",
			Buckets:     prometheus.ExponentialBuckets(0.02, 2, 8), // 20ms ~ 2.56s
			Help:        "batch recv tail latency",
			ConstLabels: constLabels,
		}, []string{LblStore})

	TiKVBatchRecvLoopDuration = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "batch_recv_loop_duration_seconds",
			Help:        "batch recv loop duration breakdown by steps",
			ConstLabels: constLabels,
		}, []string{LblStore, "step"})

	TiKVBatchHeadArrivalInterval = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "batch_head_arrival_interval_seconds",
			Help:        "arrival interval of the head request in batch",
			ConstLabels: constLabels,
		}, []string{LblStore})

	TiKVBatchBestSize = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "batch_best_size",
			Help:        "best batch size estimated by the batch client",
			ConstLabels: constLabels,
		}, []string{LblStore})

	TiKVBatchMoreRequests = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "batch_more_requests_total",
			Help:        "number of requests batched by extra fetch",
			ConstLabels: constLabels,
		}, []string{LblStore})

	TiKVBatchWaitOverLoad = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "batch_wait_overload",
			Help:        "event of tikv transport layer overload",
			ConstLabels: constLabels,
		})

	TiKVBatchPendingRequests = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "batch_pending_requests",
			Buckets:     prometheus.ExponentialBuckets(1, 2, 11), // 1 ~ 1024
			Help:        "number of requests pending in the batch channel",
			ConstLabels: constLabels,
		}, []string{LblStore})

	TiKVBatchRequests = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "batch_requests",
			Buckets:     prometheus.ExponentialBuckets(1, 2, 11), // 1 ~ 1024
			Help:        "number of requests in one batch",
			ConstLabels: constLabels,
		}, []string{LblStore})

	TiKVBatchRequestDuration = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "batch_request_duration_seconds",
			Help:        "batch request duration breakdown by steps",
			ConstLabels: constLabels,
		}, []string{"step"})

	TiKVBatchClientUnavailable = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "batch_client_unavailable_seconds",
			Buckets:     prometheus.ExponentialBuckets(0.001, 2, 28), // 1ms ~ 1.5days
			Help:        "batch client unavailable",
			ConstLabels: constLabels,
		})

	TiKVBatchClientWaitEstablish = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "batch_client_wait_connection_establish",
			Buckets:     prometheus.ExponentialBuckets(0.001, 2, 28), // 1ms ~ 1.5days
			Help:        "batch client wait new connection establish",
			ConstLabels: constLabels,
		})

	TiKVBatchClientRecycle = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "batch_client_reset",
			Buckets:     prometheus.ExponentialBuckets(0.001, 2, 28), // 1ms ~ 1.5days
			Help:        "batch client recycle connection and reconnect duration",
			ConstLabels: constLabels,
		})

	TiKVRangeTaskStats = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "range_task_stats",
			Help:        "stat of range tasks",
			ConstLabels: constLabels,
		}, []string{LblType, LblResult})

	TiKVRangeTaskPushDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "range_task_push_duration",
			Buckets:     prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms ~ 524s
			Help:        "duration to push sub tasks to range task workers",
			ConstLabels: constLabels,
		}, []string{LblType})

	TiKVTokenWaitDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "batch_executor_token_wait_duration",
			Buckets:     prometheus.ExponentialBuckets(1, 2, 34), // 1ns ~ 8s
			Help:        "tidb txn token wait duration to process batches",
			ConstLabels: constLabels,
		})

	TiKVTxnHeartBeatHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "txn_heart_beat",
			Help:        "Bucketed histogram of the txn_heartbeat request duration.",
			ConstLabels: constLabels,
			Buckets:     prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms ~ 524s
		}, []string{LblType})

	TiKVTTLManagerHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "txn_ttl_manager",
			Help:        "Bucketed histogram of the txn ttl manager lifetime duration.",
			ConstLabels: constLabels,
			Buckets:     prometheus.ExponentialBuckets(1, 2, 20), // 1s ~ 524288s
		})

	TiKVTTLLifeTimeReachCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "ttl_lifetime_reach_total",
			Help:        "Counter of ttlManager live too long.",
			ConstLabels: constLabels,
		})

	TiKVNoAvailableConnectionCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "batch_client_no_available_connection_total",
			Help:        "Counter of no available batch client.",
			ConstLabels: constLabels,
		})

	TiKVTwoPCTxnCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "commit_txn_counter",
			Help:        "Counter of 2PC transactions.",
			ConstLabels: constLabels,
		}, []string{LblType})

	TiKVAsyncCommitTxnCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "async_commit_txn_counter",
			Help:        "Counter of async commit transactions.",
			ConstLabels: constLabels,
		}, []string{LblType})

	TiKVOnePCTxnCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "one_pc_txn_counter",
			Help:        "Counter of 1PC transactions.",
			ConstLabels: constLabels,
		}, []string{LblType})

	TiKVStoreLimitErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "get_store_limit_token_error",
			Help:        "store token is up to the limit, probably because one of the stores is the hotspot or unavailable",
			ConstLabels: constLabels,
		}, []string{LblAddress, LblStore})

	TiKVGRPCConnTransientFailureCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "connection_transient_failure_count",
			Help:        "Counter of gRPC connection transient failure",
			ConstLabels: constLabels,
		}, []string{LblAddress, LblStore})

	TiKVPanicCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "panic_total",
			Help:        "Counter of panic.",
			ConstLabels: constLabels,
		}, []string{LblType})

	TiKVForwardRequestCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "forward_request_counter",
			Help:        "Counter of tikv request being forwarded through another node",
			ConstLabels: constLabels,
		}, []string{LblFromStore, LblToStore, LblType, LblResult})

	TiKVTSFutureWaitDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "ts_future_wait_seconds",
			Help:        "Bucketed histogram of seconds cost for waiting timestamp future.",
			ConstLabels: constLabels,
			Buckets:     prometheus.ExponentialBuckets(0.000005, 2, 30), // 5us ~ 2560s
		})

	TiKVSafeTSUpdateCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "safets_update_counter",
			Help:        "Counter of tikv safe_ts being updated.",
			ConstLabels: constLabels,
		}, []string{LblResult, LblStore})

	TiKVMinSafeTSGapSeconds = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "min_safets_gap_seconds",
			Help:        "The minimal (non-zero) SafeTS gap for each store.",
			ConstLabels: constLabels,
		}, []string{LblStore})

	TiKVReplicaSelectorFailureCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "replica_selector_failure_counter",
			Help:        "Counter of the reason why the replica selector cannot yield a potential leader.",
			ConstLabels: constLabels,
		}, []string{LblType})

	TiKVRequestRetryTimesHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "request_retry_times",
			Help:        "Bucketed histogram of how many times a region request retries.",
			ConstLabels: constLabels,
			Buckets:     []float64{1, 2, 3, 4, 8, 16, 32, 64, 128, 256},
		})
	TiKVTxnCommitBackoffSeconds = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "txn_commit_backoff_seconds",
			Help:        "Bucketed histogram of the total backoff duration in committing a transaction.",
			ConstLabels: constLabels,
			Buckets:     prometheus.ExponentialBuckets(0.001, 2, 22), // 1ms ~ 2097s
		})
	TiKVTxnCommitBackoffCount = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "txn_commit_backoff_count",
			Help:        "Bucketed histogram of the backoff count in committing a transaction.",
			ConstLabels: constLabels,
			Buckets:     prometheus.ExponentialBuckets(1, 2, 12), // 1 ~ 2048
		})

	// TiKVSmallReadDuration uses to collect small request read duration.
	TiKVSmallReadDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   "sli", // Always use "sli" to make it compatible with TiDB.
			Name:        "tikv_small_read_duration",
			Help:        "Read time of TiKV small read.",
			ConstLabels: constLabels,
			Buckets:     prometheus.ExponentialBuckets(0.0005, 2, 28), // 0.5ms ~ 74h
		})

	TiKVReadThroughput = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   "sli",
			Name:        "tikv_read_throughput",
			Help:        "Read throughput of TiKV read in Bytes/s.",
			ConstLabels: constLabels,
			Buckets:     prometheus.ExponentialBuckets(1024, 2, 13), // 1MB/s ~ 4GB/s
		})

	TiKVUnsafeDestroyRangeFailuresCounterVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "gc_unsafe_destroy_range_failures",
			Help:        "Counter of unsafe destroyrange failures",
			ConstLabels: constLabels,
		}, []string{LblType})

	TiKVPrewriteAssertionUsageCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "prewrite_assertion_count",
			Help:        "Counter of assertions used in prewrite requests",
			ConstLabels: constLabels,
		}, []string{LblType})

	TiKVGrpcConnectionState = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "grpc_connection_state",
			Help:        "State of gRPC connection",
			ConstLabels: constLabels,
		}, []string{"connection_id", "store_ip", "grpc_state"})

	TiKVAggressiveLockedKeysCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "aggressive_locking_count",
			Help:        "Counter of keys locked in aggressive locking mode",
			ConstLabels: constLabels,
		}, []string{LblType})

	TiKVStoreSlowScoreGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "store_slow_score",
			Help:        "Slow scores of each tikv node based on RPC timecosts",
			ConstLabels: constLabels,
		}, []string{LblStore})

	TiKVFeedbackSlowScoreGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "feedback_slow_score",
			Help:        "Slow scores of each tikv node that is calculated by TiKV and sent to the client by health feedback",
			ConstLabels: constLabels,
		}, []string{LblStore})

	TiKVHealthFeedbackOpsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "health_feedback_ops_counter",
			Help:        "Counter of operations about TiKV health feedback",
			ConstLabels: constLabels,
		}, []string{LblScope, LblType})

	TiKVPreferLeaderFlowsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "prefer_leader_flows_gauge",
			Help:        "Counter of flows under PreferLeader mode.",
			ConstLabels: constLabels,
		}, []string{LblType, LblStore})

	TiKVStaleReadCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "stale_read_counter",
			Help:        "Counter of stale read hit/miss",
			ConstLabels: constLabels,
		}, []string{LblResult})

	TiKVStaleReadReqCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "stale_read_req_counter",
			Help:        "Counter of stale read requests",
			ConstLabels: constLabels,
		}, []string{LblType})

	TiKVStaleReadBytes = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "stale_read_bytes",
			Help:        "Counter of stale read requests bytes",
			ConstLabels: constLabels,
		}, []string{LblResult, LblDirection})

	TiKVPipelinedFlushLenHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "pipelined_flush_len",
			Help:        "Bucketed histogram of length of pipelined flushed memdb",
			ConstLabels: constLabels,
			Buckets:     prometheus.ExponentialBuckets(1000, 2, 16), // 1K ~ 32M
		})

	TiKVPipelinedFlushSizeHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "pipelined_flush_size",
			Help:        "Bucketed histogram of size of pipelined flushed memdb",
			ConstLabels: constLabels,
			Buckets:     prometheus.ExponentialBuckets(16*1024*1024, 1.2, 13), // 16M ~ 142M
		})

	TiKVPipelinedFlushDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "pipelined_flush_duration",
			Help:        "Flush time of pipelined memdb.",
			ConstLabels: constLabels,
			Buckets:     prometheus.ExponentialBuckets(0.0005, 2, 28), // 0.5ms ~ 18h
		})

	TiKVValidateReadTSFromPDCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "validate_read_ts_from_pd_count",
			Help:        "Counter of validating read ts by getting a timestamp from PD",
			ConstLabels: constLabels,
		})

	TiKVLowResolutionTSOUpdateIntervalSecondsGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "low_resolution_tso_update_interval_seconds",
			Help:        "The actual working update interval for the low resolution TSO. As there are adaptive mechanism internally, this value may differ from the config.",
			ConstLabels: constLabels,
		})
	TiKVStaleRegionFromPDCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "stale_region_from_pd",
			Help:        "Counter of stale region from PD",
			ConstLabels: constLabels,
		})
	TiKVPipelinedFlushThrottleSecondsHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "pipelined_flush_throttle_seconds",
			Help:        "Throttle durations of pipelined flushes.",
			ConstLabels: constLabels,
			Buckets:     prometheus.ExponentialBuckets(0.0005, 2, 28), // 0.5ms ~ 18h
		})

	TiKVTxnWriteConflictCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "txn_write_conflict_counter",
			Help:        "Counter of txn write conflict",
			ConstLabels: constLabels,
		})

	TiKVAsyncSendReqCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "async_send_req_total",
			Help:        "Counter of async send req by region request sender.",
			ConstLabels: constLabels,
		}, []string{LblResult})

	TiKVAsyncBatchGetCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "async_batch_get_total",
			Help:        "Counter of async batch get by txn snapshot.",
			ConstLabels: constLabels,
		}, []string{LblResult})

	initShortcuts()
}

func init() {
	initMetrics("tikv", "client_go", nil)
}

// InitMetrics initializes metrics variables with given namespace and subsystem name.
func InitMetrics(namespace, subsystem string) {
	initMetrics(namespace, subsystem, nil)
}

// InitMetricsWithConstLabels initializes metrics variables with given namespace, subsystem name and const labels.
func InitMetricsWithConstLabels(namespace, subsystem string, constLabels prometheus.Labels) {
	initMetrics(namespace, subsystem, constLabels)
}

// RegisterMetrics registers all metrics variables.
// Note: to change default namespace and subsystem name, call `InitMetrics` before registering.
func RegisterMetrics() {
	prometheus.MustRegister(TiKVTxnCmdHistogram)
	prometheus.MustRegister(TiKVBackoffHistogram)
	prometheus.MustRegister(TiKVSendReqHistogram)
	prometheus.MustRegister(TiKVSendReqSummary)
	prometheus.MustRegister(TiKVRPCNetLatencyHistogram)
	prometheus.MustRegister(TiKVLockResolverCounter)
	prometheus.MustRegister(TiKVRegionErrorCounter)
	prometheus.MustRegister(TiKVRPCErrorCounter)
	prometheus.MustRegister(TiKVTxnWriteKVCountHistogram)
	prometheus.MustRegister(TiKVTxnWriteSizeHistogram)
	prometheus.MustRegister(TiKVRawkvCmdHistogram)
	prometheus.MustRegister(TiKVRawkvSizeHistogram)
	prometheus.MustRegister(TiKVTxnRegionsNumHistogram)
	prometheus.MustRegister(TiKVLoadTxnSafePointCounter)
	prometheus.MustRegister(TiKVSecondaryLockCleanupFailureCounter)
	prometheus.MustRegister(TiKVRegionCacheCounter)
	prometheus.MustRegister(TiKVLoadRegionCounter)
	prometheus.MustRegister(TiKVLoadRegionCacheHistogram)
	prometheus.MustRegister(TiKVLocalLatchWaitTimeHistogram)
	prometheus.MustRegister(TiKVStatusDuration)
	prometheus.MustRegister(TiKVStatusCounter)
	prometheus.MustRegister(TiKVBatchSendTailLatency)
	prometheus.MustRegister(TiKVBatchRecvTailLatency)
	prometheus.MustRegister(TiKVBatchSendLoopDuration)
	prometheus.MustRegister(TiKVBatchRecvLoopDuration)
	prometheus.MustRegister(TiKVBatchHeadArrivalInterval)
	prometheus.MustRegister(TiKVBatchBestSize)
	prometheus.MustRegister(TiKVBatchMoreRequests)
	prometheus.MustRegister(TiKVBatchWaitOverLoad)
	prometheus.MustRegister(TiKVBatchPendingRequests)
	prometheus.MustRegister(TiKVBatchRequests)
	prometheus.MustRegister(TiKVBatchRequestDuration)
	prometheus.MustRegister(TiKVBatchClientUnavailable)
	prometheus.MustRegister(TiKVBatchClientWaitEstablish)
	prometheus.MustRegister(TiKVBatchClientRecycle)
	prometheus.MustRegister(TiKVRangeTaskStats)
	prometheus.MustRegister(TiKVRangeTaskPushDuration)
	prometheus.MustRegister(TiKVTokenWaitDuration)
	prometheus.MustRegister(TiKVTxnHeartBeatHistogram)
	prometheus.MustRegister(TiKVTTLManagerHistogram)
	prometheus.MustRegister(TiKVTTLLifeTimeReachCounter)
	prometheus.MustRegister(TiKVNoAvailableConnectionCounter)
	prometheus.MustRegister(TiKVTwoPCTxnCounter)
	prometheus.MustRegister(TiKVAsyncCommitTxnCounter)
	prometheus.MustRegister(TiKVOnePCTxnCounter)
	prometheus.MustRegister(TiKVStoreLimitErrorCounter)
	prometheus.MustRegister(TiKVGRPCConnTransientFailureCounter)
	prometheus.MustRegister(TiKVPanicCounter)
	prometheus.MustRegister(TiKVForwardRequestCounter)
	prometheus.MustRegister(TiKVTSFutureWaitDuration)
	prometheus.MustRegister(TiKVSafeTSUpdateCounter)
	prometheus.MustRegister(TiKVMinSafeTSGapSeconds)
	prometheus.MustRegister(TiKVReplicaSelectorFailureCounter)
	prometheus.MustRegister(TiKVRequestRetryTimesHistogram)
	prometheus.MustRegister(TiKVTxnCommitBackoffSeconds)
	prometheus.MustRegister(TiKVTxnCommitBackoffCount)
	prometheus.MustRegister(TiKVSmallReadDuration)
	prometheus.MustRegister(TiKVReadThroughput)
	prometheus.MustRegister(TiKVUnsafeDestroyRangeFailuresCounterVec)
	prometheus.MustRegister(TiKVPrewriteAssertionUsageCounter)
	prometheus.MustRegister(TiKVGrpcConnectionState)
	prometheus.MustRegister(TiKVAggressiveLockedKeysCounter)
	prometheus.MustRegister(TiKVStoreSlowScoreGauge)
	prometheus.MustRegister(TiKVFeedbackSlowScoreGauge)
	prometheus.MustRegister(TiKVHealthFeedbackOpsCounter)
	prometheus.MustRegister(TiKVPreferLeaderFlowsGauge)
	prometheus.MustRegister(TiKVStaleReadCounter)
	prometheus.MustRegister(TiKVStaleReadReqCounter)
	prometheus.MustRegister(TiKVStaleReadBytes)
	prometheus.MustRegister(TiKVPipelinedFlushLenHistogram)
	prometheus.MustRegister(TiKVPipelinedFlushSizeHistogram)
	prometheus.MustRegister(TiKVPipelinedFlushDuration)
	prometheus.MustRegister(TiKVValidateReadTSFromPDCount)
	prometheus.MustRegister(TiKVLowResolutionTSOUpdateIntervalSecondsGauge)
	prometheus.MustRegister(TiKVStaleRegionFromPDCounter)
	prometheus.MustRegister(TiKVPipelinedFlushThrottleSecondsHistogram)
	prometheus.MustRegister(TiKVTxnWriteConflictCounter)
	prometheus.MustRegister(TiKVAsyncSendReqCounter)
	prometheus.MustRegister(TiKVAsyncBatchGetCounter)
}

// readCounter reads the value of a prometheus.Counter.
// Returns -1 when failing to read the value.
func readCounter(m prometheus.Counter) int64 {
	// Actually, it's not recommended to read the value of prometheus metric types directly:
	// https://github.com/prometheus/client_golang/issues/486#issuecomment-433345239
	pb := &dto.Metric{}
	// It's impossible to return an error though.
	if err := m.Write(pb); err != nil {
		return -1
	}
	return int64(pb.GetCounter().GetValue())
}

// TxnCommitCounter is the counter of transactions committed with
// different protocols, i.e. 2PC, async-commit, 1PC.
type TxnCommitCounter struct {
	TwoPC       int64 `json:"twoPC"`
	AsyncCommit int64 `json:"asyncCommit"`
	OnePC       int64 `json:"onePC"`
}

// Sub returns the difference of two counters.
func (c TxnCommitCounter) Sub(rhs TxnCommitCounter) TxnCommitCounter {
	new := TxnCommitCounter{}
	new.TwoPC = c.TwoPC - rhs.TwoPC
	new.AsyncCommit = c.AsyncCommit - rhs.AsyncCommit
	new.OnePC = c.OnePC - rhs.OnePC
	return new
}

// GetTxnCommitCounter gets the TxnCommitCounter.
func GetTxnCommitCounter() TxnCommitCounter {
	return TxnCommitCounter{
		TwoPC:       readCounter(TwoPCTxnCounterOk),
		AsyncCommit: readCounter(AsyncCommitTxnCounterOk),
		OnePC:       readCounter(OnePCTxnCounterOk),
	}
}

const (
	smallTxnReadRow  = 20
	smallTxnReadSize = 1 * 1024 * 1024 //1MB
)

// ObserveReadSLI observes the read SLI metric.
func ObserveReadSLI(readKeys uint64, readTime float64, readSize float64) {
	if readKeys != 0 && readTime != 0 {
		if readKeys <= smallTxnReadRow && readSize < smallTxnReadSize {
			TiKVSmallReadDuration.Observe(readTime)
		} else {
			TiKVReadThroughput.Observe(readSize / readTime)
		}
	}
}
