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
	"context"
	"fmt"
	"runtime/trace"
	"sync/atomic"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util"
	"github.com/tikv/client-go/v2/util/async"
	"go.uber.org/zap"
)

type ClientAsync interface {
	Client
	// SendRequestAsync sends a request to the target address asynchronously.
	SendRequestAsync(ctx context.Context, addr string, req *tikvrpc.Request, cb async.Callback[*tikvrpc.Response])
}

// SendRequestAsync implements the ClientAsync interface.
func (c *RPCClient) SendRequestAsync(ctx context.Context, addr string, req *tikvrpc.Request, cb async.Callback[*tikvrpc.Response]) {
	var err error

	if atomic.CompareAndSwapUint32(&c.idleNotify, 1, 0) {
		go c.recycleIdleConnArray()
	}

	if config.GetGlobalConfig().TiKVClient.MaxBatchSize == 0 {
		cb.Invoke(nil, errors.New("batch client is disabled"))
		return
	}
	if req.StoreTp != tikvrpc.TiKV {
		cb.Invoke(nil, errors.New("unsupported store type: "+req.StoreTp.Name()))
		return
	}

	regionRPC := trace.StartRegion(ctx, req.Type.String())
	spanRPC := opentracing.SpanFromContext(ctx)
	if spanRPC != nil && spanRPC.Tracer() != nil {
		spanRPC = spanRPC.Tracer().StartSpan(fmt.Sprintf("rpcClient.SendRequestAsync, region ID: %d, type: %s", req.RegionId, req.Type), opentracing.ChildOf(spanRPC.Context()))
		ctx = opentracing.ContextWithSpan(ctx, spanRPC)
	}

	useCodec := c.option != nil && c.option.codec != nil
	if useCodec {
		req, err = c.option.codec.EncodeRequest(req)
		if err != nil {
			cb.Invoke(nil, err)
			return
		}
	}
	tikvrpc.AttachContext(req, req.Context)

	// ToBatchCommandsRequest should be called after all modifications to req are done.
	batchReq := req.ToBatchCommandsRequest()
	if batchReq == nil {
		cb.Invoke(nil, errors.New("unsupported request type: "+req.Type.String()))
		return
	}

	// TODO(zyguan): If the client created `WithGRPCDialOptions(grpc.WithBlock())`, `getConnArray` might be blocked for
	// a while when the corresponding conn array is uninitialized. However, since tidb won't set this option, we just
	// keep `getConnArray` synchronous for now.
	connArray, err := c.getConnArray(addr, true)
	if err != nil {
		cb.Invoke(nil, err)
		return
	}

	var (
		entry = &batchCommandsEntry{
			ctx:           ctx,
			req:           batchReq,
			cb:            cb,
			forwardedHost: req.ForwardedHost,
			canceled:      0,
			err:           nil,
			pri:           req.GetResourceControlContext().GetOverridePriority(),
			start:         time.Now(),
		}
		stop func() bool
	)

	// defer post actions
	entry.cb.Inject(func(resp *tikvrpc.Response, err error) (*tikvrpc.Response, error) {
		if stop != nil {
			stop()
		}

		elapsed := time.Since(entry.start)

		// batch client metrics
		if sendLat := atomic.LoadInt64(&entry.sendLat); sendLat > 0 {
			metrics.BatchRequestDurationSend.Observe(time.Duration(sendLat).Seconds())
		}
		if recvLat := atomic.LoadInt64(&entry.recvLat); recvLat > 0 {
			metrics.BatchRequestDurationRecv.Observe(time.Duration(recvLat).Seconds())
		}
		metrics.BatchRequestDurationDone.Observe(elapsed.Seconds())

		// rpc metrics
		connArray.updateRPCMetrics(req, resp, elapsed)

		// resource control
		if stmtExec := ctx.Value(util.ExecDetailsKey); stmtExec != nil {
			execDetails := stmtExec.(*util.ExecDetails)
			atomic.AddInt64(&execDetails.WaitKVRespDuration, int64(elapsed))
			execNetworkCollector := networkCollector{}
			execNetworkCollector.onReq(req)
			execNetworkCollector.onResp(req, resp)
		}

		// tracing
		if spanRPC != nil {
			if util.TraceExecDetailsEnabled(ctx) {
				if si := buildSpanInfoFromResp(resp); si != nil {
					si.addTo(spanRPC, entry.start)
				}
			}
			spanRPC.Finish()
		}
		regionRPC.End()

		// codec
		if useCodec && err == nil {
			resp, err = c.option.codec.DecodeResponse(req, resp)
		}

		return resp, WrapErrConn(err, connArray)
	})

	stop = context.AfterFunc(ctx, func() {
		logutil.Logger(ctx).Debug("async send request cancelled (context done)", zap.String("to", addr), zap.Error(ctx.Err()))
		entry.error(ctx.Err())
	})

	batchConn := connArray.batchConn
	if val, err := util.EvalFailpoint("mockBatchCommandsChannelFullOnAsyncSend"); err == nil {
		mockBatchCommandsChannelFullOnAsyncSend(ctx, batchConn, cb, val)
	}
	select {
	case batchConn.batchCommandsCh <- entry:
		// will be fulfilled in batch send/recv loop.
	case <-ctx.Done():
		// will be fulfilled by the after callback of ctx.
	case <-batchConn.closed:
		logutil.Logger(ctx).Debug("async send request cancelled (conn closed)", zap.String("to", addr))
		cb.Invoke(nil, errors.New("batchConn closed"))
	}
}

func mockBatchCommandsChannelFullOnAsyncSend(ctx context.Context, batchConn *batchConn, cb async.Callback[*tikvrpc.Response], val any) {
	var dur time.Duration
	switch v := val.(type) {
	case int:
		dur = time.Duration(v) * time.Millisecond
	case string:
		dur, _ = time.ParseDuration(v)
	}
	if dur > 0 {
		logutil.Logger(ctx).Info("[failpoint] mock channel full for " + dur.String())
		select {
		case <-time.After(dur):
		case <-ctx.Done():
		case <-batchConn.closed:
			cb.Invoke(nil, errors.New("batchConn closed"))
		}
	} else {
		logutil.Logger(ctx).Info("[failpoint] mock channel full")
		select {
		case <-ctx.Done():
		case <-batchConn.closed:
			cb.Invoke(nil, errors.New("batchConn closed"))
		}
	}
}
