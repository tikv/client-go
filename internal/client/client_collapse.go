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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/client/client_collapse.go
//

// Copyright 2020 PingCAP, Inc.
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
	"strconv"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util/async"
	"golang.org/x/sync/singleflight"
)

var _ Client = reqCollapse{}

var resolveRegionSf singleflight.Group

type reqCollapse struct {
	Client
}

// NewReqCollapse creates a reqCollapse.
func NewReqCollapse(client Client) Client {
	return &reqCollapse{client}
}
func (r reqCollapse) Close() error {
	if r.Client == nil {
		panic("client should not be nil")
	}
	return r.Client.Close()
}

func (r reqCollapse) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	if r.Client == nil {
		panic("client should not be nil")
	}
	if canCollapse, resp, err := r.tryCollapseRequest(ctx, addr, req, timeout); canCollapse {
		return resp, err
	}
	return r.Client.SendRequest(ctx, addr, req, timeout)
}

func (r reqCollapse) SendRequestAsync(ctx context.Context, addr string, req *tikvrpc.Request, cb async.Callback[*tikvrpc.Response]) {
	if r.Client == nil {
		panic("client should not be nil")
	}
	if req.Type == tikvrpc.CmdResolveLock && len(req.ResolveLock().Keys) == 0 && len(req.ResolveLock().TxnInfos) == 0 {
		// try collapse resolve lock request.
		key := strconv.FormatUint(req.Context.RegionId, 10) + "-" + strconv.FormatUint(req.ResolveLock().StartVersion, 10)
		copyReq := *req
		rsC := resolveRegionSf.DoChan(key, func() (interface{}, error) {
			// resolveRegionSf will call this function in a goroutine, thus use SendRequest directly.
			return r.Client.SendRequest(context.Background(), addr, &copyReq, ReadTimeoutShort)
		})
		// waiting the response in another goroutine.
		cb.Executor().Go(func() {
			select {
			case <-ctx.Done():
				cb.Schedule(nil, errors.WithStack(ctx.Err()))
			case rs := <-rsC:
				if rs.Err != nil {
					cb.Schedule(nil, errors.WithStack(rs.Err))
					return
				}
				cb.Schedule(rs.Val.(*tikvrpc.Response), nil)
			}
		})
	} else {
		r.Client.SendRequestAsync(ctx, addr, req, cb)
	}
}

func (r reqCollapse) tryCollapseRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (canCollapse bool, resp *tikvrpc.Response, err error) {
	switch req.Type {
	case tikvrpc.CmdResolveLock:
		resolveLock := req.ResolveLock()
		if len(resolveLock.Keys) > 0 {
			// can not collapse resolve lock lite
			return
		}
		if len(resolveLock.TxnInfos) > 0 {
			// can not collapse batch resolve locks which is only used by GC worker.
			return
		}
		canCollapse = true
		key := strconv.FormatUint(req.Context.RegionId, 10) + "-" + strconv.FormatUint(resolveLock.StartVersion, 10)
		resp, err = r.collapse(ctx, key, &resolveRegionSf, addr, req, timeout)
		return
	default:
		// now we only support collapse resolve lock.
		return
	}
}

func (r reqCollapse) collapse(ctx context.Context, key string, sf *singleflight.Group,
	addr string, req *tikvrpc.Request, timeout time.Duration) (resp *tikvrpc.Response, err error) {
	// because the request may be used by other goroutines, copy the request to avoid data race.
	copyReq := *req
	if req.Type == tikvrpc.CmdResolveLock && req.Req != nil {
		copyReq.Req = proto.Clone(req.ResolveLock())
	}
	rsC := sf.DoChan(key, func() (interface{}, error) {
		return r.Client.SendRequest(context.Background(), addr, &copyReq, ReadTimeoutShort) // use resolveLock timeout.
	})
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		err = errors.WithStack(ctx.Err())
		return
	case <-timer.C:
		err = errors.WithStack(context.DeadlineExceeded)
		return
	case rs := <-rsC:
		if rs.Err != nil {
			err = errors.WithStack(rs.Err)
			return
		}
		resp = rs.Val.(*tikvrpc.Response)
		return
	}
}
