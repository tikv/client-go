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

package client

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/stretchr/testify/assert"
	"github.com/tikv/client-go/v2/internal/resourcecontrol"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/tikvrpc/interceptor"
	"github.com/tikv/client-go/v2/util"
	"github.com/tikv/client-go/v2/util/async"
	resourceControlClient "github.com/tikv/pd/client/resource_group/controller"
)

type emptyClient struct{}

func (c emptyClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	return nil, nil
}

func (c emptyClient) SendRequestAsync(ctx context.Context, addr string, req *tikvrpc.Request, cb async.Callback[*tikvrpc.Response]) {
	cb.Invoke(nil, nil)
}

func (c emptyClient) Close() error {
	return nil
}

func (c emptyClient) CloseAddr(addr string) error {
	return nil
}

func (c emptyClient) SetEventListener(listener ClientEventListener) {}

func TestInterceptedClient(t *testing.T) {
	executed := false
	client := NewInterceptedClient(emptyClient{})
	ctx := interceptor.WithRPCInterceptor(context.Background(), interceptor.NewRPCInterceptor("test", func(next interceptor.RPCInterceptorFunc) interceptor.RPCInterceptorFunc {
		return func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
			executed = true
			return next(target, req)
		}
	}))
	_, _ = client.SendRequest(ctx, "", &tikvrpc.Request{}, 0)
	assert.True(t, executed)
}

func TestAppendChainedInterceptor(t *testing.T) {
	executed := make([]int, 0, 10)
	client := NewInterceptedClient(emptyClient{})

	mkInterceptorFn := func(i int) interceptor.RPCInterceptor {
		return interceptor.NewRPCInterceptor(fmt.Sprintf("%d", i), func(next interceptor.RPCInterceptorFunc) interceptor.RPCInterceptorFunc {
			return func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
				executed = append(executed, i)
				return next(target, req)
			}
		})
	}

	checkChained := func(it interceptor.RPCInterceptor, count int, expected []int) {
		chain, ok := it.(*interceptor.RPCInterceptorChain)
		assert.True(t, ok)
		assert.Equal(t, chain.Len(), count)

		executed = executed[:0]
		ctx := interceptor.WithRPCInterceptor(context.Background(), it)
		_, _ = client.SendRequest(ctx, "", &tikvrpc.Request{}, 0)
		assert.Equal(t, executed, expected)
	}

	it := mkInterceptorFn(0)
	expected := []int{0}
	for i := 1; i < 3; i++ {
		it = interceptor.ChainRPCInterceptors(it, mkInterceptorFn(i))
		expected = append(expected, i)
		checkChained(it, i+1, expected)
	}

	it2 := interceptor.ChainRPCInterceptors(mkInterceptorFn(3), mkInterceptorFn(4))
	checkChained(it2, 2, []int{3, 4})

	chain := interceptor.ChainRPCInterceptors(it, it2)
	checkChained(chain, 5, []int{0, 1, 2, 3, 4})

	// add duplciated
	chain = interceptor.ChainRPCInterceptors(chain, mkInterceptorFn(1))
	checkChained(chain, 5, []int{0, 2, 3, 4, 1})
}

// recordingInterceptor counts OnRequestWait / OnResponseWait / OnRequestCancel
// invocations and returns benign zero values so the interceptor wiring can be
// exercised end-to-end without touching real PD state.
type recordingInterceptor struct {
	waitCalls   int
	respCalls   int
	cancelCalls int
}

var (
	recordingRequestConsumption = &rmpb.Consumption{RRU: 11, WRU: 3}
	recordingCancelDelta        = &rmpb.Consumption{RRU: -7}
)

func (r *recordingInterceptor) OnRequestWait(
	context.Context, string, resourceControlClient.RequestInfo,
) (*rmpb.Consumption, *rmpb.Consumption, time.Duration, uint32, error) {
	r.waitCalls++
	return recordingRequestConsumption, &rmpb.Consumption{}, 0, 0, nil
}

func (r *recordingInterceptor) OnResponse(
	string, resourceControlClient.RequestInfo, resourceControlClient.ResponseInfo,
) (*rmpb.Consumption, error) {
	return &rmpb.Consumption{}, nil
}

func (r *recordingInterceptor) OnResponseWait(
	context.Context, string, resourceControlClient.RequestInfo, resourceControlClient.ResponseInfo,
) (*rmpb.Consumption, time.Duration, error) {
	r.respCalls++
	return &rmpb.Consumption{}, 0, nil
}

func (r *recordingInterceptor) OnRequestCancel(
	context.Context, string, resourceControlClient.RequestInfo,
) {
	r.cancelCalls++
}

func (r *recordingInterceptor) OnRequestCancelWithDelta(
	context.Context, string, resourceControlClient.RequestInfo,
) *rmpb.Consumption {
	r.cancelCalls++
	return recordingCancelDelta
}

func (r *recordingInterceptor) IsBackgroundRequest(context.Context, string, string) bool {
	return false
}

func (r *recordingInterceptor) GetRUVersion() resourceControlClient.RUVersion {
	return resourceControlClient.DefaultRUVersion
}

type legacyRecordingInterceptor struct {
	waitCalls   int
	respCalls   int
	cancelCalls int
}

func (r *legacyRecordingInterceptor) OnRequestWait(
	context.Context, string, resourceControlClient.RequestInfo,
) (*rmpb.Consumption, *rmpb.Consumption, time.Duration, uint32, error) {
	r.waitCalls++
	return recordingRequestConsumption, &rmpb.Consumption{}, 0, 0, nil
}

func (r *legacyRecordingInterceptor) OnResponse(
	string, resourceControlClient.RequestInfo, resourceControlClient.ResponseInfo,
) (*rmpb.Consumption, error) {
	return &rmpb.Consumption{}, nil
}

func (r *legacyRecordingInterceptor) OnResponseWait(
	context.Context, string, resourceControlClient.RequestInfo, resourceControlClient.ResponseInfo,
) (*rmpb.Consumption, time.Duration, error) {
	r.respCalls++
	return &rmpb.Consumption{}, 0, nil
}

func (r *legacyRecordingInterceptor) OnRequestCancel(
	context.Context, string, resourceControlClient.RequestInfo,
) {
	r.cancelCalls++
}

func (r *legacyRecordingInterceptor) IsBackgroundRequest(context.Context, string, string) bool {
	return false
}

func (r *legacyRecordingInterceptor) GetRUVersion() resourceControlClient.RUVersion {
	return resourceControlClient.DefaultRUVersion
}

// failingClient simulates a transport-level RPC failure: SendRequest returns
// (nil, err) so the interceptor's OnResponseWait path is skipped and the
// cancel path must take over.
type failingClient struct{ emptyClient }

func (c failingClient) SendRequest(
	context.Context, string, *tikvrpc.Request, time.Duration,
) (*tikvrpc.Response, error) {
	return nil, errors.New("simulated transport failure")
}

func (c failingClient) SendRequestAsync(
	_ context.Context, _ string, _ *tikvrpc.Request, cb async.Callback[*tikvrpc.Response],
) {
	cb.Invoke(nil, errors.New("simulated transport failure"))
}

func withRecordingInterceptor(t *testing.T) *recordingInterceptor {
	t.Helper()
	rec := &recordingInterceptor{}
	var iface resourceControlClient.ResourceGroupKVInterceptor = rec
	ResourceControlSwitch.Store(true)
	ResourceControlInterceptor.Store(&iface)
	t.Cleanup(func() {
		ResourceControlSwitch.Store(false)
		ResourceControlInterceptor.Store(nil)
	})
	return rec
}

func withLegacyRecordingInterceptor(t *testing.T) *legacyRecordingInterceptor {
	t.Helper()
	rec := &legacyRecordingInterceptor{}
	var iface resourceControlClient.ResourceGroupKVInterceptor = rec
	ResourceControlSwitch.Store(true)
	ResourceControlInterceptor.Store(&iface)
	t.Cleanup(func() {
		ResourceControlSwitch.Store(false)
		ResourceControlInterceptor.Store(nil)
	})
	return rec
}

func newRGRequest() *tikvrpc.Request {
	req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{})
	req.ResourceControlContext = &kvrpcpb.ResourceControlContext{ResourceGroupName: "test-rg"}
	return req
}

func TestSendRequestCancelsPreChargeOnTransportFailure(t *testing.T) {
	rec := withRecordingInterceptor(t)
	client := NewInterceptedClient(failingClient{})

	resp, err := client.SendRequest(context.Background(), "", newRGRequest(), 0)
	assert.Nil(t, resp)
	assert.Error(t, err)
	assert.Equal(t, 1, rec.waitCalls, "OnRequestWait must run once")
	assert.Equal(t, 0, rec.respCalls, "OnResponseWait must be skipped when resp is nil")
	assert.Equal(t, 1, rec.cancelCalls,
		"OnRequestCancel must run when the inner client returns no response")
}

func TestSendRequestAppliesCancelDeltaToRUDetailsOnTransportFailure(t *testing.T) {
	withRecordingInterceptor(t)
	client := NewInterceptedClient(failingClient{})
	ruDetails := util.NewRUDetails()
	ctx := context.WithValue(context.Background(), util.RUDetailsCtxKey, ruDetails)

	resp, err := client.SendRequest(ctx, "", newRGRequest(), 0)
	assert.Nil(t, resp)
	assert.Error(t, err)
	assert.Equal(t, recordingRequestConsumption.RRU+recordingCancelDelta.RRU, ruDetails.RRU(),
		"failed no-response requests must apply PD's signed cancel delta to runtime stats")
	assert.Equal(t, recordingRequestConsumption.WRU, ruDetails.WRU(),
		"failed no-response requests must not locally roll back write RU")
}

func TestSendRequestLegacyCancelDoesNotRollbackRUDetails(t *testing.T) {
	rec := withLegacyRecordingInterceptor(t)
	client := NewInterceptedClient(failingClient{})
	ruDetails := util.NewRUDetails()
	ctx := context.WithValue(context.Background(), util.RUDetailsCtxKey, ruDetails)

	resp, err := client.SendRequest(ctx, "", newRGRequest(), 0)
	assert.Nil(t, resp)
	assert.Error(t, err)
	assert.Equal(t, 1, rec.cancelCalls)
	assert.Equal(t, recordingRequestConsumption.RRU, ruDetails.RRU(),
		"legacy cancel has no signed delta, so client-go must not guess a local RRU rollback")
	assert.Equal(t, recordingRequestConsumption.WRU, ruDetails.WRU(),
		"legacy cancel has no signed delta, so client-go must not guess a local WRU rollback")
}

func TestSendRequestDoesNotCancelOnSuccess(t *testing.T) {
	rec := withRecordingInterceptor(t)
	// Inner client returns a non-nil response (default emptyClient yields
	// nil but its purpose here is to NOT yield nil — wrap it).
	client := NewInterceptedClient(respondingClient{})

	resp, err := client.SendRequest(context.Background(), "", newRGRequest(), 0)
	assert.NotNil(t, resp)
	assert.NoError(t, err)
	assert.Equal(t, 1, rec.waitCalls)
	assert.Equal(t, 1, rec.respCalls, "settlement path must run when resp is non-nil")
	assert.Equal(t, 0, rec.cancelCalls, "cancel must not run when resp is non-nil")
}

func TestSendRequestAsyncCancelsPreChargeOnTransportFailure(t *testing.T) {
	rec := withRecordingInterceptor(t)
	client := NewInterceptedClient(failingClient{})

	done := make(chan struct{})
	var gotResp *tikvrpc.Response
	var gotErr error
	cb := async.NewCallback(nil, func(resp *tikvrpc.Response, err error) {
		gotResp = resp
		gotErr = err
		close(done)
	})
	client.SendRequestAsync(context.Background(), "", newRGRequest(), cb)
	<-done

	assert.Nil(t, gotResp)
	assert.Error(t, gotErr)
	assert.Equal(t, 1, rec.waitCalls)
	assert.Equal(t, 0, rec.respCalls)
	assert.Equal(t, 1, rec.cancelCalls,
		"OnRequestCancel must run on the async path too when resp is nil")
}

func TestSendRequestAsyncAppliesCancelDeltaToRUDetailsOnTransportFailure(t *testing.T) {
	withRecordingInterceptor(t)
	client := NewInterceptedClient(failingClient{})
	ruDetails := util.NewRUDetails()
	ctx := context.WithValue(context.Background(), util.RUDetailsCtxKey, ruDetails)

	done := make(chan struct{})
	var gotResp *tikvrpc.Response
	var gotErr error
	cb := async.NewCallback(nil, func(resp *tikvrpc.Response, err error) {
		gotResp = resp
		gotErr = err
		close(done)
	})
	client.SendRequestAsync(ctx, "", newRGRequest(), cb)
	<-done

	assert.Nil(t, gotResp)
	assert.Error(t, gotErr)
	assert.Equal(t, recordingRequestConsumption.RRU+recordingCancelDelta.RRU, ruDetails.RRU(),
		"failed async no-response requests must apply PD's signed cancel delta to runtime stats")
	assert.Equal(t, recordingRequestConsumption.WRU, ruDetails.WRU(),
		"failed async no-response requests must not locally roll back write RU")
}

// respondingClient yields a non-nil empty response so the interceptor settles
// through OnResponseWait rather than the cancel path.
type respondingClient struct{ emptyClient }

func (c respondingClient) SendRequest(
	context.Context, string, *tikvrpc.Request, time.Duration,
) (*tikvrpc.Response, error) {
	return &tikvrpc.Response{}, nil
}

func TestBypassRUV2FollowsRequestInfoBypass(t *testing.T) {
	req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{})
	req.RequestSource = "xxx_internal_others"
	assert.True(t, resourcecontrol.MakeRequestInfo(req).Bypass())
	assert.False(t, resourcecontrol.MakeRequestInfo(tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{})).Bypass())

	backgroundReq := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{})
	backgroundReq.ResourceControlContext = &kvrpcpb.ResourceControlContext{ResourceGroupName: "rg"}
	backgroundReq.RequestSource = "background-task"
	assert.False(t, resourcecontrol.MakeRequestInfo(backgroundReq).Bypass())
}
