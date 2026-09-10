package resourcecontrol

import (
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/assert"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/tikvrpc"
)

func TestMakeRequestInfo(t *testing.T) {
	// Test a non-write request.
	readRequestSource := "leader_external_Select"
	req := &tikvrpc.Request{
		Req: &kvrpcpb.BatchGetRequest{},
		Context: kvrpcpb.Context{
			Peer:          &metapb.Peer{StoreId: 1},
			RequestSource: readRequestSource,
		},
	}
	info := MakeRequestInfo(req)
	assert.False(t, info.IsWrite())
	assert.Equal(t, uint64(0), info.WriteBytes())
	assert.False(t, info.Bypass())
	assert.Equal(t, uint64(1), info.StoreID())
	assert.Equal(t, readRequestSource, info.RequestSource())

	// Test a prewrite request.
	mutation := &kvrpcpb.Mutation{Key: []byte("foo"), Value: []byte("bar")}
	prewriteReq := &kvrpcpb.PrewriteRequest{Mutations: []*kvrpcpb.Mutation{mutation}, PrimaryLock: []byte("baz")}
	req = &tikvrpc.Request{Type: tikvrpc.CmdPrewrite, Req: prewriteReq, ReplicaNumber: 1, Context: kvrpcpb.Context{Peer: &metapb.Peer{StoreId: 2}}}
	requestSource := "xxx_internal_others"
	req.RequestSource = requestSource
	info = MakeRequestInfo(req)
	assert.True(t, info.IsWrite())
	assert.Equal(t, uint64(9), info.WriteBytes())
	assert.True(t, info.Bypass())
	assert.Equal(t, uint64(2), info.StoreID())
	assert.Equal(t, requestSource, info.RequestSource())
	// Test a commit request.
	commitReq := &kvrpcpb.CommitRequest{Keys: [][]byte{[]byte("qux")}}
	req = &tikvrpc.Request{Type: tikvrpc.CmdCommit, Req: commitReq, ReplicaNumber: 2, Context: kvrpcpb.Context{Peer: &metapb.Peer{StoreId: 3}}}
	info = MakeRequestInfo(req)
	assert.True(t, info.IsWrite())
	assert.Equal(t, uint64(3), info.WriteBytes())
	assert.False(t, info.Bypass())
	assert.Equal(t, uint64(3), info.StoreID())
	assert.Empty(t, info.RequestSource())

	// Test Nil Peer in Context
	req = &tikvrpc.Request{Type: tikvrpc.CmdCommit, Req: commitReq, ReplicaNumber: 2, Context: kvrpcpb.Context{}}
	info = MakeRequestInfo(req)
	assert.True(t, info.IsWrite())
	assert.Equal(t, uint64(3), info.WriteBytes())
	assert.False(t, info.Bypass())
	assert.Equal(t, uint64(0), info.StoreID())
}

func TestMakeRequestInfoPredictedReadBytes(t *testing.T) {
	// A read request may carry an optional PredictedReadBytes hint on
	// tikvrpc.Request; MakeRequestInfo should propagate it to RequestInfo.
	req := &tikvrpc.Request{
		Req:                &kvrpcpb.BatchGetRequest{},
		Context:            kvrpcpb.Context{Peer: &metapb.Peer{StoreId: 7}},
		PredictedReadBytes: 256 * 1024,
	}
	info := MakeRequestInfo(req)
	assert.False(t, info.IsWrite())
	assert.Equal(t, uint64(256*1024), info.PredictedReadBytes(),
		"predictedReadBytes should propagate from tikvrpc.Request")

	// Without a hint, PredictedReadBytes defaults to 0. This test only
	// checks client-go propagation; PD still decides request eligibility.
	reqNoHint := &tikvrpc.Request{
		Req:     &kvrpcpb.BatchGetRequest{},
		Context: kvrpcpb.Context{Peer: &metapb.Peer{StoreId: 7}},
	}
	infoNoHint := MakeRequestInfo(reqNoHint)
	assert.Equal(t, uint64(0), infoNoHint.PredictedReadBytes(),
		"zero hint on the request means zero on RequestInfo")
}

func TestMakeRequestInfoIsCop(t *testing.T) {
	// Coprocessor requests must carry IsCop()==true so PD can scope
	// paging_* metrics to them.
	copReq := &tikvrpc.Request{
		Type:    tikvrpc.CmdCop,
		Req:     &coprocessor.Request{},
		Context: kvrpcpb.Context{Peer: &metapb.Peer{StoreId: 1}},
	}
	assert.True(t, MakeRequestInfo(copReq).IsCop())

	copStreamReq := &tikvrpc.Request{
		Type:    tikvrpc.CmdCopStream,
		Req:     &coprocessor.Request{},
		Context: kvrpcpb.Context{Peer: &metapb.Peer{StoreId: 1}},
	}
	assert.True(t, MakeRequestInfo(copStreamReq).IsCop())

	// Non-cop reads (Get, BatchGet, Scan) must carry IsCop()==false so
	// PD ignores them in the paging accounting branch.
	for _, req := range []*tikvrpc.Request{
		{Type: tikvrpc.CmdGet, Req: &kvrpcpb.GetRequest{}, Context: kvrpcpb.Context{Peer: &metapb.Peer{StoreId: 1}}},
		{Type: tikvrpc.CmdBatchGet, Req: &kvrpcpb.BatchGetRequest{}, Context: kvrpcpb.Context{Peer: &metapb.Peer{StoreId: 1}}},
		{Type: tikvrpc.CmdScan, Req: &kvrpcpb.ScanRequest{}, Context: kvrpcpb.Context{Peer: &metapb.Peer{StoreId: 1}}},
	} {
		assert.False(t, MakeRequestInfo(req).IsCop(),
			"non-cop cmd type %v must report IsCop()==false", req.Type)
	}
}

func TestResponseInfoReadBytes(t *testing.T) {
	resp := &tikvrpc.Response{
		Resp: &coprocessor.Response{
			ExecDetailsV2: &kvrpcpb.ExecDetailsV2{
				ScanDetailV2: &kvrpcpb.ScanDetailV2{
					TotalVersionsSize:     100,
					ProcessedVersionsSize: 80,
				},
			},
		},
	}
	info := MakeResponseInfo(resp)
	if config.NextGen {
		assert.Equal(t, uint64(100), info.ReadBytes())
	} else {
		assert.Equal(t, uint64(80), info.ReadBytes())
	}

	if config.NextGen {
		// Compatibility: when processed > total (older TiKV), use processed.
		respCompat := &tikvrpc.Response{
			Resp: &coprocessor.Response{
				ExecDetailsV2: &kvrpcpb.ExecDetailsV2{
					ScanDetailV2: &kvrpcpb.ScanDetailV2{
						TotalVersionsSize:     80,
						ProcessedVersionsSize: 100,
					},
				},
			},
		}
		infoCompat := MakeResponseInfo(respCompat)
		assert.Equal(t, uint64(100), infoCompat.ReadBytes())
	}
}

func TestResponseInfoBatchedTasks(t *testing.T) {
	// Every nested batched response must contribute scan bytes and KV CPU
	// because the top-level execution details do not include that work.
	resp := &tikvrpc.Response{
		Resp: &coprocessor.Response{
			ExecDetailsV2: &kvrpcpb.ExecDetailsV2{
				ScanDetailV2: &kvrpcpb.ScanDetailV2{
					TotalVersionsSize:     100,
					ProcessedVersionsSize: 80,
				},
				TimeDetailV2: &kvrpcpb.TimeDetailV2{ProcessWallTimeNs: 1000},
			},
			BatchResponses: []*coprocessor.StoreBatchTaskResponse{
				{
					Data: []byte("data"),
					ExecDetailsV2: &kvrpcpb.ExecDetailsV2{
						ScanDetailV2: &kvrpcpb.ScanDetailV2{
							TotalVersionsSize:     15,
							ProcessedVersionsSize: 10,
						},
						TimeDetailV2: &kvrpcpb.TimeDetailV2{ProcessWallTimeNs: 100},
					},
				},
				{
					ExecDetailsV2: &kvrpcpb.ExecDetailsV2{
						ScanDetailV2: &kvrpcpb.ScanDetailV2{
							TotalVersionsSize:     25,
							ProcessedVersionsSize: 20,
						},
						TimeDetailV2: &kvrpcpb.TimeDetailV2{ProcessWallTimeNs: 200},
					},
				},
				{Data: []byte("12345678")},
			},
		},
	}

	info := MakeResponseInfo(resp)
	expectedReadBytes := uint64(80 + 10 + 20 + 8)
	if config.NextGen {
		expectedReadBytes = 100 + 15 + 25 + 8
	}
	assert.Equal(t, expectedReadBytes, info.ReadBytes())
	assert.Equal(t, time.Duration(1000+100+200), info.KVCPU())
}
