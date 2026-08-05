package resourcecontrol

import (
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/assert"
	"github.com/tikv/client-go/v2/tikvrpc"
)

func TestMakeRequestInfo(t *testing.T) {
	// Test a non-write request.
	req := &tikvrpc.Request{Req: &kvrpcpb.BatchGetRequest{}, Context: kvrpcpb.Context{Peer: &metapb.Peer{StoreId: 1}}}
	info := MakeRequestInfo(req)
	assert.False(t, info.IsWrite())
	assert.Equal(t, uint64(0), info.WriteBytes())
	assert.False(t, info.Bypass())
	assert.Equal(t, uint64(1), info.StoreID())

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
	// Test a commit request.
	commitReq := &kvrpcpb.CommitRequest{Keys: [][]byte{[]byte("qux")}}
	req = &tikvrpc.Request{Type: tikvrpc.CmdCommit, Req: commitReq, ReplicaNumber: 2, Context: kvrpcpb.Context{Peer: &metapb.Peer{StoreId: 3}}}
	info = MakeRequestInfo(req)
	assert.True(t, info.IsWrite())
	assert.Equal(t, uint64(3), info.WriteBytes())
	assert.False(t, info.Bypass())
	assert.Equal(t, uint64(3), info.StoreID())

	// Test Nil Peer in Context
	req = &tikvrpc.Request{Type: tikvrpc.CmdCommit, Req: commitReq, ReplicaNumber: 2, Context: kvrpcpb.Context{}}
	info = MakeRequestInfo(req)
	assert.True(t, info.IsWrite())
	assert.Equal(t, uint64(3), info.WriteBytes())
	assert.False(t, info.Bypass())
	assert.Equal(t, uint64(0), info.StoreID())
}

func TestResponseInfoBatchedTasks(t *testing.T) {
	// Every nested batched response must contribute scan bytes and KV CPU
	// because the top-level execution details do not include that work.
	resp := &tikvrpc.Response{
		Resp: &coprocessor.Response{
			ExecDetailsV2: &kvrpcpb.ExecDetailsV2{
				ScanDetailV2: &kvrpcpb.ScanDetailV2{
					ProcessedVersionsSize: 80,
				},
				TimeDetailV2: &kvrpcpb.TimeDetailV2{ProcessWallTimeNs: 1000},
			},
			BatchResponses: []*coprocessor.StoreBatchTaskResponse{
				{
					Data: []byte("data"),
					ExecDetailsV2: &kvrpcpb.ExecDetailsV2{
						ScanDetailV2: &kvrpcpb.ScanDetailV2{
							ProcessedVersionsSize: 10,
						},
						TimeDetailV2: &kvrpcpb.TimeDetailV2{ProcessWallTimeNs: 100},
					},
				},
				{
					ExecDetailsV2: &kvrpcpb.ExecDetailsV2{
						ScanDetailV2: &kvrpcpb.ScanDetailV2{
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
	assert.Equal(t, uint64(80+10+20+8), info.ReadBytes())
	assert.Equal(t, time.Duration(1000+100+200), info.KVCPU())
}
