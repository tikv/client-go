package resourcecontrol

import (
	"testing"

	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/assert"
	"github.com/tikv/client-go/v2/config"
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

func TestMakeRequestInfoCopHints(t *testing.T) {
	// A cop request carries paging_size_bytes (from kvproto) and may
	// additionally carry a PredictedReadBytes hint (client-go-internal
	// field on tikvrpc.Request, not in the proto).
	copReq := &coprocessor.Request{PagingSizeBytes: 4 * 1024 * 1024}
	req := &tikvrpc.Request{
		Type:               tikvrpc.CmdCop,
		Req:                copReq,
		Context:            kvrpcpb.Context{Peer: &metapb.Peer{StoreId: 7}},
		PredictedReadBytes: 256 * 1024,
	}
	info := MakeRequestInfo(req)
	assert.False(t, info.IsWrite())
	assert.Equal(t, uint64(4*1024*1024), info.PagingSizeBytes(),
		"pagingSizeBytes should come from coprocessor.Request (proto)")
	assert.Equal(t, uint64(256*1024), info.PredictedReadBytes(),
		"predictedReadBytes should come from tikvrpc.Request (internal)")

	// Without a hint, PredictedReadBytes defaults to 0 so PD falls back to
	// PagingSizeBytes as the pre-charge basis.
	reqNoHint := &tikvrpc.Request{
		Type:    tikvrpc.CmdCop,
		Req:     &coprocessor.Request{PagingSizeBytes: 4 * 1024 * 1024},
		Context: kvrpcpb.Context{Peer: &metapb.Peer{StoreId: 7}},
	}
	infoNoHint := MakeRequestInfo(reqNoHint)
	assert.Equal(t, uint64(0), infoNoHint.PredictedReadBytes(),
		"zero hint on the request means zero on RequestInfo")
	assert.Equal(t, uint64(4*1024*1024), infoNoHint.PagingSizeBytes())
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
