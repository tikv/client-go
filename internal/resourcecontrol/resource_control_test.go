package resourcecontrol

import (
	"testing"

	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/assert"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util"
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
	requestSource := "xxx_internal_others"
	req = &tikvrpc.Request{
		Type:          tikvrpc.CmdPrewrite,
		Req:           prewriteReq,
		ReplicaNumber: 1,
		Context: kvrpcpb.Context{
			Peer:          &metapb.Peer{StoreId: 2},
			RequestSource: requestSource,
		},
	}
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

func TestMakeRequestInfoBypassCases(t *testing.T) {
	tests := []struct {
		name           string
		req            *tikvrpc.Request
		expectedBypass bool
		nextGenOnly    bool
	}{
		{
			name:           "internal others",
			req:            &tikvrpc.Request{Context: kvrpcpb.Context{RequestSource: "xxx_internal_others"}},
			expectedBypass: true,
		},
		{
			name: "add index",
			req: &tikvrpc.Request{
				Context: kvrpcpb.Context{RequestSource: util.InternalTxnAddIndex},
			},
			expectedBypass: true,
			nextGenOnly:    true,
		},
		{
			name: "merge temp index",
			req: &tikvrpc.Request{
				Context: kvrpcpb.Context{RequestSource: util.InternalTxnMergeTempIndex},
			},
			expectedBypass: true,
			nextGenOnly:    true,
		},
		{
			name: "br",
			req: &tikvrpc.Request{
				Context: kvrpcpb.Context{RequestSource: util.InternalTxnBR},
			},
			expectedBypass: true,
			nextGenOnly:    true,
		},
		{
			name: "import into",
			req: &tikvrpc.Request{
				Context: kvrpcpb.Context{RequestSource: util.InternalImportInto},
			},
			expectedBypass: true,
			nextGenOnly:    true,
		},
		{
			name: "workload learning",
			req: &tikvrpc.Request{
				Context: kvrpcpb.Context{RequestSource: util.InternalTxnWorkloadLearning},
			},
			expectedBypass: true,
			nextGenOnly:    true,
		},
		{
			name: "analyze stats",
			req: &tikvrpc.Request{
				Type: tikvrpc.CmdCop,
				Req: &coprocessor.Request{
					Tp: reqTypeAnalyze,
				},
				Context: kvrpcpb.Context{
					Peer:          &metapb.Peer{StoreId: 4},
					RequestSource: util.InternalTxnStats,
				},
			},
			expectedBypass: true,
			nextGenOnly:    true,
		},
		{
			name: "analyze stats without analyze cop",
			req: &tikvrpc.Request{
				Type: tikvrpc.CmdCop,
				Req: &coprocessor.Request{
					Tp: 1,
				},
				Context: kvrpcpb.Context{
					RequestSource: util.InternalTxnStats,
				},
			},
			expectedBypass: false,
			nextGenOnly:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.nextGenOnly && !config.NextGen {
				t.Skip("rule only applies to nextgen")
			}
			info := MakeRequestInfo(tt.req)
			assert.Equal(t, tt.expectedBypass, info.Bypass())
		})
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
