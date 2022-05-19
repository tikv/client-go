package client

import (
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikvrpc"
	"testing"
)

func TestEncodeRequest(t *testing.T) {
	req := &tikvrpc.Request{
		Type: tikvrpc.CmdRawGet,
		Req: &kvrpcpb.RawGetRequest{
			Key: []byte("key"),
		},
	}
	req.ApiVersion = kvrpcpb.APIVersion_V2

	req, err := EncodeRequest(req)
	require.Nil(t, err)
	require.Equal(t, []byte("rkey"), req.RawGet().Key)

	req, err = EncodeRequest(req)
	require.Nil(t, err)
	require.Equal(t, []byte("rkey"), req.RawGet().Key)
}
