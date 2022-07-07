package client

import (
	"testing"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikvrpc"
)

func TestEncodeRequest(t *testing.T) {
	req := &tikvrpc.Request{
		Type: tikvrpc.CmdRawGet,
		Req: &kvrpcpb.RawGetRequest{
			Key: []byte("key"),
		},
	}
	req.ApiVersion = kvrpcpb.APIVersion_V2

	r, err := EncodeRequest(req)
	require.Nil(t, err)
	require.Equal(t, append(APIV2RawKeyPrefix, []byte("key")...), r.RawGet().Key)

	r, err = EncodeRequest(req)
	require.Nil(t, err)
	require.Equal(t, append(APIV2RawKeyPrefix, []byte("key")...), r.RawGet().Key)
}
