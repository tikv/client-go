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

func TestEncodeEncodeV2KeyRanges(t *testing.T) {
	keyRanges := []*kvrpcpb.KeyRange{
		{
			StartKey: []byte{},
			EndKey:   []byte{},
		},
		{
			StartKey: []byte{},
			EndKey:   []byte{'z'},
		},
		{
			StartKey: []byte{'a'},
			EndKey:   []byte{},
		},
		{
			StartKey: []byte{'a'},
			EndKey:   []byte{'z'},
		},
	}
	expect := []*kvrpcpb.KeyRange{
		{
			StartKey: getV2Prefix(ModeRaw),
			EndKey:   getV2EndKey(ModeRaw),
		},
		{
			StartKey: getV2Prefix(ModeRaw),
			EndKey:   append(getV2Prefix(ModeRaw), 'z'),
		},
		{
			StartKey: append(getV2Prefix(ModeRaw), 'a'),
			EndKey:   getV2EndKey(ModeRaw),
		},
		{
			StartKey: append(getV2Prefix(ModeRaw), 'a'),
			EndKey:   append(getV2Prefix(ModeRaw), 'z'),
		},
	}
	encodedKeyRanges := EncodeV2KeyRanges(ModeRaw, keyRanges)
	require.Equal(t, expect, encodedKeyRanges)
}
