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
	codec := NewCodecV2(ModeRaw)

	r, err := EncodeRequest(req, codec)
	require.Nil(t, err)
	require.Equal(t, append(APIV2RawKeyPrefix, []byte("key")...), r.RawGet().Key)

	r, err = EncodeRequest(req, codec)
	require.Nil(t, err)
	require.Equal(t, append(APIV2RawKeyPrefix, []byte("key")...), r.RawGet().Key)
}

func TestEncodeV2KeyRanges(t *testing.T) {
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
			StartKey: APIV2RawKeyPrefix,
			EndKey:   APIV2RawEndKey,
		},
		{
			StartKey: APIV2RawKeyPrefix,
			EndKey:   append(APIV2RawKeyPrefix, 'z'),
		},
		{
			StartKey: append(APIV2RawKeyPrefix, 'a'),
			EndKey:   APIV2RawEndKey,
		},
		{
			StartKey: append(APIV2RawKeyPrefix, 'a'),
			EndKey:   append(APIV2RawKeyPrefix, 'z'),
		},
	}
	codec := NewCodecV2(ModeRaw)
	encodedKeyRanges := codec.EncodeKeyRanges(keyRanges)
	require.Equal(t, expect, encodedKeyRanges)
}
