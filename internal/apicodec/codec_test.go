package apicodec

import (
	"math"
	"testing"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikvrpc"
)

var (
	defaultRawPrefix = []byte{'r', 0, 0, 0}
	defaultRawEnd    = []byte{'r', 0, 0, 1}
)

func TestEncodeRequest(t *testing.T) {
	re := require.New(t)
	req := &tikvrpc.Request{
		Type: tikvrpc.CmdRawGet,
		Req: &kvrpcpb.RawGetRequest{
			Key: []byte("key"),
		},
	}
	req.ApiVersion = kvrpcpb.APIVersion_V2
	codec, err := NewCodecV2(ModeRaw, DefaultKeyspaceID)
	re.NoError(err)

	r, err := codec.EncodeRequest(req)
	re.NoError(err)
	re.Equal(append(defaultRawPrefix, []byte("key")...), r.RawGet().Key)

	r, err = codec.EncodeRequest(req)
	re.NoError(err)
	re.Equal(append(defaultRawPrefix, []byte("key")...), r.RawGet().Key)
}

func TestEncodeV2KeyRanges(t *testing.T) {
	re := require.New(t)
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
			StartKey: defaultRawPrefix,
			EndKey:   defaultRawEnd,
		},
		{
			StartKey: defaultRawPrefix,
			EndKey:   append(defaultRawPrefix, 'z'),
		},
		{
			StartKey: append(defaultRawPrefix, 'a'),
			EndKey:   defaultRawEnd,
		},
		{
			StartKey: append(defaultRawPrefix, 'a'),
			EndKey:   append(defaultRawPrefix, 'z'),
		},
	}
	codec, err := NewCodecV2(ModeRaw, DefaultKeyspaceID)
	re.NoError(err)

	v2Codec, ok := codec.(*codecV2)
	re.True(ok)
	encodedKeyRanges := v2Codec.encodeKeyRanges(keyRanges)
	re.Equal(expect, encodedKeyRanges)
}

func TestNewCodecV2(t *testing.T) {
	re := require.New(t)
	testCases := []struct {
		mode           Mode
		spaceID        uint32
		shouldErr      bool
		expectedPrefix []byte
		expectedEnd    []byte
	}{
		{
			mode: ModeRaw,
			// A too large keyspaceID should result in error.
			spaceID:   math.MaxUint32,
			shouldErr: true,
		},
		{
			// Bad mode should result in error.
			mode:      Mode(99),
			spaceID:   DefaultKeyspaceID,
			shouldErr: true,
		},
		{
			mode:           ModeRaw,
			spaceID:        1<<24 - 2,
			expectedPrefix: []byte{'r', 255, 255, 254},
			expectedEnd:    []byte{'r', 255, 255, 255},
		},
		{
			// EndKey should be able to carry over increment from lower byte.
			mode:           ModeTxn,
			spaceID:        1<<8 - 1,
			expectedPrefix: []byte{'x', 0, 0, 255},
			expectedEnd:    []byte{'x', 0, 1, 0},
		},
		{
			// EndKey should be able to carry over increment from lower byte.
			mode:           ModeTxn,
			spaceID:        1<<16 - 1,
			expectedPrefix: []byte{'x', 0, 255, 255},
			expectedEnd:    []byte{'x', 1, 0, 0},
		},
		{
			// If prefix is the last keyspace, then end should change the mode byte.
			mode:           ModeRaw,
			spaceID:        1<<24 - 1,
			expectedPrefix: []byte{'r', 255, 255, 255},
			expectedEnd:    []byte{'s', 0, 0, 0},
		},
	}
	for _, testCase := range testCases {
		if testCase.shouldErr {
			_, err := NewCodecV2(testCase.mode, testCase.spaceID)
			re.Error(err)
			continue
		}
		codec, err := NewCodecV2(testCase.mode, testCase.spaceID)
		re.NoError(err)

		v2Codec, ok := codec.(*codecV2)
		re.True(ok)
		re.Equal(testCase.expectedPrefix, v2Codec.prefix)
		re.Equal(testCase.expectedEnd, v2Codec.endKey)
	}
}
