package apicodec

import (
	"math"
	"testing"

	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/client-go/v2/tikvrpc"
)

var (
	testKeyspaceID = uint32(4242)
	// Keys below are ordered as following:
	// beforePrefix, keyspacePrefix, insideLeft, insideRight, keyspaceEndKey, afterEndKey
	// where valid keyspace range is [keyspacePrefix, keyspaceEndKey)
	keyspacePrefix = []byte{'r', 0, 16, 146}
	keyspaceEndKey = []byte{'r', 0, 16, 147}
	beforePrefix   = []byte{'r', 0, 0, 1}
	afterEndKey    = []byte{'r', 1, 0, 0}
	insideLeft     = []byte{'r', 0, 16, 146, 100}
	insideRight    = []byte{'r', 0, 16, 146, 200}
)

type testCodecV2Suite struct {
	suite.Suite
	codec *codecV2
}

func TestCodecV2(t *testing.T) {
	suite.Run(t, new(testCodecV2Suite))
}

func (suite *testCodecV2Suite) SetupSuite() {
	codec, err := NewCodecV2(ModeRaw, testKeyspaceID)
	suite.NoError(err)
	suite.Equal(keyspacePrefix, codec.GetKeyspace())
	suite.codec = codec.(*codecV2)
}

func (suite *testCodecV2Suite) TestEncodeRequest() {
	re := suite.Require()
	req := &tikvrpc.Request{
		Type: tikvrpc.CmdRawGet,
		Req: &kvrpcpb.RawGetRequest{
			Key: []byte("key"),
		},
	}
	req.ApiVersion = kvrpcpb.APIVersion_V2

	r, err := suite.codec.EncodeRequest(req)
	re.NoError(err)
	re.Equal(append(keyspacePrefix, []byte("key")...), r.RawGet().Key)

	r, err = suite.codec.EncodeRequest(req)
	re.NoError(err)
	re.Equal(append(keyspacePrefix, []byte("key")...), r.RawGet().Key)
}

func (suite *testCodecV2Suite) TestEncodeV2KeyRanges() {
	re := suite.Require()
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
			StartKey: keyspacePrefix,
			EndKey:   keyspaceEndKey,
		},
		{
			StartKey: keyspacePrefix,
			EndKey:   append(keyspacePrefix, 'z'),
		},
		{
			StartKey: append(keyspacePrefix, 'a'),
			EndKey:   keyspaceEndKey,
		},
		{
			StartKey: append(keyspacePrefix, 'a'),
			EndKey:   append(keyspacePrefix, 'z'),
		},
	}

	encodedKeyRanges := suite.codec.encodeKeyRanges(keyRanges)
	re.Equal(expect, encodedKeyRanges)
}

func (suite *testCodecV2Suite) TestNewCodecV2() {
	re := suite.Require()
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

func (suite *testCodecV2Suite) TestDecodeEpochNotMatch() {
	re := suite.Require()
	codec := suite.codec
	regionErr := &errorpb.Error{
		EpochNotMatch: &errorpb.EpochNotMatch{
			CurrentRegions: []*metapb.Region{
				{
					// Region 1:
					// keyspace range:	------[------)------
					// region range:	------[------)------
					// after decode:	------[------)------
					Id:       1,
					StartKey: codec.memCodec.encodeKey(keyspacePrefix),
					EndKey:   codec.memCodec.encodeKey(keyspaceEndKey),
				},
				{
					// Region 2:
					// keyspace range:	------[------)------
					// region range:	---[-------------)--
					// after decode:	------[------)------
					Id:       2,
					StartKey: codec.memCodec.encodeKey(beforePrefix),
					EndKey:   codec.memCodec.encodeKey(afterEndKey),
				},
				{
					// Region 3:
					// keyspace range:	------[------)------
					// region range:	---[----)-----------
					// after decode:	------[-)-----------
					Id:       3,
					StartKey: codec.memCodec.encodeKey(beforePrefix),
					EndKey:   codec.memCodec.encodeKey(insideLeft),
				},
				{
					// Region 4:
					// keyspace range:	------[------)------
					// region range:	--------[--)--------
					// after decode:	      --[--)--
					Id:       4,
					StartKey: codec.memCodec.encodeKey(insideLeft),
					EndKey:   codec.memCodec.encodeKey(insideRight),
				},
				{
					// Region 5:
					// keyspace range:	------[------)------
					// region range:	---[--)-------------
					// after decode:	StartKey out of bound, should be removed.
					Id:       5,
					StartKey: codec.memCodec.encodeKey(beforePrefix),
					EndKey:   codec.memCodec.encodeKey(keyspacePrefix),
				},
				{
					// Region 6:
					// keyspace range:	------[------)------
					// region range:	-------------[--)---
					// after decode:	EndKey out of bound, should be removed.
					Id:       6,
					StartKey: codec.memCodec.encodeKey(keyspaceEndKey),
					EndKey:   codec.memCodec.encodeKey(afterEndKey),
				},
			},
		},
	}

	expected := &errorpb.Error{
		EpochNotMatch: &errorpb.EpochNotMatch{
			CurrentRegions: []*metapb.Region{
				{
					Id:       1,
					StartKey: []byte{},
					EndKey:   []byte{},
				},
				{
					Id:       2,
					StartKey: []byte{},
					EndKey:   []byte{},
				},
				{
					Id:       3,
					StartKey: []byte{},
					EndKey:   insideLeft[len(keyspacePrefix):],
				},
				{
					Id:       4,
					StartKey: insideLeft[len(keyspacePrefix):],
					EndKey:   insideRight[len(keyspacePrefix):],
				},
				// Region 5 should be removed.
				// Region 6 should be removed.
			},
		},
	}

	result, err := codec.decodeRegionError(regionErr)
	re.NoError(err)
	for i := range result.EpochNotMatch.CurrentRegions {
		re.Equal(expected.EpochNotMatch.CurrentRegions[i], result.EpochNotMatch.CurrentRegions[i], "index: %d", i)
	}
}

func (suite *testCodecV2Suite) TestGetKeyspaceID() {
	suite.Equal(KeyspaceID(testKeyspaceID), suite.codec.GetKeyspaceID())
}
