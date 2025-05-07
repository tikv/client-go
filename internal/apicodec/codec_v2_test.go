package apicodec

import (
	"math"
	"testing"

	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/mpp"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util/codec"
)

var (
	testKeyspaceID = uint32(4242)
	// Keys below are ordered as following:
	// beforePrefix, keyspacePrefix, insideLeft, insideRight, keyspaceEndKey, afterEndKey
	// where valid keyspace range is [keyspacePrefix, keyspaceEndKey)
	prevKeyspacePrefix = []byte{'r', 0, 16, 145}
	keyspacePrefix     = []byte{'r', 0, 16, 146}
	keyspaceEndKey     = []byte{'r', 0, 16, 147}
	beforePrefix       = []byte{'r', 0, 0, 1}
	afterEndKey        = []byte{'r', 1, 0, 0}
	insideLeft         = []byte{'r', 0, 16, 146, 100}
	insideRight        = []byte{'r', 0, 16, 146, 200}
)

type testCodecV2Suite struct {
	suite.Suite
	codec *codecV2
}

func TestCodecV2(t *testing.T) {
	suite.Run(t, new(testCodecV2Suite))
}

func (suite *testCodecV2Suite) SetupSuite() {
	testKeyspaceMeta := keyspacepb.KeyspaceMeta{
		Id: testKeyspaceID,
	}
	codec, err := NewCodecV2(ModeRaw, &testKeyspaceMeta)
	suite.NoError(err)
	suite.Equal(keyspacePrefix, codec.GetKeyspace())
	suite.codec = codec.(*codecV2)
}

func (suite *testCodecV2Suite) TestEncodeRequest() {
	re := suite.Require()
	requests := []struct {
		name     string
		req      *tikvrpc.Request
		validate func(*tikvrpc.Request)
	}{
		{
			name: "CmdRawGet",
			req: &tikvrpc.Request{
				Type: tikvrpc.CmdRawGet,
				Req: &kvrpcpb.RawGetRequest{
					Key: []byte("key"),
				},
			},
			validate: func(encoded *tikvrpc.Request) {
				re.Equal(append(keyspacePrefix, []byte("key")...), encoded.RawGet().Key)
			},
		},
		{
			name: "CmdCommitWithOutPrimaryKey",
			req: &tikvrpc.Request{
				Type: tikvrpc.CmdCommit,
				Req: &kvrpcpb.CommitRequest{
					Keys: [][]byte{[]byte("key1"), []byte("key2")},
				},
			},
			validate: func(encoded *tikvrpc.Request) {
				re.Equal([][]byte{
					append(keyspacePrefix, []byte("key1")...),
					append(keyspacePrefix, []byte("key2")...),
				}, encoded.Commit().Keys)
				re.Empty(encoded.Commit().PrimaryKey)
			},
		},
		{
			name: "CmdCommitWithPrimaryKey",
			req: &tikvrpc.Request{
				Type: tikvrpc.CmdCommit,
				Req: &kvrpcpb.CommitRequest{
					Keys:       [][]byte{[]byte("key1"), []byte("key2")},
					PrimaryKey: []byte("key1"),
				},
			},
			validate: func(encoded *tikvrpc.Request) {
				re.Equal([][]byte{
					append(keyspacePrefix, []byte("key1")...),
					append(keyspacePrefix, []byte("key2")...),
				}, encoded.Commit().Keys)
				re.Equal(append(keyspacePrefix, []byte("key1")...), encoded.Commit().PrimaryKey)
			},
		},
	}

	for _, req := range requests {
		suite.Run(req.name, func() {
			encoded, err := suite.codec.EncodeRequest(req.req)
			re.NoError(err)
			req.validate(encoded)
		})
	}
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
		keyspaceID     uint32
		shouldErr      bool
		expectedPrefix []byte
		expectedEnd    []byte
	}{
		{
			mode: ModeRaw,
			// A too large keyspaceID should result in error.
			keyspaceID: math.MaxUint32,
			shouldErr:  true,
		},
		{
			// Bad mode should result in error.
			mode:       Mode(99),
			keyspaceID: DefaultKeyspaceID,
			shouldErr:  true,
		},
		{
			mode:           ModeRaw,
			keyspaceID:     1<<24 - 2,
			expectedPrefix: []byte{'r', 255, 255, 254},
			expectedEnd:    []byte{'r', 255, 255, 255},
		},
		{
			// EndKey should be able to carry over increment from lower byte.
			mode:           ModeTxn,
			keyspaceID:     1<<8 - 1,
			expectedPrefix: []byte{'x', 0, 0, 255},
			expectedEnd:    []byte{'x', 0, 1, 0},
		},
		{
			// EndKey should be able to carry over increment from lower byte.
			mode:           ModeTxn,
			keyspaceID:     1<<16 - 1,
			expectedPrefix: []byte{'x', 0, 255, 255},
			expectedEnd:    []byte{'x', 1, 0, 0},
		},
		{
			// If prefix is the last keyspace, then end should change the mode byte.
			mode:           ModeRaw,
			keyspaceID:     1<<24 - 1,
			expectedPrefix: []byte{'r', 255, 255, 255},
			expectedEnd:    []byte{'s', 0, 0, 0},
		},
	}
	for _, testCase := range testCases {

		keyspaceMeta := &keyspacepb.KeyspaceMeta{Id: testCase.keyspaceID}
		if testCase.shouldErr {
			_, err := NewCodecV2(testCase.mode, keyspaceMeta)
			re.Error(err)
			continue
		}
		codec, err := NewCodecV2(testCase.mode, keyspaceMeta)
		re.NoError(err)

		v2Codec, ok := codec.(*codecV2)
		re.True(ok)
		re.Equal(keyspaceMeta, v2Codec.keyspaceMeta)
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

func (suite *testCodecV2Suite) TestDecodeKeyError() {
	re := suite.Require()
	errors := []struct {
		name     string
		err      *kvrpcpb.KeyError
		validate func(*kvrpcpb.KeyError)
	}{
		{
			name: "TxnLockNotFound",
			err: &kvrpcpb.KeyError{
				TxnLockNotFound: &kvrpcpb.TxnLockNotFound{
					Key: append(keyspacePrefix, []byte("key1")...),
				},
			},
			validate: func(decoded *kvrpcpb.KeyError) {
				re.Equal([]byte("key1"), decoded.TxnLockNotFound.Key)
			},
		},
		{
			name: "MvccDebugInfo",
			err: &kvrpcpb.KeyError{
				TxnLockNotFound: &kvrpcpb.TxnLockNotFound{
					Key: append(keyspacePrefix, []byte("key1")...),
				},
				DebugInfo: &kvrpcpb.DebugInfo{
					MvccInfo: []*kvrpcpb.MvccDebugInfo{
						{
							Key:  append(keyspacePrefix, []byte("key1")...),
							Mvcc: &kvrpcpb.MvccInfo{},
						},
					},
				},
			},
			validate: func(decoded *kvrpcpb.KeyError) {
				re.Equal([]byte("key1"), decoded.TxnLockNotFound.Key)
				re.Equal(1, len(decoded.DebugInfo.MvccInfo))
				re.Equal([]byte("key1"), decoded.DebugInfo.MvccInfo[0].Key)
			},
		},
	}

	codec := suite.codec
	for _, keyErr := range errors {
		decoded, err := codec.decodeKeyError(keyErr.err)
		re.NoError(err)
		keyErr.validate(decoded)
	}
}

func (suite *testCodecV2Suite) TestGetKeyspaceID() {
	suite.Equal(KeyspaceID(testKeyspaceID), suite.codec.GetKeyspaceID())
}

func (suite *testCodecV2Suite) TestEncodeMPPRequest() {
	req, err := suite.codec.EncodeRequest(&tikvrpc.Request{
		Type: tikvrpc.CmdMPPTask,
		Req: &mpp.DispatchTaskRequest{
			Meta: &mpp.TaskMeta{},
			Regions: []*coprocessor.RegionInfo{
				{
					Ranges: []*coprocessor.KeyRange{{Start: []byte("a"), End: []byte("b")}},
				},
			},
		},
	})
	suite.Nil(err)
	task, ok := req.Req.(*mpp.DispatchTaskRequest)
	suite.True(ok)
	suite.Equal(task.Meta.KeyspaceId, testKeyspaceID)
	suite.Equal(task.Meta.ApiVersion, kvrpcpb.APIVersion_V2)
	suite.Equal(task.Regions[0].Ranges[0].Start, suite.codec.EncodeKey([]byte("a")))
	suite.Equal(task.Regions[0].Ranges[0].End, suite.codec.EncodeKey([]byte("b")))
}

func (suite *testCodecV2Suite) TestDecodeBucketKeys() {
	encodeWithPrefix := func(prefix, key []byte) []byte {
		return codec.EncodeBytes(nil, append(prefix, key...))
	}

	bucketKeys := [][]byte{
		encodeWithPrefix(prevKeyspacePrefix, []byte("a")),
		encodeWithPrefix(prevKeyspacePrefix, []byte("b")),
		encodeWithPrefix(prevKeyspacePrefix, []byte("c")),
		suite.codec.EncodeRegionKey([]byte("a")),
		suite.codec.EncodeRegionKey([]byte("b")),
		suite.codec.EncodeRegionKey([]byte("c")),
		encodeWithPrefix(keyspaceEndKey, []byte("")),
		encodeWithPrefix(keyspaceEndKey, []byte("a")),
		encodeWithPrefix(keyspaceEndKey, []byte("b")),
		encodeWithPrefix(keyspaceEndKey, []byte("c")),
	}
	keys, err := suite.codec.DecodeBucketKeys(bucketKeys)
	suite.Nil(err)
	suite.Equal([][]byte{
		{},
		[]byte("a"),
		[]byte("b"),
		[]byte("c"),
		{},
	}, keys)

	bucketKeys = [][]byte{
		encodeWithPrefix(prevKeyspacePrefix, []byte("a")),
		encodeWithPrefix(prevKeyspacePrefix, []byte("b")),
		encodeWithPrefix(prevKeyspacePrefix, []byte("c")),
		suite.codec.EncodeRegionKey([]byte("")),
		suite.codec.EncodeRegionKey([]byte("a")),
		suite.codec.EncodeRegionKey([]byte("b")),
		suite.codec.EncodeRegionKey([]byte("c")),
		{},
	}
	keys, err = suite.codec.DecodeBucketKeys(bucketKeys)
	suite.Nil(err)
	suite.Equal([][]byte{
		{},
		[]byte("a"),
		[]byte("b"),
		[]byte("c"),
		{},
	}, keys)

	bucketKeys = [][]byte{
		{},
		encodeWithPrefix(prevKeyspacePrefix, []byte("a")),
		encodeWithPrefix(prevKeyspacePrefix, []byte("b")),
		encodeWithPrefix(prevKeyspacePrefix, []byte("c")),
		suite.codec.EncodeRegionKey([]byte("")),
		suite.codec.EncodeRegionKey([]byte("a")),
		suite.codec.EncodeRegionKey([]byte("b")),
		suite.codec.EncodeRegionKey([]byte("c")),
		{},
	}
	keys, err = suite.codec.DecodeBucketKeys(bucketKeys)
	suite.Nil(err)
	suite.Equal([][]byte{
		{},
		[]byte("a"),
		[]byte("b"),
		[]byte("c"),
		{},
	}, keys)
}
