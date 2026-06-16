package apicodec

import (
	"math"
	"testing"

	"github.com/pingcap/kvproto/pkg/coprocessor"
	deadlockpb "github.com/pingcap/kvproto/pkg/deadlock"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/mpp"
	"github.com/stretchr/testify/require"
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
		{
			name: "CmdFlush",
			req: &tikvrpc.Request{
				Type: tikvrpc.CmdFlush,
				Req: &kvrpcpb.FlushRequest{
					Mutations: []*kvrpcpb.Mutation{
						{
							Op:  kvrpcpb.Op_Put,
							Key: []byte("key1"),
						},
						{
							Op:  kvrpcpb.Op_Del,
							Key: []byte("key2"),
						},
					},
					PrimaryKey: []byte("primary"),
				},
			},
			validate: func(encoded *tikvrpc.Request) {
				re.Len(encoded.Flush().Mutations, 2)
				re.Equal(append(keyspacePrefix, []byte("key1")...), encoded.Flush().Mutations[0].Key)
				re.Equal(append(keyspacePrefix, []byte("key2")...), encoded.Flush().Mutations[1].Key)
				re.Equal(append(keyspacePrefix, []byte("primary")...), encoded.Flush().PrimaryKey)
			},
		},
		{
			name: "CmdBufferBatchGet",
			req: &tikvrpc.Request{
				Type: tikvrpc.CmdBufferBatchGet,
				Req: &kvrpcpb.BufferBatchGetRequest{
					Keys: [][]byte{[]byte("key1"), []byte("key2")},
				},
			},
			validate: func(encoded *tikvrpc.Request) {
				re.Equal([][]byte{
					append(keyspacePrefix, []byte("key1")...),
					append(keyspacePrefix, []byte("key2")...),
				}, encoded.BufferBatchGet().Keys)
			},
		},
		{
			name: "CmdFlashbackToVersion",
			req: &tikvrpc.Request{
				Type: tikvrpc.CmdFlashbackToVersion,
				Req: &kvrpcpb.FlashbackToVersionRequest{
					StartKey: []byte("start"),
				},
			},
			validate: func(encoded *tikvrpc.Request) {
				re.Equal(append(keyspacePrefix, []byte("start")...), encoded.FlashbackToVersion().StartKey)
				re.Equal(keyspaceEndKey, encoded.FlashbackToVersion().EndKey)
			},
		},
		{
			name: "CmdPrepareFlashbackToVersion",
			req: &tikvrpc.Request{
				Type: tikvrpc.CmdPrepareFlashbackToVersion,
				Req: &kvrpcpb.PrepareFlashbackToVersionRequest{
					StartKey: []byte("prepare-start"),
				},
			},
			validate: func(encoded *tikvrpc.Request) {
				re.Equal(append(keyspacePrefix, []byte("prepare-start")...), encoded.PrepareFlashbackToVersion().StartKey)
				re.Equal(keyspaceEndKey, encoded.PrepareFlashbackToVersion().EndKey)
			},
		},
		{
			name: "CmdCompact",
			req: &tikvrpc.Request{
				Type: tikvrpc.CmdCompact,
				Req: &kvrpcpb.CompactRequest{
					StartKey: []byte("compact-start"),
				},
			},
			validate: func(encoded *tikvrpc.Request) {
				// Compact cursors are TiFlash RowKeyValue cursors, not API v2 storage keys.
				// TiFlash carries API v2 context separately through ApiVersion and KeyspaceId.
				re.Equal([]byte("compact-start"), encoded.Compact().StartKey)
				re.Equal(kvrpcpb.APIVersion_V2, encoded.Compact().ApiVersion)
				re.Equal(testKeyspaceID, encoded.Compact().KeyspaceId)
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
	errors := []struct {
		name     string
		err      *kvrpcpb.KeyError
		validate func(*require.Assertions, *kvrpcpb.KeyError)
	}{
		{
			name: "TxnLockNotFound",
			err: &kvrpcpb.KeyError{
				TxnLockNotFound: &kvrpcpb.TxnLockNotFound{
					Key: append(keyspacePrefix, []byte("key1")...),
				},
			},
			validate: func(re *require.Assertions, decoded *kvrpcpb.KeyError) {
				re.Equal([]byte("key1"), decoded.TxnLockNotFound.Key)
			},
		},
		{
			name: "LockedSharedLockInfos",
			err: &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					Key:         append(keyspacePrefix, []byte("outer-key")...),
					PrimaryLock: append(keyspacePrefix, []byte("outer-primary")...),
					Secondaries: [][]byte{
						append(keyspacePrefix, []byte("outer-secondary")...),
					},
					LockType: kvrpcpb.Op_SharedLock,
					SharedLockInfos: []*kvrpcpb.LockInfo{
						{
							Key:         append(keyspacePrefix, []byte("inner-key")...),
							PrimaryLock: append(keyspacePrefix, []byte("inner-primary")...),
							Secondaries: [][]byte{
								append(keyspacePrefix, []byte("inner-secondary")...),
							},
							LockType: kvrpcpb.Op_Lock,
						},
					},
				},
			},
			validate: func(re *require.Assertions, decoded *kvrpcpb.KeyError) {
				re.Equal([]byte("outer-key"), decoded.Locked.Key)
				re.Equal([]byte("outer-primary"), decoded.Locked.PrimaryLock)
				re.Equal([][]byte{[]byte("outer-secondary")}, decoded.Locked.Secondaries)
				re.Len(decoded.Locked.SharedLockInfos, 1)
				inner := decoded.Locked.SharedLockInfos[0]
				re.Equal([]byte("inner-key"), inner.Key)
				re.Equal([]byte("inner-primary"), inner.PrimaryLock)
				re.Equal([][]byte{[]byte("inner-secondary")}, inner.Secondaries)
			},
		},
		{
			name: "PrimaryMismatchLockInfo",
			err: &kvrpcpb.KeyError{
				PrimaryMismatch: &kvrpcpb.PrimaryMismatch{
					LockInfo: &kvrpcpb.LockInfo{
						Key:         append(keyspacePrefix, []byte("mismatch-key")...),
						PrimaryLock: append(keyspacePrefix, []byte("mismatch-primary")...),
						Secondaries: [][]byte{
							append(keyspacePrefix, []byte("mismatch-secondary")...),
						},
					},
				},
			},
			validate: func(re *require.Assertions, decoded *kvrpcpb.KeyError) {
				lockInfo := decoded.PrimaryMismatch.LockInfo
				re.Equal([]byte("mismatch-key"), lockInfo.Key)
				re.Equal([]byte("mismatch-primary"), lockInfo.PrimaryLock)
				re.Equal([][]byte{[]byte("mismatch-secondary")}, lockInfo.Secondaries)
			},
		},
		{
			name: "DeadlockDeadlockKey",
			err: &kvrpcpb.KeyError{
				Deadlock: &kvrpcpb.Deadlock{
					LockKey:     append(keyspacePrefix, []byte("lock-key")...),
					DeadlockKey: append(keyspacePrefix, []byte("deadlock-key")...),
					WaitChain: []*deadlockpb.WaitForEntry{
						{
							Key: append(keyspacePrefix, []byte("wait-key")...),
						},
					},
				},
			},
			validate: func(re *require.Assertions, decoded *kvrpcpb.KeyError) {
				re.Equal([]byte("lock-key"), decoded.Deadlock.LockKey)
				re.Equal([]byte("deadlock-key"), decoded.Deadlock.DeadlockKey)
				re.Len(decoded.Deadlock.WaitChain, 1)
				re.Equal([]byte("wait-key"), decoded.Deadlock.WaitChain[0].Key)
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
							Key: append(keyspacePrefix, []byte("key1")...),
							Mvcc: &kvrpcpb.MvccInfo{
								Lock: &kvrpcpb.MvccLock{
									Primary: append(keyspacePrefix, []byte("debug-primary")...),
									Secondaries: [][]byte{
										append(keyspacePrefix, []byte("debug-secondary-1")...),
										append(keyspacePrefix, []byte("debug-secondary-2")...),
									},
								},
							},
						},
					},
				},
			},
			validate: func(re *require.Assertions, decoded *kvrpcpb.KeyError) {
				re.Equal([]byte("key1"), decoded.TxnLockNotFound.Key)
				re.Equal(1, len(decoded.DebugInfo.MvccInfo))
				re.Equal([]byte("key1"), decoded.DebugInfo.MvccInfo[0].Key)
				re.Equal([]byte("debug-primary"), decoded.DebugInfo.MvccInfo[0].Mvcc.Lock.Primary)
				re.Equal([][]byte{
					[]byte("debug-secondary-1"),
					[]byte("debug-secondary-2"),
				}, decoded.DebugInfo.MvccInfo[0].Mvcc.Lock.Secondaries)
			},
		},
	}

	codec := suite.codec
	for _, keyErr := range errors {
		keyErr := keyErr
		suite.Run(keyErr.name, func() {
			re := suite.Require()
			decoded, err := codec.decodeKeyError(keyErr.err)
			re.NoError(err)
			keyErr.validate(re, decoded)
		})
	}
}

func (suite *testCodecV2Suite) TestDecodeResponseHotPathCommands() {
	makeRegionError := func(key, start, end []byte) *errorpb.Error {
		encodedStart, encodedEnd := suite.codec.EncodeRegionRange(start, end)
		return &errorpb.Error{
			KeyNotInRegion: &errorpb.KeyNotInRegion{
				Key:      suite.codec.EncodeKey(key),
				StartKey: encodedStart,
				EndKey:   encodedEnd,
			},
		}
	}

	tests := []struct {
		name     string
		req      *tikvrpc.Request
		resp     *tikvrpc.Response
		validate func(*require.Assertions, *tikvrpc.Response)
	}{
		{
			name: "CmdFlush",
			req: &tikvrpc.Request{
				Type: tikvrpc.CmdFlush,
				Req:  &kvrpcpb.FlushRequest{},
			},
			resp: &tikvrpc.Response{
				Resp: &kvrpcpb.FlushResponse{
					RegionError: makeRegionError([]byte("region-key"), []byte("range-start"), []byte("range-end")),
					Errors: []*kvrpcpb.KeyError{
						{
							TxnLockNotFound: &kvrpcpb.TxnLockNotFound{
								Key: suite.codec.EncodeKey([]byte("lock-key")),
							},
						},
					},
				},
			},
			validate: func(re *require.Assertions, decoded *tikvrpc.Response) {
				resp := decoded.Resp.(*kvrpcpb.FlushResponse)
				re.Equal([]byte("region-key"), resp.RegionError.KeyNotInRegion.Key)
				re.Equal([]byte("range-start"), resp.RegionError.KeyNotInRegion.StartKey)
				re.Equal([]byte("range-end"), resp.RegionError.KeyNotInRegion.EndKey)
				re.Len(resp.Errors, 1)
				re.Equal([]byte("lock-key"), resp.Errors[0].TxnLockNotFound.Key)
			},
		},
		{
			name: "CmdBufferBatchGet",
			req: &tikvrpc.Request{
				Type: tikvrpc.CmdBufferBatchGet,
				Req:  &kvrpcpb.BufferBatchGetRequest{},
			},
			resp: &tikvrpc.Response{
				Resp: &kvrpcpb.BufferBatchGetResponse{
					RegionError: makeRegionError([]byte("region-key"), []byte("range-start"), []byte("range-end")),
					Error: &kvrpcpb.KeyError{
						TxnLockNotFound: &kvrpcpb.TxnLockNotFound{
							Key: suite.codec.EncodeKey([]byte("response-lock")),
						},
					},
					Pairs: []*kvrpcpb.KvPair{
						{
							Key: suite.codec.EncodeKey([]byte("pair-key")),
							Error: &kvrpcpb.KeyError{
								TxnLockNotFound: &kvrpcpb.TxnLockNotFound{
									Key: suite.codec.EncodeKey([]byte("pair-lock")),
								},
							},
						},
					},
				},
			},
			validate: func(re *require.Assertions, decoded *tikvrpc.Response) {
				resp := decoded.Resp.(*kvrpcpb.BufferBatchGetResponse)
				re.Equal([]byte("region-key"), resp.RegionError.KeyNotInRegion.Key)
				re.Equal([]byte("range-start"), resp.RegionError.KeyNotInRegion.StartKey)
				re.Equal([]byte("range-end"), resp.RegionError.KeyNotInRegion.EndKey)
				re.Equal([]byte("response-lock"), resp.Error.TxnLockNotFound.Key)
				re.Len(resp.Pairs, 1)
				re.Equal([]byte("pair-key"), resp.Pairs[0].Key)
				re.Equal([]byte("pair-lock"), resp.Pairs[0].Error.TxnLockNotFound.Key)
			},
		},
	}

	for _, test := range tests {
		test := test
		suite.Run(test.name, func() {
			re := suite.Require()
			decoded, err := suite.codec.DecodeResponse(test.req, test.resp)
			re.NoError(err)
			test.validate(re, decoded)
		})
	}
}

func (suite *testCodecV2Suite) TestDecodeResponseSecondWaveCommands() {
	makeRegionError := func(key, start, end []byte) *errorpb.Error {
		encodedStart, encodedEnd := suite.codec.EncodeRegionRange(start, end)
		return &errorpb.Error{
			KeyNotInRegion: &errorpb.KeyNotInRegion{
				Key:      suite.codec.EncodeKey(key),
				StartKey: encodedStart,
				EndKey:   encodedEnd,
			},
		}
	}

	tests := []struct {
		name     string
		req      *tikvrpc.Request
		resp     *tikvrpc.Response
		validate func(*require.Assertions, *tikvrpc.Response)
	}{
		{
			name: "CmdCheckLockObserver",
			req: &tikvrpc.Request{
				Type: tikvrpc.CmdCheckLockObserver,
				Req:  &kvrpcpb.CheckLockObserverRequest{},
			},
			resp: &tikvrpc.Response{
				Resp: &kvrpcpb.CheckLockObserverResponse{
					Locks: []*kvrpcpb.LockInfo{
						{
							Key:         suite.codec.EncodeKey([]byte("observer-key")),
							PrimaryLock: suite.codec.EncodeKey([]byte("observer-primary")),
							Secondaries: [][]byte{suite.codec.EncodeKey([]byte("observer-secondary"))},
						},
					},
				},
			},
			validate: func(re *require.Assertions, decoded *tikvrpc.Response) {
				resp := decoded.Resp.(*kvrpcpb.CheckLockObserverResponse)
				re.Len(resp.Locks, 1)
				re.Equal([]byte("observer-key"), resp.Locks[0].Key)
				re.Equal([]byte("observer-primary"), resp.Locks[0].PrimaryLock)
				re.Equal([][]byte{[]byte("observer-secondary")}, resp.Locks[0].Secondaries)
			},
		},
		{
			name: "CmdLockWaitInfo",
			req: &tikvrpc.Request{
				Type: tikvrpc.CmdLockWaitInfo,
				Req:  &kvrpcpb.GetLockWaitInfoRequest{},
			},
			resp: &tikvrpc.Response{
				Resp: &kvrpcpb.GetLockWaitInfoResponse{
					RegionError: makeRegionError([]byte("region-key"), []byte("range-start"), []byte("range-end")),
					Entries: []*deadlockpb.WaitForEntry{
						{
							Key: suite.codec.EncodeKey([]byte("wait-key")),
						},
					},
				},
			},
			validate: func(re *require.Assertions, decoded *tikvrpc.Response) {
				resp := decoded.Resp.(*kvrpcpb.GetLockWaitInfoResponse)
				re.Equal([]byte("region-key"), resp.RegionError.KeyNotInRegion.Key)
				re.Equal([]byte("range-start"), resp.RegionError.KeyNotInRegion.StartKey)
				re.Equal([]byte("range-end"), resp.RegionError.KeyNotInRegion.EndKey)
				re.Len(resp.Entries, 1)
				re.Equal([]byte("wait-key"), resp.Entries[0].Key)
			},
		},
		{
			name: "CmdMvccGetByStartTs",
			req: &tikvrpc.Request{
				Type: tikvrpc.CmdMvccGetByStartTs,
				Req:  &kvrpcpb.MvccGetByStartTsRequest{},
			},
			resp: &tikvrpc.Response{
				Resp: &kvrpcpb.MvccGetByStartTsResponse{
					RegionError: makeRegionError([]byte("region-key"), []byte("range-start"), []byte("range-end")),
					Key:         suite.codec.EncodeKey([]byte("mvcc-key")),
					Info: &kvrpcpb.MvccInfo{
						Lock: &kvrpcpb.MvccLock{
							Primary: suite.codec.EncodeKey([]byte("mvcc-primary")),
							Secondaries: [][]byte{
								suite.codec.EncodeKey([]byte("mvcc-secondary-1")),
								suite.codec.EncodeKey([]byte("mvcc-secondary-2")),
							},
						},
					},
				},
			},
			validate: func(re *require.Assertions, decoded *tikvrpc.Response) {
				resp := decoded.Resp.(*kvrpcpb.MvccGetByStartTsResponse)
				re.Equal([]byte("region-key"), resp.RegionError.KeyNotInRegion.Key)
				re.Equal([]byte("range-start"), resp.RegionError.KeyNotInRegion.StartKey)
				re.Equal([]byte("range-end"), resp.RegionError.KeyNotInRegion.EndKey)
				re.Equal([]byte("mvcc-key"), resp.Key)
				re.Equal([]byte("mvcc-primary"), resp.Info.Lock.Primary)
				re.Equal([][]byte{
					[]byte("mvcc-secondary-1"),
					[]byte("mvcc-secondary-2"),
				}, resp.Info.Lock.Secondaries)
			},
		},
		{
			name: "CmdMvccGetByKey",
			req: &tikvrpc.Request{
				Type: tikvrpc.CmdMvccGetByKey,
				Req:  &kvrpcpb.MvccGetByKeyRequest{},
			},
			resp: &tikvrpc.Response{
				Resp: &kvrpcpb.MvccGetByKeyResponse{
					RegionError: makeRegionError([]byte("region-key"), []byte("range-start"), []byte("range-end")),
					Info: &kvrpcpb.MvccInfo{
						Lock: &kvrpcpb.MvccLock{
							Primary: suite.codec.EncodeKey([]byte("mvcc-by-key-primary")),
							Secondaries: [][]byte{
								suite.codec.EncodeKey([]byte("mvcc-by-key-secondary-1")),
								suite.codec.EncodeKey([]byte("mvcc-by-key-secondary-2")),
							},
						},
					},
				},
			},
			validate: func(re *require.Assertions, decoded *tikvrpc.Response) {
				resp := decoded.Resp.(*kvrpcpb.MvccGetByKeyResponse)
				re.Equal([]byte("region-key"), resp.RegionError.KeyNotInRegion.Key)
				re.Equal([]byte("range-start"), resp.RegionError.KeyNotInRegion.StartKey)
				re.Equal([]byte("range-end"), resp.RegionError.KeyNotInRegion.EndKey)
				re.Equal([]byte("mvcc-by-key-primary"), resp.Info.Lock.Primary)
				re.Equal([][]byte{
					[]byte("mvcc-by-key-secondary-1"),
					[]byte("mvcc-by-key-secondary-2"),
				}, resp.Info.Lock.Secondaries)
			},
		},
		{
			name: "CmdFlashbackToVersion",
			req: &tikvrpc.Request{
				Type: tikvrpc.CmdFlashbackToVersion,
				Req:  &kvrpcpb.FlashbackToVersionRequest{},
			},
			resp: &tikvrpc.Response{
				Resp: &kvrpcpb.FlashbackToVersionResponse{
					RegionError: makeRegionError([]byte("region-key"), []byte("range-start"), []byte("range-end")),
				},
			},
			validate: func(re *require.Assertions, decoded *tikvrpc.Response) {
				resp := decoded.Resp.(*kvrpcpb.FlashbackToVersionResponse)
				re.Equal([]byte("region-key"), resp.RegionError.KeyNotInRegion.Key)
				re.Equal([]byte("range-start"), resp.RegionError.KeyNotInRegion.StartKey)
				re.Equal([]byte("range-end"), resp.RegionError.KeyNotInRegion.EndKey)
			},
		},
		{
			name: "CmdPrepareFlashbackToVersion",
			req: &tikvrpc.Request{
				Type: tikvrpc.CmdPrepareFlashbackToVersion,
				Req:  &kvrpcpb.PrepareFlashbackToVersionRequest{},
			},
			resp: &tikvrpc.Response{
				Resp: &kvrpcpb.PrepareFlashbackToVersionResponse{
					RegionError: makeRegionError([]byte("region-key"), []byte("range-start"), []byte("range-end")),
				},
			},
			validate: func(re *require.Assertions, decoded *tikvrpc.Response) {
				resp := decoded.Resp.(*kvrpcpb.PrepareFlashbackToVersionResponse)
				re.Equal([]byte("region-key"), resp.RegionError.KeyNotInRegion.Key)
				re.Equal([]byte("range-start"), resp.RegionError.KeyNotInRegion.StartKey)
				re.Equal([]byte("range-end"), resp.RegionError.KeyNotInRegion.EndKey)
			},
		},
		{
			name: "CmdCompact",
			req: &tikvrpc.Request{
				Type: tikvrpc.CmdCompact,
				Req:  &kvrpcpb.CompactRequest{},
			},
			resp: &tikvrpc.Response{
				Resp: &kvrpcpb.CompactResponse{
					// Compact cursors are TiFlash RowKeyValue cursors, not API v2 storage keys.
					// TiFlash carries API v2 context separately through ApiVersion and KeyspaceId.
					CompactedStartKey: []byte("compacted-start"),
					CompactedEndKey:   []byte("compacted-end"),
				},
			},
			validate: func(re *require.Assertions, decoded *tikvrpc.Response) {
				resp := decoded.Resp.(*kvrpcpb.CompactResponse)
				re.Equal([]byte("compacted-start"), resp.CompactedStartKey)
				re.Equal([]byte("compacted-end"), resp.CompactedEndKey)
			},
		},
	}

	for _, test := range tests {
		test := test
		suite.Run(test.name, func() {
			re := suite.Require()
			decoded, err := suite.codec.DecodeResponse(test.req, test.resp)
			re.NoError(err)
			test.validate(re, decoded)
		})
	}
}

func (suite *testCodecV2Suite) TestDecodeMvccInfoPreservesEmptyLockKeys() {
	re := suite.Require()
	info := &kvrpcpb.MvccInfo{
		Lock: &kvrpcpb.MvccLock{
			Primary: []byte{},
			Secondaries: [][]byte{
				{},
				suite.codec.EncodeKey([]byte("mvcc-secondary")),
			},
		},
	}

	decoded, err := suite.codec.decodeMvccInfo(info)
	re.NoError(err)
	re.Same(info, decoded)
	re.NotNil(decoded.Lock.Primary)
	re.Empty(decoded.Lock.Primary)
	re.Len(decoded.Lock.Secondaries, 2)
	re.NotNil(decoded.Lock.Secondaries[0])
	re.Empty(decoded.Lock.Secondaries[0])
	re.Equal([]byte("mvcc-secondary"), decoded.Lock.Secondaries[1])
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
