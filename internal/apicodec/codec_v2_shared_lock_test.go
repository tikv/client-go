package apicodec

import (
	"testing"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util/intest"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func newSharedLockTestCodec(t *testing.T) *codecV2 {
	t.Helper()
	c, err := NewCodecV2(ModeTxn, &keyspacepb.KeyspaceMeta{
		Id: testKeyspaceID,
	})
	require.NoError(t, err)
	return c.(*codecV2)
}

func sharedLockTestInfo(c *codecV2) *kvrpcpb.LockInfo {
	return &kvrpcpb.LockInfo{
		Key:      c.EncodeKey([]byte("shared-key")),
		LockType: kvrpcpb.Op_SharedLock,
		SharedLockInfos: []*kvrpcpb.LockInfo{
			{
				Key:         c.EncodeKey([]byte("shared-key")),
				PrimaryLock: c.EncodeKey([]byte("primary-1")),
				LockType:    kvrpcpb.Op_Lock,
				LockVersion: 101,
				LockTtl:     3000,
			},
			{
				Key:         c.EncodeKey([]byte("shared-key")),
				PrimaryLock: c.EncodeKey([]byte("primary-2")),
				LockType:    kvrpcpb.Op_PessimisticLock,
				LockVersion: 202,
				LockTtl:     5000,
			},
		},
	}
}

func TestDecodeSharedLockInfoEmptyKeyWarnings(t *testing.T) {
	if intest.InTest {
		t.Skip("empty-key diagnostics require a build without the intest tag")
	}
	c := newSharedLockTestCodec(t)
	for _, test := range []struct {
		name     string
		modify   func(*kvrpcpb.LockInfo)
		warnings int
	}{
		{
			name:   "nil wrapper primary",
			modify: func(info *kvrpcpb.LockInfo) {},
		},
		{
			name:   "empty wrapper primary",
			modify: func(info *kvrpcpb.LockInfo) { info.PrimaryLock = []byte{} },
		},
		{
			name: "nonempty wrapper primary",
			modify: func(info *kvrpcpb.LockInfo) {
				info.PrimaryLock = c.EncodeKey([]byte("primary"))
			},
		},
		{
			name: "ordinary lock with nil primary",
			modify: func(info *kvrpcpb.LockInfo) {
				info.LockType = kvrpcpb.Op_PessimisticLock
				info.SharedLockInfos = nil
			},
			warnings: 1,
		},
		{
			name: "ordinary lock with empty primary",
			modify: func(info *kvrpcpb.LockInfo) {
				info.LockType = kvrpcpb.Op_Lock
				info.PrimaryLock = []byte{}
				info.SharedLockInfos = nil
			},
			warnings: 1,
		},
		{
			name:     "holder with empty primary",
			modify:   func(info *kvrpcpb.LockInfo) { info.SharedLockInfos[1].PrimaryLock = nil },
			warnings: 1,
		},
		{
			name:     "wrapper with empty key",
			modify:   func(info *kvrpcpb.LockInfo) { info.Key = nil },
			warnings: 1,
		},
		{
			name:     "holder with empty key",
			modify:   func(info *kvrpcpb.LockInfo) { info.SharedLockInfos[1].Key = nil },
			warnings: 1,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			// These tests must remain sequential because they replace the global logger.
			core, logs := observer.New(zap.WarnLevel)
			t.Cleanup(log.ReplaceGlobals(zap.New(core), &log.ZapProperties{Core: core}))
			info := sharedLockTestInfo(c)
			test.modify(info)
			hasPrimary := len(info.PrimaryLock) > 0
			decoded, err := c.decodeKeyError(&kvrpcpb.KeyError{Locked: info})
			require.NoError(t, err)
			if hasPrimary {
				require.Equal(t, []byte("primary"), decoded.Locked.PrimaryLock)
			} else {
				require.Empty(t, decoded.Locked.PrimaryLock)
			}
			warnings := logs.FilterMessage("codecV2.DecodeKey called with empty key. This shouldn't happen in prod")
			require.Equal(t, test.warnings, warnings.Len())
			require.Equal(t, test.warnings, logs.Len())
			for _, entry := range warnings.All() {
				require.Equal(t, zap.WarnLevel, entry.Level)
				require.NotEmpty(t, entry.ContextMap()["stack"])
			}
		})
	}
}

func TestDecodeSharedLockInfoInvalidKeyspace(t *testing.T) {
	c := newSharedLockTestCodec(t)
	for _, field := range []struct {
		name string
		set  func(*kvrpcpb.LockInfo, []byte)
	}{
		{"wrapper key", func(info *kvrpcpb.LockInfo, key []byte) { info.Key = key }},
		{"wrapper primary", func(info *kvrpcpb.LockInfo, key []byte) { info.PrimaryLock = key }},
		{"wrapper secondary", func(info *kvrpcpb.LockInfo, key []byte) { info.Secondaries = [][]byte{key} }},
		{"holder key", func(info *kvrpcpb.LockInfo, key []byte) { info.SharedLockInfos[1].Key = key }},
		{"holder primary", func(info *kvrpcpb.LockInfo, key []byte) { info.SharedLockInfos[1].PrimaryLock = key }},
		{"holder secondary", func(info *kvrpcpb.LockInfo, key []byte) { info.SharedLockInfos[1].Secondaries = [][]byte{key} }},
	} {
		for _, invalid := range []struct {
			name string
			key  []byte
		}{
			{"different keyspace", []byte{'x', 0, 16, 147, 'k'}},
			{"different mode", []byte{'r', 0, 16, 146, 'k'}},
			{"truncated prefix", []byte{'x', 0}},
		} {
			t.Run(field.name+"/"+invalid.name, func(t *testing.T) {
				core, logs := observer.New(zap.WarnLevel)
				t.Cleanup(log.ReplaceGlobals(zap.New(core), &log.ZapProperties{Core: core}))
				info := sharedLockTestInfo(c)
				field.set(info, invalid.key)
				decoded, err := c.decodeKeyError(&kvrpcpb.KeyError{Locked: info})
				require.ErrorIs(t, err, errKeyOutOfBound)
				require.Nil(t, decoded)
				require.Equal(t, 1, logs.FilterMessage("key not in keyspace").Len())
			})
		}
	}
}
