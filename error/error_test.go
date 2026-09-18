package error

import (
	stderrs "errors"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestErrIncompatibleRequest(t *testing.T) {
	inner := &errorpb.IncompatibleRequest{
		Reason:                          errorpb.IncompatibleRequestReason_IncompatibleRequestReasonTxnProtocolVersionOutOfRange,
		Message:                         "txn protocol version 2 is out of range",
		ProvidedTxnProtocolVersion:      uint32(kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_SHARED_LOCK),
		MinCompatibleTxnProtocolVersion: 0,
		MaxCompatibleTxnProtocolVersion: uint32(kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_INCOMPATIBLE_ERROR_HANDLING),
	}
	var typed *ErrIncompatibleRequest
	require.ErrorAs(t, NewErrIncompatibleRequest(inner), &typed)
	require.Equal(t, inner, typed.IncompatibleRequest)
	require.Equal(t, inner.GetReason(), typed.GetReason())
	require.Equal(t, inner.GetMaxCompatibleTxnProtocolVersion(), typed.GetMaxCompatibleTxnProtocolVersion())

	// The message must remain diagnosable, and must not be the only carrier of
	// the structured details. It uses the proto text form of the payload, so the
	// exact shape is proto-owned rather than asserted here.
	msg := typed.Error()
	require.Contains(t, msg, inner.GetMessage())
	require.Contains(t, msg, inner.GetReason().String())
	require.Contains(t, msg, "provided_txn_protocol_version:2")
	require.Contains(t, msg, "max_compatible_txn_protocol_version:1")

	require.NotPanics(t, func() {
		require.NotEmpty(t, (&ErrIncompatibleRequest{}).Error())
	})
}

func TestExtractKeyErrLockUpgradeConflict(t *testing.T) {
	keyErr := &kvrpcpb.KeyError{
		LockUpgradeConflict: &kvrpcpb.LockUpgradeConflict{
			Key:          []byte("key"),
			StartTs:      101,
			OwnerStartTs: 202,
			Reason:       kvrpcpb.LockUpgradeConflict_SecondUpgrader,
		},
	}

	err := ExtractKeyErr(keyErr)
	require.Error(t, err)
	require.False(t, IsErrWriteConflict(err))

	var retryable *ErrRetryable
	require.False(t, stderrs.As(err, &retryable))

	var conflict *ErrLockUpgradeConflict
	require.ErrorAs(t, err, &conflict)
	require.Equal(t, []byte("key"), conflict.Key)
	require.Equal(t, uint64(101), conflict.StartTs)
	require.Equal(t, uint64(202), conflict.OwnerStartTs)
	require.Equal(t, kvrpcpb.LockUpgradeConflict_SecondUpgrader, conflict.Reason)
}

func TestExtractKeyErrSharedLockLost(t *testing.T) {
	keyErr := &kvrpcpb.KeyError{
		SharedLockLost: &kvrpcpb.SharedLockLost{
			Key:     []byte("key"),
			StartTs: 101,
		},
		Conflict: &kvrpcpb.WriteConflict{
			StartTs: 101,
			Reason:  kvrpcpb.WriteConflict_PessimisticRetry,
		},
	}

	err := ExtractKeyErr(keyErr)
	require.Error(t, err)
	require.False(t, IsErrWriteConflict(err))
	require.False(t, IsErrorUndetermined(err))

	var retryable *ErrRetryable
	require.False(t, stderrs.As(err, &retryable))
	var deadlock *ErrDeadlock
	require.False(t, stderrs.As(err, &deadlock))

	var lost *ErrSharedLockLost
	require.ErrorAs(t, err, &lost)
	require.Equal(t, []byte("key"), lost.Key)
	require.Equal(t, uint64(101), lost.StartTs)
}

func TestExtractDebugInfoStrFromKeyErr(t *testing.T) {
	origRedact := errors.RedactLogEnabled.Load()
	defer errors.RedactLogEnabled.Store(origRedact)

	errors.RedactLogEnabled.Store(errors.RedactLogDisable)
	// empty debug info
	assert.Equal(t, "", ExtractDebugInfoStrFromKeyErr(&kvrpcpb.KeyError{
		TxnLockNotFound: &kvrpcpb.TxnLockNotFound{Key: []byte("byte")},
	}))
	// non-empty debug info
	debugInfo := &kvrpcpb.DebugInfo{
		MvccInfo: []*kvrpcpb.MvccDebugInfo{
			{
				Key: []byte("byte"),
				Mvcc: &kvrpcpb.MvccInfo{
					Lock: &kvrpcpb.MvccLock{
						Type:    kvrpcpb.Op_Del,
						StartTs: 128,
						Primary: []byte("k1"),
						Secondaries: [][]byte{
							[]byte("k1"),
							[]byte("k2"),
						},
						ShortValue: []byte("v1"),
					},
					Writes: []*kvrpcpb.MvccWrite{
						{
							Type:       kvrpcpb.Op_Insert,
							StartTs:    64,
							CommitTs:   86,
							ShortValue: []byte{0x1, 0x2, 0x3, 0x4, 0x5, 0x6},
						},
					},
					Values: []*kvrpcpb.MvccValue{
						{
							StartTs: 64,
							Value:   []byte{0x11, 0x12},
						},
					},
				},
			},
		},
	}

	expectedStr := `{"mvcc_info":[{"key":"Ynl0ZQ==","mvcc":{"lock":{"type":1,"start_ts":128,"primary":"azE=","short_value":"djE=","secondaries":["azE=","azI="]},"writes":[{"type":4,"start_ts":64,"commit_ts":86,"short_value":"AQIDBAUG"}],"values":[{"start_ts":64,"value":"ERI="}]}}]}`
	assert.Equal(t, expectedStr, ExtractDebugInfoStrFromKeyErr(&kvrpcpb.KeyError{
		TxnLockNotFound: &kvrpcpb.TxnLockNotFound{Key: []byte("byte")},
		DebugInfo:       debugInfo,
	}))

	// redact log enabled
	errors.RedactLogEnabled.Store(errors.RedactLogEnable)
	expectedStr = `{"mvcc_info":[{"key":"Pw==","mvcc":{"lock":{"type":1,"start_ts":128,"primary":"Pw==","short_value":"Pw==","secondaries":["Pw==","Pw=="]},"writes":[{"type":4,"start_ts":64,"commit_ts":86,"short_value":"Pw=="}],"values":[{"start_ts":64,"value":"Pw=="}]}}]}`
	assert.Equal(t, expectedStr, ExtractDebugInfoStrFromKeyErr(&kvrpcpb.KeyError{
		TxnLockNotFound: &kvrpcpb.TxnLockNotFound{Key: []byte("byte")},
		DebugInfo:       debugInfo,
	}))
}
