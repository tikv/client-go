package error

import (
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/assert"
)

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

	expectedStr := `{"mvcc_info":[{"key":"Ynl0ZQ==","mvcc":{"lock":{"type":1,"start_ts":128},"writes":[{"type":4,"start_ts":64,"commit_ts":86,"short_value":"AQIDBAUG"}],"values":[{"start_ts":64,"value":"ERI="}]}}]}`
	assert.Equal(t, expectedStr, ExtractDebugInfoStrFromKeyErr(&kvrpcpb.KeyError{
		TxnLockNotFound: &kvrpcpb.TxnLockNotFound{Key: []byte("byte")},
		DebugInfo:       debugInfo,
	}))

	// redact log enabled
	errors.RedactLogEnabled.Store(errors.RedactLogEnable)
	assert.Equal(t, "?", ExtractDebugInfoStrFromKeyErr(&kvrpcpb.KeyError{
		TxnLockNotFound: &kvrpcpb.TxnLockNotFound{Key: []byte("byte")},
		DebugInfo:       debugInfo,
	}))
}
