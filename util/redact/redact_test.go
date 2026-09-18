package redact

import (
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/require"
)

func TestRedactKeyErrSharedLockLostAndLockUpgradeConflict(t *testing.T) {
	originalMode := errors.RedactLogEnabled.Load()
	t.Cleanup(func() { errors.RedactLogEnabled.Store(originalMode) })

	newKeyErr := func() *kvrpcpb.KeyError {
		return &kvrpcpb.KeyError{
			SharedLockLost: &kvrpcpb.SharedLockLost{Key: []byte("lost-key")},
			LockUpgradeConflict: &kvrpcpb.LockUpgradeConflict{
				Key: []byte("conflict-key"),
			},
		}
	}

	errors.RedactLogEnabled.Store(errors.RedactLogDisable)
	unredacted := newKeyErr()
	RedactKeyErrIfNecessary(unredacted)
	require.Equal(t, []byte("lost-key"), unredacted.SharedLockLost.Key)
	require.Equal(t, []byte("conflict-key"), unredacted.LockUpgradeConflict.Key)

	errors.RedactLogEnabled.Store(errors.RedactLogEnable)
	redacted := newKeyErr()
	RedactKeyErrIfNecessary(redacted)
	require.Equal(t, []byte("?"), redacted.SharedLockLost.Key)
	require.Equal(t, []byte("?"), redacted.LockUpgradeConflict.Key)
}
