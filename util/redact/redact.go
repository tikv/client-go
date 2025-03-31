package redact

import (
	"encoding/hex"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
)

// NeedRedact returns whether to redact log
func NeedRedact() bool {
	mode := errors.RedactLogEnabled.Load()
	return mode != errors.RedactLogDisable && mode != ""
}

// Key receives a key return omitted information if redact log enabled
func Key(key []byte) string {
	if NeedRedact() {
		return "?"
	}
	return String(toUpperASCIIInplace(encodeToString(key)))
}

// KeyBytes receives a key return omitted information if redact log enabled
func KeyBytes(key []byte) []byte {
	if NeedRedact() {
		return []byte{'?'}
	}
	return toUpperASCIIInplace(encodeToString(key))
}

// String converts slice of bytes to string without copy.
func String(b []byte) (s string) {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}

// encodeToString overrides hex.encodeToString implementation. Difference: returns []byte, not string
func encodeToString(src []byte) []byte {
	dst := make([]byte, hex.EncodedLen(len(src)))
	hex.Encode(dst, src)
	return dst
}

// toUpperASCIIInplace bytes.ToUpper but zero-cost
func toUpperASCIIInplace(s []byte) []byte {
	hasLower := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		hasLower = hasLower || ('a' <= c && c <= 'z')
	}

	if !hasLower {
		return s
	}
	var c byte
	for i := 0; i < len(s); i++ {
		c = s[i]
		if 'a' <= c && c <= 'z' {
			c -= 'a' - 'A'
		}
		s[i] = c
	}
	return s
}

// RedactKeyErrIfNecessary redact the key in *kvrpcpb.KeyError.
func RedactKeyErrIfNecessary(err *kvrpcpb.KeyError) {
	if !NeedRedact() {
		return
	}
	redactMarker := []byte{'?'}
	if e := err.Locked; e != nil {
		if len(e.PrimaryLock) > 0 {
			e.PrimaryLock = redactMarker
		}
		if len(e.Key) > 0 {
			e.Key = redactMarker
		}
		for i := range e.Secondaries {
			e.Secondaries[i] = redactMarker
		}
	}
	if e := err.Conflict; e != nil {
		if len(e.Key) > 0 {
			e.Key = redactMarker
		}
		if len(e.Primary) > 0 {
			e.Primary = redactMarker
		}
	}
	if e := err.AlreadyExist; e != nil {
		if len(e.Key) > 0 {
			e.Key = redactMarker
		}
	}
	if e := err.Deadlock; e != nil {
		if len(e.LockKey) > 0 {
			e.LockKey = redactMarker
		}
		if len(e.DeadlockKey) > 0 {
			e.DeadlockKey = redactMarker
		}
		for i := range e.WaitChain {
			if len(e.WaitChain[i].Key) > 0 {
				e.WaitChain[i].Key = redactMarker
			}
		}
	}
	if e := err.CommitTsExpired; e != nil {
		if len(e.Key) > 0 {
			e.Key = redactMarker
		}
	}
	if e := err.TxnNotFound; e != nil {
		if len(e.PrimaryKey) > 0 {
			e.PrimaryKey = redactMarker
		}
	}
	if e := err.AssertionFailed; e != nil {
		if len(e.Key) > 0 {
			e.Key = redactMarker
		}
	}
	if mismatchErr := err.PrimaryMismatch; mismatchErr != nil {
		if e := mismatchErr.LockInfo; e != nil {
			if len(e.PrimaryLock) > 0 {
				e.PrimaryLock = redactMarker
			}
			if len(e.Key) > 0 {
				e.Key = redactMarker
			}
			for i := range e.Secondaries {
				e.Secondaries[i] = redactMarker
			}
		}
	}
}
