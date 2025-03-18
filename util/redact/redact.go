package redact

import (
	"encoding/hex"
	"unsafe"

	"github.com/pingcap/errors"
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
