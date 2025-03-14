package redact

import (
	"encoding/hex"
	"unsafe"

	"go.uber.org/atomic"
)

// RedactLogEnabled defines whether the arguments of Error need to be redacted.
var RedactLogEnabled atomic.String

const (
	RedactLogEnable  string = "ON"
	RedactLogDisable string = "OFF"
	RedactLogMarker  string = "MARKER"
)

// NeedRedact returns whether to redact log
func NeedRedact() bool {
	mode := RedactLogEnabled.Load()
	return mode != RedactLogDisable && mode != ""
}

// Key receives a key return omitted information if redact log enabled
func Key(key []byte) string {
	if NeedRedact() {
		return "?"
	}
	return String(ToUpperASCIIInplace(EncodeToString(key)))
}

// KeyBytes receives a key return omitted information if redact log enabled
func KeyBytes(key []byte) []byte {
	if NeedRedact() {
		return []byte{'?'}
	}
	return ToUpperASCIIInplace(EncodeToString(key))
}

// String converts slice of bytes to string without copy.
func String(b []byte) (s string) {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}

// EncodeToString overrides hex.EncodeToString implementation. Difference: returns []byte, not string
func EncodeToString(src []byte) []byte {
	dst := make([]byte, hex.EncodedLen(len(src)))
	hex.Encode(dst, src)
	return dst
}

// ToUpperASCIIInplace bytes.ToUpper but zero-cost
func ToUpperASCIIInplace(s []byte) []byte {
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
