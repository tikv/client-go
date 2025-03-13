package redact

import (
	"encoding/hex"
	"strings"

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
	return strings.ToUpper(hex.EncodeToString(key))
}
