// Copyright 2021 TiKV Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// NOTE: The code in this file is based on code from the
// TiDB project, licensed under the Apache License v 2.0
//
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/util/misc.go
//

// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"context"
	"encoding/hex"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/internal/logutil"
	"go.uber.org/zap"
)

// GCTimeFormat is the format that gc_worker used to store times.
const GCTimeFormat = "20060102-15:04:05 -0700"

// CompatibleParseGCTime parses a string with `GCTimeFormat` and returns a time.Time. If `value` can't be parsed as that
// format, truncate to last space and try again. This function is only useful when loading times that saved by
// gc_worker. We have changed the format that gc_worker saves time (removed the last field), but when loading times it
// should be compatible with the old format.
func CompatibleParseGCTime(value string) (time.Time, error) {
	t, err := time.Parse(GCTimeFormat, value)

	if err != nil {
		// Remove the last field that separated by space
		parts := strings.Split(value, " ")
		prefix := strings.Join(parts[:len(parts)-1], " ")
		t, err = time.Parse(GCTimeFormat, prefix)
	}

	if err != nil {
		err = errors.Errorf("string \"%v\" doesn't has a prefix that matches format \"%v\"", value, GCTimeFormat)
	}
	return t, err
}

// WithRecovery wraps goroutine startup call with force recovery.
// it will dump current goroutine stack into log if catch any recover result.
//   exec:      execute logic function.
//   recoverFn: handler will be called after recover and before dump stack, passing `nil` means noop.
func WithRecovery(exec func(), recoverFn func(r interface{})) {
	defer func() {
		r := recover()
		if recoverFn != nil {
			recoverFn(r)
		}
		if r != nil {
			logutil.BgLogger().Error("panic in the recoverable goroutine",
				zap.Reflect("r", r),
				zap.Stack("stack trace"))
		}
	}()
	exec()
}

type sessionIDCtxKey struct{}

// SessionID is the context key type to mark a session.
var SessionID = sessionIDCtxKey{}

// SetSessionID sets session id into context
func SetSessionID(ctx context.Context, sessionID uint64) context.Context {
	return context.WithValue(ctx, SessionID, sessionID)
}

const (
	byteSizeGB = int64(1 << 30)
	byteSizeMB = int64(1 << 20)
	byteSizeKB = int64(1 << 10)
	byteSizeBB = int64(1)
)

// FormatBytes uses to format bytes, this function will prune precision before format bytes.
func FormatBytes(numBytes int64) string {
	if numBytes <= byteSizeKB {
		return BytesToString(numBytes)
	}
	unit, unitStr := getByteUnit(numBytes)
	if unit == byteSizeBB {
		return BytesToString(numBytes)
	}
	v := float64(numBytes) / float64(unit)
	decimal := 1
	if numBytes%unit == 0 {
		decimal = 0
	} else if v < 10 {
		decimal = 2
	}
	return fmt.Sprintf("%v %s", strconv.FormatFloat(v, 'f', decimal, 64), unitStr)
}

func getByteUnit(b int64) (int64, string) {
	if b > byteSizeGB {
		return byteSizeGB, "GB"
	} else if b > byteSizeMB {
		return byteSizeMB, "MB"
	} else if b > byteSizeKB {
		return byteSizeKB, "KB"
	}
	return byteSizeBB, "Bytes"
}

// BytesToString converts the memory consumption to a readable string.
func BytesToString(numBytes int64) string {
	GB := float64(numBytes) / float64(byteSizeGB)
	if GB > 1 {
		return fmt.Sprintf("%v GB", GB)
	}

	MB := float64(numBytes) / float64(byteSizeMB)
	if MB > 1 {
		return fmt.Sprintf("%v MB", MB)
	}

	KB := float64(numBytes) / float64(byteSizeKB)
	if KB > 1 {
		return fmt.Sprintf("%v KB", KB)
	}

	return fmt.Sprintf("%v Bytes", numBytes)
}

// String converts slice of bytes to string without copy.
func String(b []byte) (s string) {
	if len(b) == 0 {
		return ""
	}
	pbytes := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	pstring := (*reflect.StringHeader)(unsafe.Pointer(&s))
	pstring.Data = pbytes.Data
	pstring.Len = pbytes.Len
	return
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

// EncodeToString overrides hex.EncodeToString implementation. Difference: returns []byte, not string
func EncodeToString(src []byte) []byte {
	dst := make([]byte, hex.EncodedLen(len(src)))
	hex.Encode(dst, src)
	return dst
}

// HexRegionKey converts region key to hex format. Used for formating region in
// logs.
func HexRegionKey(key []byte) []byte {
	return ToUpperASCIIInplace(EncodeToString(key))
}

// HexRegionKeyStr converts region key to hex format. Used for formating region in
// logs.
func HexRegionKeyStr(key []byte) string {
	return String(HexRegionKey(key))
}
