package redact

import (
	"encoding/hex"
	"fmt"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
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
	if e := err.SharedLockLost; e != nil {
		if len(e.Key) > 0 {
			e.Key = redactMarker
		}
	}
	if e := err.LockUpgradeConflict; e != nil {
		if len(e.Key) > 0 {
			e.Key = redactMarker
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

// Stringer returns s itself if redact log is disabled, otherwise it returns a
// fmt.Stringer that prints only the redact marker. It is intended for values
// such as kvrpcpb requests whose String() may contain user keys or values.
func Stringer(s fmt.Stringer) fmt.Stringer {
	if !NeedRedact() {
		return s
	}
	return redactedStringer{}
}

type redactedStringer struct{}

func (redactedStringer) String() string { return "?" }

// RegionMeta returns region itself if redact log is disabled, otherwise it
// returns a shallow copy of region whose start key and end key are redacted.
// The returned value must only be used for logging.
func RegionMeta(region *metapb.Region) *metapb.Region {
	if region == nil || !NeedRedact() {
		return region
	}
	return redactRegionMeta(region)
}

// RegionMetas is like RegionMeta, but for a slice of regions.
func RegionMetas(regions []*metapb.Region) []*metapb.Region {
	if len(regions) == 0 || !NeedRedact() {
		return regions
	}
	return redactRegionMetas(regions)
}

// EpochNotMatch returns e itself if redact log is disabled, otherwise it
// returns a copy of e whose current regions have redacted start and end keys.
// The returned value must only be used for logging.
func EpochNotMatch(e *errorpb.EpochNotMatch) *errorpb.EpochNotMatch {
	if e == nil || !NeedRedact() {
		return e
	}
	return redactEpochNotMatch(e)
}

// RegionError returns e itself if redact log is disabled, otherwise it returns
// a shallow copy of e whose key fields (KeyNotInRegion, EpochNotMatch and
// BucketVersionNotMatch) are redacted. The returned value must only be used
// for logging.
func RegionError(e *errorpb.Error) *errorpb.Error {
	if e == nil || !NeedRedact() {
		return e
	}
	redacted := *e
	if k := e.KeyNotInRegion; k != nil {
		kc := *k
		kc.Key = redactBytes(kc.Key)
		kc.StartKey = redactBytes(kc.StartKey)
		kc.EndKey = redactBytes(kc.EndKey)
		redacted.KeyNotInRegion = &kc
	}
	if en := e.EpochNotMatch; en != nil {
		redacted.EpochNotMatch = redactEpochNotMatch(en)
	}
	if b := e.BucketVersionNotMatch; b != nil {
		bc := *b
		bc.Keys = make([][]byte, len(b.Keys))
		for i := range b.Keys {
			bc.Keys[i] = redactBytes(b.Keys[i])
		}
		redacted.BucketVersionNotMatch = &bc
	}
	return &redacted
}

func redactRegionMeta(region *metapb.Region) *metapb.Region {
	if region == nil {
		return nil
	}
	redacted := *region
	redacted.StartKey = redactBytes(region.StartKey)
	redacted.EndKey = redactBytes(region.EndKey)
	return &redacted
}

func redactRegionMetas(regions []*metapb.Region) []*metapb.Region {
	redacted := make([]*metapb.Region, len(regions))
	for i, r := range regions {
		redacted[i] = redactRegionMeta(r)
	}
	return redacted
}

func redactEpochNotMatch(e *errorpb.EpochNotMatch) *errorpb.EpochNotMatch {
	redacted := *e
	redacted.CurrentRegions = redactRegionMetas(e.CurrentRegions)
	return &redacted
}

// redactBytes replaces a non-empty key with the redact marker. Empty keys are
// kept as is, since they carry no user data and mark unbounded ranges.
func redactBytes(key []byte) []byte {
	if len(key) == 0 {
		return key
	}
	return []byte{'?'}
}
