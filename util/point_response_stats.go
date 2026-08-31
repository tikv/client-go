// Copyright 2026 TiKV Authors
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
	"math"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
)

// PointReadScanDetail is the storage work reported by point-read responses.
type PointReadScanDetail struct {
	// TotalKeys is the total number of MVCC versions encountered by storage.
	TotalKeys int64
	// ProcessedKeys is the number of user keys processed by storage.
	ProcessedKeys int64
	// ProcessedKeysSize is the total size of the processed user keys and values.
	ProcessedKeysSize int64
}

// PointResponseStats is a value snapshot of Get, BatchGet, and BufferBatchGet
// response statistics. Its zero value is valid but has no response coverage.
// Use RecordResponse and Merge to populate both the values and coverage state.
// The value may be copied; callers must synchronize concurrent reads and writes.
type PointResponseStats struct {
	// ScanDetail is the aggregated storage work from ScanDetailV2 records.
	ScanDetail PointReadScanDetail
	// PayloadBytes is the logical successful-response payload, not the encoded
	// protobuf or network response size. Get contributes value bytes.
	// BatchGet and BufferBatchGet contribute key plus value bytes for successful
	// pairs. Misses, response errors, and pair errors contribute zero; commit
	// timestamps, error text, protobuf fields, and transport framing are excluded.
	PayloadBytes uint64

	seenResponse      bool
	missingScanDetail bool
	invalid           bool
}

// IsValid reports whether the snapshot has not been invalidated. Missing scan
// detail alone does not invalidate it; use ScanDetailComplete to check coverage.
func (stats PointResponseStats) IsValid() bool {
	return !stats.invalid
}

// ScanDetailComplete reports whether at least one recognized response was
// observed and every recognized response carried ScanDetailV2. A valid zero
// response snapshot is not complete evidence that a point-read operation ran.
// Complete message coverage does not prove that every protobuf scalar field is
// supported by the backend because those fields do not carry presence.
func (stats PointResponseStats) ScanDetailComplete() bool {
	return stats.IsValid() && stats.seenResponse && !stats.missingScanDetail
}

// PayloadComplete reports whether at least one recognized response was observed
// and every recognized response had its payload accounted for. This includes
// zero-byte misses and errors, but does not cover transport failures or
// unrecognized responses.
func (stats PointResponseStats) PayloadComplete() bool {
	return stats.IsValid() && stats.seenResponse
}

// Invalidate marks the snapshot invalid. Recording or merging further responses
// cannot make it valid again.
func (stats *PointResponseStats) Invalidate() {
	stats.invalid = true
}

// RecordResponse records one recognized response after transport and region
// errors have been handled. Empty and key-error responses must also be recorded,
// and each retry response is recorded separately. A nil scanDetail records
// missing scan detail. A false payloadValid or any numeric overflow invalidates
// the snapshot without adding the response's values.
func (stats *PointResponseStats) RecordResponse(scanDetail *kvrpcpb.ScanDetailV2, payloadBytes uint64, payloadValid bool) {
	if !payloadValid {
		stats.Invalidate()
		return
	}
	delta := PointResponseStats{
		PayloadBytes:      payloadBytes,
		seenResponse:      true,
		missingScanDetail: scanDetail == nil,
	}
	if scanDetail != nil {
		if scanDetail.TotalVersions > math.MaxInt64 ||
			scanDetail.ProcessedVersions > math.MaxInt64 ||
			scanDetail.ProcessedVersionsSize > math.MaxInt64 {
			stats.Invalidate()
			return
		}
		delta.ScanDetail = PointReadScanDetail{
			TotalKeys:         int64(scanDetail.TotalVersions),
			ProcessedKeys:     int64(scanDetail.ProcessedVersions),
			ProcessedKeysSize: int64(scanDetail.ProcessedVersionsSize),
		}
	}
	stats.Merge(delta)
}

// Merge adds another snapshot's values and preserves missing-detail and invalid
// states. Invalid input or arithmetic overflow invalidates the receiver without
// changing its accumulated values. Merging a zero-value snapshot is a no-op.
func (stats *PointResponseStats) Merge(other PointResponseStats) {
	if !stats.IsValid() || !other.IsValid() {
		stats.Invalidate()
		return
	}
	totalKeys, totalOK := checkedAddNonNegativeInt64(stats.ScanDetail.TotalKeys, other.ScanDetail.TotalKeys)
	processedKeys, processedOK := checkedAddNonNegativeInt64(stats.ScanDetail.ProcessedKeys, other.ScanDetail.ProcessedKeys)
	processedSize, sizeOK := checkedAddNonNegativeInt64(stats.ScanDetail.ProcessedKeysSize, other.ScanDetail.ProcessedKeysSize)
	if !totalOK || !processedOK || !sizeOK || other.PayloadBytes > math.MaxUint64-stats.PayloadBytes {
		stats.Invalidate()
		return
	}
	*stats = PointResponseStats{
		ScanDetail: PointReadScanDetail{
			TotalKeys:         totalKeys,
			ProcessedKeys:     processedKeys,
			ProcessedKeysSize: processedSize,
		},
		PayloadBytes:      stats.PayloadBytes + other.PayloadBytes,
		seenResponse:      stats.seenResponse || other.seenResponse,
		missingScanDetail: stats.missingScanDetail || other.missingScanDetail,
	}
}

func checkedAddNonNegativeInt64(current, delta int64) (int64, bool) {
	if current < 0 || delta < 0 || delta > math.MaxInt64-current {
		return 0, false
	}
	return current + delta, true
}
