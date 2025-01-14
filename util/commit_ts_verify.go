// Copyright 2025 TiKV Authors
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
	"fmt"
	"sync/atomic"
)

// CommitInfo stores the information of a COMMITTED transaction.
type CommitInfo struct {
	TxnType     string
	StartTS     uint64
	CommitTS    uint64
	MutationLen int
	TxnSize     int
	Primary     []byte
}

// String returns the string representation of CommitInfo.
func (c *CommitInfo) String() string {
	return fmt.Sprintf("TxnType: %s, StartTS: %d, CommitTS: %d, MutationLen: %d, TxnSize: %d, Primary: %v",
		c.TxnType, c.StartTS, c.CommitTS, c.MutationLen, c.TxnSize, c.Primary)
}

// Verify checks validation of this commit information from the given ts.
func (c *CommitInfo) Verify(ts uint64) {
	if ts < c.CommitTS || ts <= c.StartTS {
		panic(fmt.Sprintf("Verified ts: %d, LastCommit: %s", ts, c.String()))
	}
}

// TSVerifier is used to verify the commit ts.
type TSVerifier struct {
	lastCommitInfo atomic.Pointer[CommitInfo]
}

// NewTSVerifier creates a new TSVerifier.
func NewTSVerifier() *TSVerifier {
	return &TSVerifier{}
}

// SetLastCommitInfo stores the commit information of a transaction.
func (t *TSVerifier) SetLastCommitInfo(commitInfo *CommitInfo) {
	for {
		last := t.lastCommitInfo.Load()
		if last != nil && commitInfo.CommitTS <= last.CommitTS {
			return
		}
		if t.lastCommitInfo.CompareAndSwap(last, commitInfo) {
			return
		}
	}
}

// GetLastCommitInfo gets the last commit information of a transaction.
func (t *TSVerifier) GetLastCommitInfo() *CommitInfo {
	return t.lastCommitInfo.Load()
}
