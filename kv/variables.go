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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/kv/variables.go
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

package kv

// Variables defines the variables used by KV storage.
type Variables struct {
	// BackoffLockFast specifies the LockFast backoff base duration in milliseconds.
	BackoffLockFast int

	// BackOffWeight specifies the weight of the max back off time duration.
	BackOffWeight int

	// Pointer to SessionVars.Killed
	// Killed is a flag to indicate that this query is killed.
	// This is an enum value rather than a boolean. See sqlkiller.go
	// in TiDB for its definition.
	// When its value is 0, it's not killed
	// When its value is not 0, it's killed, the value indicates concrete reason.
	Killed *uint32

	// DisableTxnFile specifies whether file-based txn is disabled.
	DisableTxnFile bool

	// EnableColumnarExecution specifies whether columnar execution is enabled.
	EnableColumnarExecution bool
}

// NewVariables create a new Variables instance with default values.
func NewVariables(killed *uint32) *Variables {
	return &Variables{
		BackoffLockFast:         DefBackoffLockFast,
		BackOffWeight:           DefBackOffWeight,
		Killed:                  killed,
		DisableTxnFile:          false,
		EnableColumnarExecution: false,
	}
}

var ignoreKill uint32

// DefaultVars is the default variables instance.
var DefaultVars = NewVariables(&ignoreKill)

// Default values
const (
	DefBackoffLockFast = 10
	DefBackOffWeight   = 2
)
