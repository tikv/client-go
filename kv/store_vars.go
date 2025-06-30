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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/kv/store_vars.go
//

// Copyright 2019 PingCAP, Inc.
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

import (
	"fmt"

	"go.uber.org/atomic"
)

// StoreLimit will update from config reload and global variable set.
var StoreLimit atomic.Int64

// DefTxnCommitBatchSize is the default value of TxnCommitBatchSize.
const DefTxnCommitBatchSize uint64 = 16 * 1024

// TxnCommitBatchSize controls the batch size of transaction commit related requests sent by client to TiKV,
// TiKV recommends each RPC packet should be less than ~1MB.
var TxnCommitBatchSize atomic.Uint64

func init() {
	TxnCommitBatchSize.Store(DefTxnCommitBatchSize)
}

// ReplicaReadType is the type of replica to read data from
type ReplicaReadType byte

const (
	// ReplicaReadLeader stands for 'read from leader'.
	ReplicaReadLeader ReplicaReadType = iota
	// ReplicaReadFollower stands for 'read from follower'.
	ReplicaReadFollower
	// ReplicaReadMixed stands for 'read from leader and follower and learner'.
	ReplicaReadMixed
	// ReplicaReadLearner stands for 'read from learner'.
	ReplicaReadLearner
	// ReplicaReadPreferLeader stands for 'read from leader and auto-turn to followers if leader is abnormal'.
	ReplicaReadPreferLeader
)

// IsFollowerRead checks if follower is going to be used to read data.
func (r ReplicaReadType) IsFollowerRead() bool {
	return r != ReplicaReadLeader
}

// String implements fmt.Stringer interface.
func (r ReplicaReadType) String() string {
	switch r {
	case ReplicaReadLeader:
		return "leader"
	case ReplicaReadFollower:
		return "follower"
	case ReplicaReadMixed:
		return "mixed"
	case ReplicaReadLearner:
		return "learner"
	case ReplicaReadPreferLeader:
		return "prefer-leader"
	default:
		return fmt.Sprintf("unknown-%v", byte(r))
	}
}
