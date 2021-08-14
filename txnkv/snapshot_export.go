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

package txnkv

import (
	"github.com/tikv/client-go/v2/txnkv/txnsnapshot"
)

// Scanner support tikv scan
type Scanner = txnsnapshot.Scanner

// KVSnapshot implements the tidbkv.Snapshot interface.
type KVSnapshot = txnsnapshot.KVSnapshot

// SnapshotRuntimeStats records the runtime stats of snapshot.
type SnapshotRuntimeStats = txnsnapshot.SnapshotRuntimeStats

// IsoLevel is the transaction's isolation level.
type IsoLevel = txnsnapshot.IsoLevel

// IsoLevel value for transaction priority.
const (
	SI = txnsnapshot.SI
	RC = txnsnapshot.RC
)
