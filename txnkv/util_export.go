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

import "github.com/tikv/client-go/v2/txnkv/txnutil"

// Priority is the priority for tikv to execute a command.
type Priority = txnutil.Priority

// Priority value for transaction priority.
const (
	PriorityHigh   = txnutil.PriorityHigh
	PriorityNormal = txnutil.PriorityNormal
	PriorityLow    = txnutil.PriorityLow
)
