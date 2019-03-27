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
// See the License for the specific language governing permissions and
// limitations under the License.

package proxy

import (
	"errors"
	"sync"

	"github.com/google/uuid"
)

// Proxy errors. Use errors.Cause() to determine error type.
var (
	ErrClientNotFound = errors.New("client not found")
	ErrTxnNotFound    = errors.New("txn not found")
	ErrIterNotFound   = errors.New("iterator not found")
)

// UUID is a global unique ID to identify clients, transactions, or iterators.
type UUID string

func insertWithRetry(m *sync.Map, d interface{}) UUID {
	for {
		id := UUID(uuid.New().String())
		if _, hasOld := m.LoadOrStore(id, d); !hasOld {
			return id
		}
	}
}
