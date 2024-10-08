// Copyright 2024 TiKV Authors
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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/tests/snapshot_test.go
//

// Copyright 2016 PingCAP, Inc.
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

package transaction

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMinCommitTsManager(t *testing.T) {
	t.Run(
		"Initial state", func(t *testing.T) {
			manager := newMinCommitTsManager()
			assert.Equal(t, uint64(0), manager.get(), "Initial value should be 0")
			assert.Equal(
				t,
				ttlAccess,
				manager.getRequiredWriteAccess(),
				"Initial write access should be ttlAccess",
			)
		},
	)

	t.Run(
		"TTL updates", func(t *testing.T) {
			manager := newMinCommitTsManager()

			manager.tryUpdate(10, ttlAccess)
			assert.Equal(t, uint64(10), manager.get(), "Value should be 10")

			manager.tryUpdate(5, ttlAccess)
			assert.Equal(t, uint64(10), manager.get(), "Value should remain 10")
		},
	)

	t.Run(
		"Elevate write access", func(t *testing.T) {
			manager := newMinCommitTsManager()
			manager.tryUpdate(10, ttlAccess)

			currentValue := manager.elevateWriteAccess(twoPCAccess)
			assert.Equal(t, uint64(10), currentValue, "Current value should be 10")
			assert.Equal(
				t,
				twoPCAccess,
				manager.getRequiredWriteAccess(),
				"Required write access should be twoPCAccess",
			)
		},
	)

	t.Run(
		"Updates after elevation", func(t *testing.T) {
			manager := newMinCommitTsManager()
			manager.tryUpdate(10, ttlAccess)
			manager.elevateWriteAccess(twoPCAccess)

			manager.tryUpdate(20, ttlAccess)
			assert.Equal(t, uint64(10), manager.get(), "Value should remain 10")

			manager.tryUpdate(30, twoPCAccess)
			assert.Equal(t, uint64(30), manager.get(), "Value should be 30")
		},
	)

	t.Run(
		"Concurrent updates", func(t *testing.T) {
			manager := newMinCommitTsManager()
			done := make(chan bool)

			go func() {
				for i := 0; i < 1000; i++ {
					manager.tryUpdate(uint64(i), ttlAccess)
				}
				done <- true
			}()

			go func() {
				for i := 0; i < 1000; i++ {
					manager.tryUpdate(uint64(1000+i), ttlAccess)
				}
				done <- true
			}()

			<-done
			<-done

			assert.Equal(t, manager.get(), uint64(1999))
		},
	)
}
