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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/mockstore/deadlock/deadlock_test.go
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

package deadlock

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeadlock(t *testing.T) {
	assert := assert.New(t)
	detector := NewDetector()
	err := detector.Detect(1, 2, 100)
	assert.Nil(err)
	err = detector.Detect(2, 3, 200)
	assert.Nil(err)
	err = detector.Detect(3, 1, 300)
	assert.EqualError(err, "deadlock(200)")
	detector.CleanUp(2)
	list2 := detector.waitForMap[2]
	assert.Nil(list2)

	// After cycle is broken, no deadlock now.
	err = detector.Detect(3, 1, 300)
	assert.Nil(err)
	list3 := detector.waitForMap[3]
	assert.Len(list3.txns, 1)

	// Different keyHash grows the list.
	err = detector.Detect(3, 1, 400)
	assert.Nil(err)
	assert.Len(list3.txns, 2)

	// Same waitFor and key hash doesn't grow the list.
	err = detector.Detect(3, 1, 400)
	assert.Nil(err)
	assert.Len(list3.txns, 2)

	detector.CleanUpWaitFor(3, 1, 300)
	assert.Len(list3.txns, 1)
	detector.CleanUpWaitFor(3, 1, 400)
	list3 = detector.waitForMap[3]
	assert.Nil(list3)
	detector.Expire(1)
	assert.Len(detector.waitForMap, 1)
	detector.Expire(2)
	assert.Len(detector.waitForMap, 0)
}
