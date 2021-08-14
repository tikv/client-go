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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/mockstore/mocktikv/mock_tikv_test.go
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

package mocktikv

import (
	"testing"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/assert"
)

func TestMarshalmvccLock(t *testing.T) {
	assert := assert.New(t)
	l := mvccLock{
		startTS:     47,
		primary:     []byte{'a', 'b', 'c'},
		value:       []byte{'d', 'e'},
		op:          kvrpcpb.Op_Put,
		ttl:         444,
		minCommitTS: 666,
	}
	bin, err := l.MarshalBinary()
	assert.Nil(err)

	var l1 mvccLock
	err = l1.UnmarshalBinary(bin)
	assert.Nil(err)

	assert.Equal(l.startTS, l1.startTS)
	assert.Equal(l.op, l1.op)
	assert.Equal(l.ttl, l1.ttl)
	assert.Equal(string(l.primary), string(l1.primary))
	assert.Equal(string(l.value), string(l1.value))
	assert.Equal(l.minCommitTS, l1.minCommitTS)
}

func TestMarshalmvccValue(t *testing.T) {
	assert := assert.New(t)
	v := mvccValue{
		valueType: typePut,
		startTS:   42,
		commitTS:  55,
		value:     []byte{'d', 'e'},
	}
	bin, err := v.MarshalBinary()
	assert.Nil(err)

	var v1 mvccValue
	err = v1.UnmarshalBinary(bin)
	assert.Nil(err)

	assert.Equal(v.valueType, v1.valueType)
	assert.Equal(v.startTS, v1.startTS)
	assert.Equal(v.commitTS, v1.commitTS)
	assert.Equal(string(v.value), string(v.value))
}
