// Copyright 2023 TiKV Authors
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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/util/execdetails.go
//

// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetRequestSource(t *testing.T) {
	rsi := true
	rst := "test"
	ers := "explicit_test"
	rs := &RequestSource{
		RequestSourceInternal:     rsi,
		RequestSourceType:         rst,
		ExplicitRequestSourceType: ers,
	}

	// Test internal request
	expected := "internal_test_explicit_test"
	actual := rs.GetRequestSource()
	assert.Equal(t, expected, actual)

	// Test external request
	rs.RequestSourceInternal = false
	expected = "external_test_explicit_test"
	actual = rs.GetRequestSource()
	assert.Equal(t, expected, actual)

	// Test nil pointer
	rs = nil
	expected = "unknown"
	actual = rs.GetRequestSource()
	assert.Equal(t, expected, actual)

	// Test empty RequestSourceType and ExplicitRequestSourceType
	rs = &RequestSource{}
	expected = "unknown"
	actual = rs.GetRequestSource()
	assert.Equal(t, expected, actual)

	// Test empty ExplicitRequestSourceType
	rs.RequestSourceType = "test"
	expected = "external_test"
	actual = rs.GetRequestSource()
	assert.Equal(t, expected, actual)

	// Test empty RequestSourceType
	rs.RequestSourceType = ""
	rs.ExplicitRequestSourceType = "explicit_test"
	expected = "external_explicit_test"
	actual = rs.GetRequestSource()
	assert.Equal(t, expected, actual)
}
