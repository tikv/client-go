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

package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetRequestSource(t *testing.T) {
	rsi := true
	rst := "test"
	ers := "lightning"
	rs := &RequestSource{
		RequestSourceInternal:     rsi,
		RequestSourceType:         rst,
		ExplicitRequestSourceType: ers,
	}

	// Test internal request
	expected := "internal_test_lightning"
	actual := rs.GetRequestSource()
	assert.Equal(t, expected, actual)

	// Test external request
	rs.RequestSourceInternal = false
	expected = "external_test_lightning"
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
	rs.ExplicitRequestSourceType = "lightning"
	expected = "external_unknown_lightning"
	actual = rs.GetRequestSource()
	assert.Equal(t, expected, actual)
}

func TestBuildRequestSource(t *testing.T) {
	// Test internal request
	expected := "internal_test_lightning"
	actual := BuildRequestSource(true, "test", "lightning")
	assert.Equal(t, expected, actual)

	// Test external request
	expected = "external_test_lightning"
	actual = BuildRequestSource(false, "test", "lightning")
	assert.Equal(t, expected, actual)

	// Test empty ExplicitRequestSourceType
	expected = "external_test"
	actual = BuildRequestSource(false, "test", "")
	assert.Equal(t, expected, actual)

	// Test empty RequestSourceType
	expected = "external_unknown_lightning"
	actual = BuildRequestSource(false, "", "lightning")
	assert.Equal(t, expected, actual)

	// Test RequestSourceType && ExplicitRequestSourceType both empty
	expected = "unknown"
	actual = BuildRequestSource(true, "", "")
	assert.Equal(t, expected, actual)
}
