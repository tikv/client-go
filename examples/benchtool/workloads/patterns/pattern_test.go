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

package patterns

import (
	"testing"

	"benchtool/config"

	"gotest.tools/v3/assert"
)

func TestPatterns(t *testing.T) {
	// Read the YAML file
	filePath := "/Users/lucasliang/Workspace/client-go/examples/benchtool/test.yaml"
	patternsConfig := &config.PatternsConfig{
		FilePath: filePath,
	}
	err := patternsConfig.Parse()
	assert.Equal(t, err == nil, true)

	for _, pattern := range patternsConfig.Plans {
		if pattern.GetRawKVConfig() == nil {
			assert.Equal(t, pattern.GetTxnKVConfig() == nil, false)
		} else {
			assert.Equal(t, pattern.GetRawKVConfig() == nil, false)
		}
		workloads := pattern.GetWorkloads()
		assert.Equal(t, len(workloads) > 0, true)
	}
}
