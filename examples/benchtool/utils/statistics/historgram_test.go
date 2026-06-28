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

package statistics

import (
	"math/rand"
	"testing"
	"time"
)

func TestHist(t *testing.T) {
	h := NewPerfHistogram(1*time.Millisecond, 20*time.Minute, 1)
	for i := 0; i < 10000; i++ {
		n := rand.Intn(15020)
		h.Record(time.Millisecond * time.Duration(n))
	}
	h.Record(time.Minute * 9)
	h.Record(time.Minute * 100)
	t.Logf("%+v", h.Format())
}
