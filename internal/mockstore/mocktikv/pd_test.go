// Copyright 2024 TiKV Project Authors.
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

package mocktikv

import (
	"fmt"
	"testing"

	"github.com/asaskevich/govalidator"
	"github.com/stretchr/testify/require"
)

func TestMockPDServiceDiscovery(t *testing.T) {
	re := require.New(t)
	pdAddrs := []string{"invalid_pd_address", "127.0.0.1:2379", "http://172.32.21.32:2379"}
	for i, addr := range pdAddrs {
		check := govalidator.IsURL(addr)
		fmt.Println(i)
		if i > 0 {
			re.True(check)
		} else {
			re.False(check)
		}
	}
	sd := newMockPDServiceDiscovery(pdAddrs)
	clis := sd.GetAllServiceClients()
	re.Len(clis, 2)
	re.Equal(clis[0].GetHTTPAddress(), "http://127.0.0.1:2379")
	re.Equal(clis[1].GetHTTPAddress(), "http://172.32.21.32:2379")
}
