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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/util/rate_limit_test.go
//

// Copyright 2023 PingCAP, Inc.
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
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"
)

func TestPDRequestRetry(t *testing.T) {
	EnableFailpoints()
	ctx := context.Background()
	require := require.New(t)
	require.Nil(failpoint.Enable("tikvclient/FastRetry", `return()`))
	defer func() {
		require.Nil(failpoint.Disable("tikvclient/FastRetry"))
	}()

	count := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count++
		if count <= pdRequestRetryTime-1 {
			w.WriteHeader(http.StatusGatewayTimeout)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	cli := http.DefaultClient
	taddr := ts.URL
	_, reqErr := pdRequest(ctx, taddr, "", cli, http.MethodGet, nil)
	require.Nil(reqErr)
	ts.Close()

	count = 0
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count++
		if count <= pdRequestRetryTime+1 {
			w.WriteHeader(http.StatusGatewayTimeout)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	taddr = ts.URL
	_, reqErr = pdRequest(ctx, taddr, "", cli, http.MethodGet, nil)
	require.Error(reqErr)
	ts.Close()

	require.Nil(failpoint.Enable("tikvclient/InjectClosed", fmt.Sprintf("return(%d)", 0)))
	defer func() {
		require.Nil(failpoint.Disable("tikvclient/InjectClosed"))
	}()
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	taddr = ts.URL
	_, reqErr = pdRequest(ctx, taddr, "", cli, http.MethodGet, nil)
	require.Error(reqErr)
	ts.Close()
}
