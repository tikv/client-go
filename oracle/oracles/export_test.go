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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/oracle/oracles/export_test.go
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

package oracles

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
)

// SetOracleHookCurrentTime exports localOracle's time hook to test.
func SetOracleHookCurrentTime(oc oracle.Oracle, t time.Time) {
	switch o := oc.(type) {
	case *localOracle:
		if o.hook == nil {
			o.hook = &struct {
				currentTime time.Time
			}{}
		}
		o.hook.currentTime = t
	}
}

// NewEmptyPDOracle exports pdOracle struct to test
func NewEmptyPDOracle() oracle.Oracle {
	return &pdOracle{}
}

func NewPdOracleWithClient(client pd.Client) oracle.Oracle {
	return &pdOracle{
		c: client,
	}
}

func StartTsUpdateLoop(o oracle.Oracle, ctx context.Context, wg *sync.WaitGroup) {
	pd, ok := o.(*pdOracle)
	if !ok {
		panic("expected pdOracle")
	}
	pd.quit = make(chan struct{})
	wg.Add(1)
	go func() {
		pd.updateTS(ctx)
		wg.Done()
	}()
}

// SetEmptyPDOracleLastTs exports PD oracle's global last ts to test.
func SetEmptyPDOracleLastTs(oc oracle.Oracle, ts uint64) {
	switch o := oc.(type) {
	case *pdOracle:
		lastTSInterface, _ := o.lastTSMap.LoadOrStore(oracle.GlobalTxnScope, &atomic.Pointer[lastTSO]{})
		lastTSPointer := lastTSInterface.(*atomic.Pointer[lastTSO])
		lastTSPointer.Store(&lastTSO{tso: ts, arrival: ts})
	}
}
