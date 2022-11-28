// Copyright 2022 TiKV Authors
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

	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/atomic"
)

type localExternalTimestamp struct {
	externalTimestamp atomic.Uint64
}

func (l *localExternalTimestamp) setExternalTimestamp(ctx context.Context, o oracle.Oracle, newTimestamp uint64) error {
	currentTSO, err := o.GetTimestamp(ctx, nil)
	if err != nil {
		return err
	}

	if newTimestamp > currentTSO {
		return errors.New("external timestamp is greater than global tso")
	}
	for {
		externalTimestamp := l.externalTimestamp.Load()
		if externalTimestamp > newTimestamp {
			return errors.New("cannot decrease the external timestamp")
		} else if externalTimestamp == newTimestamp {
			return nil
		}

		if l.externalTimestamp.CompareAndSwap(externalTimestamp, newTimestamp) {
			return nil
		}
	}
}

func (l *localExternalTimestamp) getExternalTimestamp(ctx context.Context) (uint64, error) {
	return l.externalTimestamp.Load(), nil
}
