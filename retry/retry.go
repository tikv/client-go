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
// See the License for the specific language governing permissions and
// limitations under the License.

package retry

import "github.com/tikv/client-go/v2/internal/retry"

// Re-export internal/retry for compatibility.
// TODO: Remove it.

// NewNoopBackoff create a Backoffer do nothing just return error directly
var NewNoopBackoff = retry.NewNoopBackoff

// BoTiFlashRPC is the backoff config for TiFlash RPCs.
var BoTiFlashRPC = retry.BoTiFlashRPC
