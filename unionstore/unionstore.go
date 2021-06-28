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

package unionstore

import "github.com/tikv/client-go/v2/internal/unionstore"

// Re-export internal/unionstore for compatibility.
// TODO: Remove it.

// Getter is the interface for the Get method.
type Getter = unionstore.Getter

// Iterator is the interface for a iterator on KV store.
type Iterator = unionstore.Iterator

// MemDB is rollbackable Red-Black Tree optimized for transaction states buffer use scenario.
// You can think MemDB is a combination of two separate tree map, one for key => value and another for key => keyFlags.
//
// The value map is rollbackable, that means you can use the `Staging`, `Release` and `Cleanup` API to safely modify KVs.
//
// The flags map is not rollbackable. There are two types of flag, persistent and non-persistent.
// When discarding a newly added KV in `Cleanup`, the non-persistent flags will be cleared.
// If there are persistent flags associated with key, we will keep this key in node without value.
type MemDB = unionstore.MemDB
