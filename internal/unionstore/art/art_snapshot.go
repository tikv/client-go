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

package art

import "context"

func (*ART) SnapshotGetter() *SnapshotGetter {
	panic("unimplemented")
}

func (*ART) SnapshotIter([]byte, []byte) *SnapshotIter {
	panic("unimplemented")
}

func (*ART) SnapshotIterReverse([]byte, []byte) *SnapshotIter {
	panic("unimplemented")
}

type SnapshotGetter struct{}

func (s *SnapshotGetter) Get(context.Context, []byte) ([]byte, error) {
	panic("unimplemented")
}

type SnapshotIter struct{}

func (i *SnapshotIter) Valid() bool   { panic("unimplemented") }
func (i *SnapshotIter) Key() []byte   { panic("unimplemented") }
func (i *SnapshotIter) Value() []byte { panic("unimplemented") }
func (i *SnapshotIter) Next() error   { panic("unimplemented") }
func (i *SnapshotIter) Close()        { panic("unimplemented") }
