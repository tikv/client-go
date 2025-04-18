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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/logutil/hex.go
//

// Copyright 2021 PingCAP, Inc.
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

package logutil

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"strings"

	"github.com/golang/protobuf/proto" //nolint:staticcheck
	"github.com/tikv/client-go/v2/util/redact"
)

// Hex defines a fmt.Stringer for proto.Message.
// We can't define the String() method on proto.Message, but we can wrap it.
func Hex(msg proto.Message) fmt.Stringer {
	return hexStringer{msg}
}

type hexStringer struct {
	proto.Message
}

func (h hexStringer) String() string {
	val := reflect.ValueOf(h.Message)
	var w bytes.Buffer
	prettyPrint(&w, val)
	return w.String()
}

func prettyPrint(w io.Writer, val reflect.Value) {
	tp := val.Type()
	switch val.Kind() {
	case reflect.Slice:
		elemType := tp.Elem()
		if elemType.Kind() == reflect.Uint8 {
			fmt.Fprintf(w, "%s", redact.Key(val.Bytes()))
		} else {
			fmt.Fprintf(w, "%s", val.Interface())
		}
	case reflect.Struct:
		fmt.Fprintf(w, "{")
		for i := 0; i < val.NumField(); i++ {
			fv := val.Field(i)
			ft := tp.Field(i)
			if strings.HasPrefix(ft.Name, "XXX_") {
				continue
			}
			if i != 0 {
				fmt.Fprintf(w, " ")
			}
			fmt.Fprintf(w, "%s:", ft.Name)
			prettyPrint(w, fv)
		}
		fmt.Fprintf(w, "}")
	case reflect.Ptr:
		if val.IsNil() {
			fmt.Fprintf(w, "%v", val.Interface())
		} else {
			prettyPrint(w, reflect.Indirect(val))
		}
	default:
		fmt.Fprintf(w, "%v", val.Interface())
	}
}
