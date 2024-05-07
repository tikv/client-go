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

package utils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"

	"github.com/olekukonko/tablewriter"
)

const (
	DEFAULT_PRECISION = 2
	MAX_INT64         = 1<<63 - 1
)

const (
	OutputStylePlain = "plain"
	OutputStyleTable = "table"
	OutputStyleJson  = "json"
)

// ReadWriteRatio is used to parse the read-write ratio.
type ReadWriteRatio struct {
	Ratio        string
	readPercent  int
	writePercent int
}

func NewReadWriteRatio(ratio string) *ReadWriteRatio {
	return &ReadWriteRatio{Ratio: ratio, readPercent: -1, writePercent: -1}
}

func (r *ReadWriteRatio) ParseRatio() error {
	if r.Ratio == "" {
		return fmt.Errorf("empty read-write-ratio")
	}
	ratios := strings.Split(r.Ratio, ":")
	if len(ratios) == 2 {
		readRatio := 0
		writeRatio := 0

		readRatio, _ = strconv.Atoi(ratios[0])
		writeRatio, _ = strconv.Atoi(ratios[1])
		if readRatio < 0 || writeRatio < 0 {
			return fmt.Errorf("invalid read-write-ratio format")
		}

		sumRatio := readRatio + writeRatio
		r.readPercent = readRatio * 100 / sumRatio
		r.writePercent = 100 - r.readPercent
	} else {
		return fmt.Errorf("invalid read-write-ratio format")
	}
	return nil
}

func (r *ReadWriteRatio) GetPercent(choice string) int {
	if r.Ratio == "" {
		return 0
	}
	// Not parsed yet.
	if r.readPercent == -1 || r.writePercent == -1 {
		if r.ParseRatio() != nil {
			return 0
		}
	}
	if choice == "read" {
		return r.readPercent
	} else if choice == "write" {
		return r.writePercent
	}
	return 0
}

// Converting functions.

func FloatToString(num float64) string {
	return strconv.FormatFloat(num, 'f', DEFAULT_PRECISION, 64)
}

func IntToString(num int64) string {
	return strconv.FormatInt(num, 10)
}

func StrArrsToByteArrs(strArrs []string) [][]byte {
	byteArrs := make([][]byte, 0, len(strArrs))
	for _, strArr := range strArrs {
		byteArrs = append(byteArrs, []byte(strArr))
	}
	return byteArrs
}

func GenRandomStr(prefix string, keySize int) string {
	return fmt.Sprintf("%s@%0*d", prefix, keySize, rand.Intn(MAX_INT64))
}

func GenRandomStrArrs(prefix string, keySize, count int) []string {
	keys := make([]string, 0, count)
	for i := 0; i < count; i++ {
		keys = append(keys, GenRandomStr(prefix, keySize))
	}
	return keys
}

func GenRandomByteArrs(prefix string, keySize, count int) [][]byte {
	keys := make([][]byte, 0, count)
	for i := 0; i < count; i++ {
		keys = append(keys, []byte(GenRandomStr(prefix, keySize)))
	}
	return keys
}

// Output formatting functions.

func RenderString(format string, headers []string, values [][]string) {
	if len(values) == 0 {
		return
	}
	if len(headers) == 0 {
		for _, value := range values {
			args := make([]interface{}, len(value))
			for i, v := range value {
				args[i] = v
			}
			fmt.Printf(format, args...)
		}
		return
	}

	buf := new(bytes.Buffer)
	for _, value := range values {
		args := make([]string, len(headers))
		for i, header := range headers {
			args[i] = header + ": " + value[i+2]
		}
		buf.WriteString(fmt.Sprintf(format, value[0], value[1], strings.Join(args, ", ")))
	}
	fmt.Print(buf.String())
}

func RenderTable(headers []string, values [][]string) {
	if len(values) == 0 {
		return
	}
	tb := tablewriter.NewWriter(os.Stdout)
	tb.SetHeader(headers)
	tb.AppendBulk(values)
	tb.Render()
}

func RenderJson(headers []string, values [][]string) {
	if len(values) == 0 {
		return
	}
	data := make([]map[string]string, 0, len(values))
	for _, value := range values {
		line := make(map[string]string, 0)
		for i, header := range headers {
			line[header] = value[i]
		}
		data = append(data, line)
	}
	outStr, err := json.Marshal(data)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(string(outStr))
}
