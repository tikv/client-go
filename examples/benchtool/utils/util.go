package utils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/olekukonko/tablewriter"
)

const (
	DEFAULT_PRECISION = 2
)

const (
	OutputStylePlain = "plain"
	OutputStyleTable = "table"
	OutputStyleJson  = "json"
)

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

func FloatToString(num float64) string {
	return strconv.FormatFloat(num, 'f', DEFAULT_PRECISION, 64)
}

func IntToString(num int64) string {
	return strconv.FormatInt(num, 10)
}

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
