// Copyright 2026 TiKV Authors
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

package txn_file_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/pingcap/log"
	"go.uber.org/goleak"
)

const txnFileLogPath = "txn-file.log"

func TestMain(m *testing.M) {
	logFile, err := os.Create(txnFileLogPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create txn-file test log: %v\n", err)
		os.Exit(1)
	}
	logger, props, err := log.InitLoggerWithWriteSyncer(&log.Config{Level: "info"}, logFile, logFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "initialize txn-file test logger: %v\n", err)
		if closeErr := logFile.Close(); closeErr != nil {
			fmt.Fprintf(os.Stderr, "close txn-file test log after initialization failure: %v\n", closeErr)
		}
		os.Exit(1)
	}
	restoreLogger := log.ReplaceGlobals(logger, props)

	goleak.VerifyTestMain(m,
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
		goleak.Cleanup(func(exitCode int) {
			if err := log.Sync(); err != nil {
				fmt.Fprintf(os.Stderr, "flush txn-file test log: %v\n", err)
				exitCode = 1
			}
			restoreLogger()
			if err := logFile.Close(); err != nil {
				fmt.Fprintf(os.Stderr, "close txn-file test log: %v\n", err)
				exitCode = 1
			}
			os.Exit(exitCode)
		}),
	)
}
