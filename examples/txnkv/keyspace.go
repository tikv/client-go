// Copyright 2025 TiKV Authors
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

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/tikv/client-go/v2/txnkv"
)

var (
	pdAddr       = flag.String("pd", "127.0.0.1:2379", "comma-separated PD address list")
	keyspaceName = flag.String("keyspace", "", "keyspace name (required for API v2)")
)

func main() {
	injectEnvVars()
	flag.Parse()

	if *keyspaceName == "" {
		fmt.Fprintln(os.Stderr, "missing --keyspace. Create or enable one with pd-ctl or the PD keyspace API before running this example.")
		os.Exit(2)
	}

	ctx := context.Background()
	client := mustNewKeyspaceClient(*pdAddr, *keyspaceName)
	defer client.Close()

	key := []byte("shared-logical-key")
	value := []byte(fmt.Sprintf("value recorded by %s at %s", *keyspaceName, time.Now().Format(time.RFC3339Nano)))

	fmt.Printf("Writing %q into keyspace %q...\n", key, *keyspaceName)
	if err := writeKey(ctx, client, key, value); err != nil {
		panic(err)
	}

	got, err := readKey(ctx, client, key)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Read back value %q from keyspace %q\n", got, *keyspaceName)
	fmt.Println("Run the program twice with different --keyspace values but the same logical key to observe keyspace isolation.")
}

func injectEnvVars() {
	if pd := os.Getenv("PD_ADDR"); pd != "" {
		os.Args = append(os.Args, "-pd", pd)
	}
	if ks := os.Getenv("KEYSPACE_NAME"); ks != "" {
		os.Args = append(os.Args, "-keyspace", ks)
	}
}

func mustNewKeyspaceClient(pd, keyspace string) *txnkv.Client {
	addrs := parsePDAddrs(pd)
	client, err := txnkv.NewClient(
		addrs,
		txnkv.WithAPIVersion(kvrpcpb.APIVersion_V2),
		txnkv.WithKeyspace(keyspace),
	)
	if err != nil {
		panic(fmt.Sprintf("failed to connect to PD %v with keyspace %q: %v", addrs, keyspace, err))
	}
	return client
}

func parsePDAddrs(raw string) []string {
	parts := strings.Split(raw, ",")
	addrs := make([]string, 0, len(parts))
	for _, part := range parts {
		addr := strings.TrimSpace(part)
		if addr != "" {
			addrs = append(addrs, addr)
		}
	}
	if len(addrs) == 0 {
		panic("pd address flag must not be empty")
	}
	return addrs
}

func writeKey(ctx context.Context, client *txnkv.Client, key, value []byte) error {
	txn, err := client.Begin()
	if err != nil {
		return err
	}
	if err := txn.Set(key, value); err != nil {
		return err
	}
	return txn.Commit(ctx)
}

func readKey(ctx context.Context, client *txnkv.Client, key []byte) ([]byte, error) {
	txn, err := client.Begin()
	if err != nil {
		return nil, err
	}
	return txn.Get(ctx, key)
}
