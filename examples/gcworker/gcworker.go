package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/tikv/client-go/v2/tikv"
)

var (
	client *tikv.KVStore
	pdAddr = flag.String("pd", "127.0.0.1:2379", "pd address")
)

// Init initializes information.
func initStore() {
	var err error
	client, err = tikv.NewTxnClient([]string{*pdAddr})
	if err != nil {
		panic(err)
	}
}

func main() {
	pdAddr := os.Getenv("PD_ADDR")
	if pdAddr != "" {
		os.Args = append(os.Args, "-pd", pdAddr)
	}
	flag.Parse()
	initStore()

	safepoint := time.Now().AddDate(0, -1, -1)
	newSafepoint, err := client.GC(context.Background(), safepoint)
	if err != nil {
		panic(err)
	}
	fmt.Println("Finished GC, expect safepoint:", safepoint, " real safepoint:", newSafepoint)
}
