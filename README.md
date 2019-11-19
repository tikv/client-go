Go client for TiKV
------------

RawAPI (provide atomic read/write on single key-value level), example:

```
package main

import (
	"context"
	"fmt"

	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/rawkv"
)

func main() {
	cli, err := rawkv.NewClient(context.TODO(), []string{"127.0.0.1:2379"}, config.Default())
	if err != nil {
		panic(err)
	}
	defer cli.Close()

	fmt.Printf("cluster ID: %d\n", cli.ClusterID())

	key := []byte("Company")
	val := []byte("PingCAP")

	// put key into tikv
	err = cli.Put(context.TODO(), key, val)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Successfully put %s:%s to tikv\n", key, val)

	// get key from tikv
	val, err = cli.Get(context.TODO(), key)
	if err != nil {
		panic(err)
	}
	fmt.Printf("found val: %s for key: %s\n", val, key)

	// delete key from tikv
	err = cli.Delete(context.TODO(), key)
	if err != nil {
		panic(err)
	}
	fmt.Printf("key: %s deleted\n", key)

	// get key again from tikv
	val, err = cli.Get(context.TODO(), key)
	if err != nil {
		panic(err)
	}
	fmt.Printf("found val: %s for key: %s\n", val, key)
}
```

Transactional API (provide cross-row transaction support with SI(Snapshot Isolation) level), example:

```
package main

import (
	"context"
	"flag"
	"fmt"

	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/txnkv"
)

var (
	pdAddr = flag.String("pd", "127.0.0.1:2379", "pd address")
)

func main() {
	client, err := txnkv.NewClient(context.TODO(), []string{*pdAddr}, config.Default())
	if err != nil {
		panic(err)
	}
  defer client.Close()

	tx, err := client.Begin(context.TODO())
	if err != nil {
		panic(err)
	}

	tx.Set([]byte("k1"), []byte("v1"))
	tx.Set([]byte("k2"), []byte("v2"))

	tx.Commit(context.Background())

	tx, err = client.Begin(context.TODO())
	var v1, v2 []byte
	v1, err = tx.Get(context.TODO(), []byte("k1"))
	v2, err = tx.Get(context.TODO(), []byte("k2"))
	fmt.Println(v1, v2)
}
```
