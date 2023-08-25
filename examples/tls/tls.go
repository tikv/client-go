package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"github.com/google/uuid"
	"github.com/pingcap/log"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

func k(k string) []byte {
	return []byte(k)
}

var v = k

func main() {
	pdAddrs := []string{"127.0.0.1:2379", "127.0.0.1:2382", "127.0.0.1:2384"}
	pdCli, err := pd.NewClient(pdAddrs, pd.SecurityOption{})
	if err != nil {
		log.Fatal("fail to create pd client", zap.Error(err))
	}

	config.UpdateGlobal(func(c *config.Config) {
		c.PreferSecureConnection = true
	})
	codecPdCli, err := tikv.NewCodecPDClientWithKeyspace(tikv.ModeTxn, pdCli, "a")
	if err != nil {
		log.Fatal("fail to create codec pd client", zap.Error(err))
	}

	codec := codecPdCli.GetCodec()

	spkv, err := tikv.NewEtcdSafePointKV(pdAddrs, &tls.Config{}, tikv.WithPrefix("/keyspaces/tidb/1"))
	if err != nil {
		log.Fatal("fail to create safe point kv", zap.Error(err))
	}

	rpcCli := tikv.NewRPCClient(
		tikv.WithCodec(codec),
		tikv.WithSecurity(config.Security{
			ClusterSSLCA:   "/home/iosmanthus/endless-t/tls/root.crt",
			ClusterSSLCert: "/home/iosmanthus/endless-t/tls/tikv.crt",
			ClusterSSLKey:  "/home/iosmanthus/endless-t/tls/tikv.key",
		}))

	id := uuid.New().String()
	s, err := tikv.NewKVStore(id, codecPdCli, spkv, rpcCli)
	if err != nil {
		log.Fatal("fail to create kv store", zap.Error(err))
	}

	txn, err := s.Begin()
	if err != nil {
		log.Fatal("fail to begin txn", zap.Error(err))
	}

	err = txn.Set(k("a"), v("b"))
	if err != nil {
		log.Fatal("fail to set", zap.Error(err))
	}

	value, err := txn.Get(context.TODO(), k("a"))
	if !bytes.Equal(value, v("b")) {
		log.Fatal("wrong get", zap.Error(err))
	}

	err = txn.Commit(context.Background())
	if err != nil {
		log.Fatal("fail to commit", zap.Error(err))
	}
}
