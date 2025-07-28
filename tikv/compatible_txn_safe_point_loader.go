package tikv

import (
	"context"
	"crypto/tls"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/internal/apicodec"
	pd "github.com/tikv/pd/client"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	unifiedTxnSafePointPath       = "/tidb/store/gcworker/saved_safe_point"
	keyspaceLevelTxnSafePointPath = "/keyspaces/tidb/%d/tidb/store/gcworker/saved_safe_point"
)

// compatibleTxnSafePointLoader is used to load txn safe point from etcd for old versions where the GetGCState API
// is not yet supported.
//
// The same functionality can be provided by EtcdSafePointKV. However, EtcdSafePointKV determines whether the key needs
// to be prefixed. Meanwhile, when client-go's user creates KVStore, it needs manually create EtcdSafePointKV first.
// This means, whether the etcd client is correctly prefixed depends on whether the user creates it correctly.
// Considering that TiDB is not the only user of client-go, this was a bad design, and we cannot assume the caller
// can easily make it correct (e.g., whether the txn safe point is stored in prefixed key depends on whether
// keyspace level GC is enabled for a keyspace). To minimize the risk of misusing, we rewrote a special util for loading
// it. The EtcdSafePointKV, on the contrary, will be deprecated in the future.
type compatibleTxnSafePointLoader struct {
	mu        sync.Mutex
	etcdCli   atomic.Pointer[clientv3.Client]
	endpoints []string
	tlsConfig *tls.Config
	codec     apicodec.Codec
}

func newCompatibleTxnSafePointLoader(codec apicodec.Codec, endpoints []string, tlsConfig *tls.Config) *compatibleTxnSafePointLoader {
	return &compatibleTxnSafePointLoader{
		endpoints: endpoints,
		tlsConfig: tlsConfig,
		codec:     codec,
	}
}

func (l *compatibleTxnSafePointLoader) getEtcdCli() (*clientv3.Client, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	cli := l.etcdCli.Load()
	// It may be concurrently initialized by other goroutine before we successfully acquire the mutex
	if cli != nil {
		return cli, nil
	}
	cli, err := createEtcdKV(l.endpoints, l.tlsConfig)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	l.etcdCli.Store(cli)
	return cli, nil
}

func (l *compatibleTxnSafePointLoader) loadTxnSafePoint(ctx context.Context) (uint64, error) {
	cli := l.etcdCli.Load()
	var err error
	if cli == nil {
		// Lazy create the etcd client.
		cli, err = l.getEtcdCli()
		if err != nil {
			return 0, errors.WithStack(err)
		}
	}

	key := unifiedTxnSafePointPath
	keyspaceMeta := l.codec.GetKeyspaceMeta()
	if pd.IsKeyspaceUsingKeyspaceLevelGC(keyspaceMeta) {
		key = fmt.Sprintf(keyspaceLevelTxnSafePointPath, keyspaceMeta.Id)
	}

	// Follow the same implementation as the EtcdSafePointKV by setting the timeout 5 seconds.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	resp, err := cli.Get(ctx, key)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	if len(resp.Kvs) == 0 {
		return 0, nil
	}

	str := string(resp.Kvs[0].Value)
	if len(str) == 0 {
		return 0, nil
	}
	value, err := strconv.ParseUint(str, 10, 64)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return value, nil
}

func (l *compatibleTxnSafePointLoader) Close() error {
	cli := l.etcdCli.Load()
	if cli != nil {
		return cli.Close()
	}
	return nil
}
