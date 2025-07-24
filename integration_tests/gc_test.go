package tikv_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	pdgc "github.com/tikv/pd/client/clients/gc"
	"github.com/tikv/pd/client/pkg/caller"
)

type hookedGCStatesClient struct {
	inner           pdgc.GCStatesClient
	getGCStatesHook func(inner pdgc.GCStatesClient, ctx context.Context) (pdgc.GCState, error)
}

func (c *hookedGCStatesClient) SetGCBarrier(ctx context.Context, barrierID string, barrierTS uint64, ttl time.Duration) (*pdgc.GCBarrierInfo, error) {
	return c.inner.SetGCBarrier(ctx, barrierID, barrierTS, ttl)
}

func (c *hookedGCStatesClient) DeleteGCBarrier(ctx context.Context, barrierID string) (*pdgc.GCBarrierInfo, error) {
	return c.inner.DeleteGCBarrier(ctx, barrierID)
}

func (c *hookedGCStatesClient) GetGCState(ctx context.Context) (pdgc.GCState, error) {
	if c.getGCStatesHook != nil {
		return c.getGCStatesHook(c.inner, ctx)
	}
	return c.inner.GetGCState(ctx)
}

var _ = pdgc.GCStatesClient(&hookedGCStatesClient{})

func TestGCWithTiKVSuite(t *testing.T) {
	suite.Run(t, new(testGCWithTiKVSuite))
}

type testGCWithTiKVSuite struct {
	suite.Suite
	pd    pd.Client
	addrs []string
	pdCli *tikv.CodecPDClient
	store *tikv.StoreProbe
}

func (s *testGCWithTiKVSuite) SetupTest() {
	if !*withTiKV {
		s.T().Skip("Cannot run without real tikv cluster")
	}
	s.addrs = strings.Split(*pdAddrs, ",")
	s.pdCli, s.store = s.createClient(nil)
}

func (s *testGCWithTiKVSuite) TearDownTest() {
	if s.pd != nil {
		s.pd.Close()
		s.Require().NoError(s.store.Close())
	}
}

func (s *testGCWithTiKVSuite) createClient(keyspaceMeta *keyspacepb.KeyspaceMeta) (*tikv.CodecPDClient, *tikv.StoreProbe) {
	re := s.Require()
	var pdCli *tikv.CodecPDClient
	if keyspaceMeta == nil {
		inner, err := pd.NewClient(caller.TestComponent, s.addrs, pd.SecurityOption{})
		re.NoError(err)
		pdCli = tikv.NewCodecPDClient(tikv.ModeTxn, inner)
	} else {
		inner, err := pd.NewClientWithKeyspace(context.Background(), caller.TestComponent, keyspaceMeta.GetId(), s.addrs, pd.SecurityOption{})
		re.NoError(err)
		pdCli, err = tikv.NewCodecPDClientWithKeyspace(tikv.ModeTxn, inner, keyspaceMeta.GetName())
		re.NoError(err)
	}
	tlsConfig, err := (&config.Security{}).ToTLSConfig()
	re.NoError(err)
	var spkv tikv.SafePointKV
	if keyspaceMeta == nil {
		spkv, err = tikv.NewEtcdSafePointKV(s.addrs, tlsConfig)
		re.NoError(err)
	} else {
		spkv, err = tikv.NewEtcdSafePointKV(s.addrs, tlsConfig, tikv.WithPrefix(keyspace.MakeKeyspaceEtcdNamespace(pdCli.GetCodec())))
		re.NoError(err)
	}
	store, err := tikv.NewKVStore("test-store", pdCli, spkv, tikv.NewRPCClient(tikv.WithCodec(pdCli.GetCodec())))
	return pdCli, &tikv.StoreProbe{KVStore: store}
}

func (s *testGCWithTiKVSuite) createKeyspace(name string, keyspaceLevelGC bool) *keyspacepb.KeyspaceMeta {
	re := s.Require()

	gcManagementType := "unified"
	if keyspaceLevelGC {
		gcManagementType = "keyspace_level"
	}
	req := struct {
		Name   string            `json:"name"`
		Config map[string]string `json:"config"`
	}{
		Name:   name,
		Config: map[string]string{"gc_management_type": gcManagementType},
	}
	reqJson, err := json.Marshal(req)
	re.NoError(err)
	resp, err := http.Post(fmt.Sprintf("http://%s/pd/api/v2/keyspaces", s.addrs[0]), "application/json", strings.NewReader(string(reqJson)))
	re.NoError(err)
	defer resp.Body.Close()
	respBody := new(strings.Builder)
	_, err = io.Copy(respBody, resp.Body)
	re.NoError(err)
	re.Equal(http.StatusOK, resp.StatusCode, "Failed to create keyspace %s, response: %s", name, err)

	meta, err := s.pdCli.LoadKeyspace(context.Background(), name)
	re.NoError(err)
	return meta
}

func (s *testGCWithTiKVSuite) dropKeyspace(keyspaceMeta *keyspacepb.KeyspaceMeta) {
	re := s.Require()
	_, err := s.pdCli.UpdateKeyspaceState(context.Background(), keyspaceMeta.Id, keyspacepb.KeyspaceState_ARCHIVED)
	re.NoError(err)
}

func genKeyspaceName() string {
	return uuid.New().String()
}

func (s *testGCWithTiKVSuite) TestLoadTxnSafePointFallback() {
	re := s.Require()

	// Keyspace using unified GC
	ks1 := s.createKeyspace(genKeyspaceName(), false)
	defer s.dropKeyspace(ks1)
	// Keyspace using keyspace level GC
	ks2 := s.createKeyspace(genKeyspaceName(), true)
	defer s.dropKeyspace(ks2)

	pd1, store1 := s.createClient(ks1)
	defer pd1.Close()
	defer store1.Close()
	pd2, store2 := s.createClient(ks2)
	defer pd2.Close()
	defer store2.Close()

}
