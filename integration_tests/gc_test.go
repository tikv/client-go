package tikv_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	pdgc "github.com/tikv/pd/client/clients/gc"
	"github.com/tikv/pd/client/constants"
	"github.com/tikv/pd/client/pkg/caller"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

func hookGCStatesClientForStore(store *tikv.StoreProbe, getGCStatesHook func(inner pdgc.GCStatesClient, ctx context.Context) (pdgc.GCState, error)) {
	store.ReplaceGCStatesClient(&hookedGCStatesClient{
		inner:           store.GetGCStatesClient(),
		getGCStatesHook: getGCStatesHook,
	})
}

var _ = pdgc.GCStatesClient(&hookedGCStatesClient{})

func TestGCWithTiKVSuite(t *testing.T) {
	suite.Run(t, new(testGCWithTiKVSuite))
}

type testGCWithTiKVSuite struct {
	suite.Suite
	globalPDCli pd.Client
	addrs       []string

	pdClis    []*tikv.CodecPDClient
	stores    []*tikv.StoreProbe
	keyspaces []*keyspacepb.KeyspaceMeta
}

func (s *testGCWithTiKVSuite) SetupTest() {
	if !*withTiKV {
		s.T().Skip("Cannot run without real tikv cluster")
	}
	s.addrs = strings.Split(*pdAddrs, ",")
	var err error
	s.globalPDCli, err = pd.NewClient(caller.TestComponent, s.addrs, pd.SecurityOption{})
	s.Require().NoError(err)
}

func (s *testGCWithTiKVSuite) TearDownTest() {
	re := s.Require()
	for _, store := range s.stores {
		re.NoError(store.Close())
	}
	if s.globalPDCli != nil {
		s.globalPDCli.Close()
	}
	for _, pdCli := range s.pdClis {
		pdCli.Close()
	}
	for _, keyspaceMeta := range s.keyspaces {
		if keyspaceMeta == nil {
			s.dropKeyspace(keyspaceMeta)
		}
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
	resp, err := http.Post(fmt.Sprintf("%s/pd/api/v2/keyspaces", s.addrs[0]), "application/json", bytes.NewBuffer(reqJson))
	re.NoError(err)
	defer resp.Body.Close()
	respBody := new(strings.Builder)
	_, err = io.Copy(respBody, resp.Body)
	re.NoError(err)
	re.Equal(http.StatusOK, resp.StatusCode, "Failed to create keyspace %s, response: %s", name, respBody.String())

	meta, err := s.globalPDCli.LoadKeyspace(context.Background(), name)
	re.NoError(err)
	// Avoid goroutine leak in the test.
	http.DefaultClient.CloseIdleConnections()
	return meta
}

type storeKeyspaceType int

const (
	storeNullKeyspace storeKeyspaceType = iota
	storeKeyspaceLevelGCKeyspace
	storeUnifiedGCKeyspace
)

func (s *testGCWithTiKVSuite) dropKeyspace(keyspaceMeta *keyspacepb.KeyspaceMeta) {
	re := s.Require()
	// Nil might be used to represent the null keyspace.
	if keyspaceMeta != nil {
		_, err := s.globalPDCli.UpdateKeyspaceState(context.Background(), keyspaceMeta.Id, keyspacepb.KeyspaceState_ARCHIVED)
		re.NoError(err)
	}
}

func genKeyspaceName() string {
	return uuid.New().String()
}

func (s *testGCWithTiKVSuite) prepareClients(storeKeyspaceTypes ...storeKeyspaceType) {
	s.pdClis = make([]*tikv.CodecPDClient, 0, len(storeKeyspaceTypes))
	s.stores = make([]*tikv.StoreProbe, 0, len(storeKeyspaceTypes))
	s.keyspaces = make([]*keyspacepb.KeyspaceMeta, 0, len(storeKeyspaceTypes))

	for _, t := range storeKeyspaceTypes {
		var keyspaceMeta *keyspacepb.KeyspaceMeta
		switch t {
		case storeNullKeyspace:
			// represents by nil keyspace meta
		case storeKeyspaceLevelGCKeyspace:
			keyspaceMeta = s.createKeyspace(genKeyspaceName(), true)
		case storeUnifiedGCKeyspace:
			keyspaceMeta = s.createKeyspace(genKeyspaceName(), false)
		}
		pdCli, store := s.createClient(keyspaceMeta)
		s.keyspaces = append(s.keyspaces, keyspaceMeta)
		s.pdClis = append(s.pdClis, pdCli)
		s.stores = append(s.stores, store)
	}
}

func (s *testGCWithTiKVSuite) TestLoadTxnSafePointFallback() {
	re := s.Require()

	re.NoError(failpoint.Enable("tikvclient/noBuiltInTxnSafePointUpdater", "return"))
	defer func() {
		re.NoError(failpoint.Disable("tikvclient/noBuiltInTxnSafePointUpdater"))
	}()

	s.prepareClients(storeNullKeyspace, storeUnifiedGCKeyspace, storeKeyspaceLevelGCKeyspace)

	ctx := context.Background()

	// The new keyspaces always have zero txn safe point, while the null keyspace may have larger txn safe point due
	// to other tests that has been run on the cluster. The following test will use values from the null keyspace's
	// txn safe point.
	state, err := s.pdClis[0].GetGCStatesClient(constants.NullKeyspaceID).GetGCState(ctx)
	re.NoError(err)
	base := state.TxnSafePoint

	callCounter := 0
	hook := func(inner pdgc.GCStatesClient, ctx context.Context) (pdgc.GCState, error) {
		callCounter += 1
		return pdgc.GCState{}, status.Errorf(codes.Unimplemented, "simulated unimplemented error")
	}
	for _, store := range s.stores {
		hookGCStatesClientForStore(store, hook)
		_, err = store.GetGCStatesClient().GetGCState(ctx)
		re.Error(err)
		re.Equal(codes.Unimplemented, status.Code(err))
	}
	re.Equal(len(s.stores), callCounter)
	callCounter = 0

	// Updating the null keyspace. The new value is visible by the null keyspace and keyspaces using unified GC,
	// but not by keyspaces using keyspace-level GC.
	res, err := s.pdClis[0].GetGCInternalController(constants.NullKeyspaceID).AdvanceTxnSafePoint(ctx, base+1)
	re.NoError(err)
	re.Equal(base+1, res.NewTxnSafePoint)
	// Call LoadTxnSafePoint twice, so that by checking the callCounter, we chan ensure the new API is called at most
	// once (for each KVStore) and won't be called again since the first falling back.
	// For store[0] (null keyspace) and store[1] (unified GC)
	for i := 0; i < 2; i++ {
		for _, store := range s.stores[0:2] {
			txnSafePoint, err := store.LoadTxnSafePoint(ctx)
			re.NoError(err)
			re.Equal(base+1, txnSafePoint)
		}
		// For store[2] (keyspace level GC): Invisible to the new txn safe point.
		txnSafePoint, err := s.stores[2].LoadTxnSafePoint(ctx)
		re.NoError(err)
		re.Less(txnSafePoint, base+1)
	}

	// Updating the keyspace with keyspace level GC. The effect is visible only in the same keyspace.
	res, err = s.pdClis[2].GetGCInternalController(s.keyspaces[2].GetId()).AdvanceTxnSafePoint(ctx, base+2)
	re.NoError(err)
	re.Equal(base+2, res.NewTxnSafePoint)
	for i := 0; i < 2; i++ {
		txnSafePoint, err := s.stores[2].LoadTxnSafePoint(ctx)
		re.NoError(err)
		re.Equal(base+2, txnSafePoint)
		for _, store := range s.stores[0:2] {
			txnSafePoint, err := store.LoadTxnSafePoint(ctx)
			re.NoError(err)
			re.Less(txnSafePoint, base+2)
		}
	}

	// Each store tries `GetGCState` only once.
	re.Equal(len(s.stores), callCounter)
}
