package locate

import (
	"context"
	"errors"
	"testing"

	"github.com/pingcap/kvproto/pkg/apipb"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"

	"github.com/tikv/client-go/v2/internal/apicodec"
	"github.com/tikv/client-go/v2/internal/mockstore/mocktikv"
	"github.com/tikv/client-go/v2/testutils"
)

// mockPDClient wraps a mock PD client and lets tests control LoadKeyspace.
type mockPDClient struct {
	pd.Client
	meta       *keyspacepb.KeyspaceMeta
	loadErr    error
	loadedName string
}

func (c *mockPDClient) LoadKeyspace(ctx context.Context, name string) (*keyspacepb.KeyspaceMeta, error) {
	c.loadedName = name
	return c.meta, c.loadErr
}

func TestNewCodecPDClientWithKeyspaceState(t *testing.T) {
	client, cluster, _, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	defer client.Close()
	pdClient := mocktikv.NewPDClient(cluster)

	states := []keyspacepb.KeyspaceState{
		keyspacepb.KeyspaceState_ENABLED,
		keyspacepb.KeyspaceState_DISABLED,
		keyspacepb.KeyspaceState_ARCHIVED,
		keyspacepb.KeyspaceState_TOMBSTONE,
		keyspacepb.KeyspaceState(999),
	}
	for _, mode := range []struct {
		name  string
		value apicodec.Mode
	}{{"raw", apicodec.ModeRaw}, {"txn", apicodec.ModeTxn}} {
		for _, state := range states {
			t.Run(mode.name+"/"+state.String(), func(t *testing.T) {
				meta := &keyspacepb.KeyspaceMeta{
					Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 1},
					Name:     "test-keyspace",
					State:    state,
				}
				codecClient, err := NewCodecPDClientWithKeyspace(mode.value, &mockPDClient{Client: pdClient, meta: meta}, meta.Name)
				if state != keyspacepb.KeyspaceState_ENABLED {
					require.ErrorContains(t, err, "not enabled")
					require.ErrorContains(t, err, meta.Name)
					require.Nil(t, codecClient)
					return
				}
				require.NoError(t, err)
				require.Equal(t, apicodec.KeyspaceID(meta.GetId()), codecClient.GetCodec().GetKeyspaceID())
				require.Same(t, meta, codecClient.GetCodec().GetKeyspaceMeta())
			})
		}
	}
}

func TestNewCodecPDClientWithKeyspaceLookup(t *testing.T) {
	client, cluster, _, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	defer client.Close()
	pdClient := mocktikv.NewPDClient(cluster)

	t.Run("default keyspace", func(t *testing.T) {
		meta := &keyspacepb.KeyspaceMeta{
			Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: apicodec.DefaultKeyspaceID},
			Name:     apicodec.DefaultKeyspaceName,
			State:    keyspacepb.KeyspaceState_ENABLED,
		}
		lookupClient := &mockPDClient{Client: pdClient, meta: meta}
		codecClient, err := NewCodecPDClientWithKeyspace(apicodec.ModeTxn, lookupClient, "")
		require.NoError(t, err)
		require.Equal(t, apicodec.DefaultKeyspaceName, lookupClient.loadedName)
		require.Equal(t, apicodec.KeyspaceID(apicodec.DefaultKeyspaceID), codecClient.GetCodec().GetKeyspaceID())
	})
	t.Run("PD error", func(t *testing.T) {
		loadErr := errors.New("keyspace lookup failed")
		codecClient, err := NewCodecPDClientWithKeyspace(apicodec.ModeTxn, &mockPDClient{Client: pdClient, loadErr: loadErr}, "test-keyspace")
		require.ErrorIs(t, err, loadErr)
		require.Nil(t, codecClient)
	})
	t.Run("missing metadata", func(t *testing.T) {
		codecClient, err := NewCodecPDClientWithKeyspace(apicodec.ModeTxn, &mockPDClient{Client: pdClient}, "test-keyspace")
		require.ErrorContains(t, err, "keyspaceMeta is nil")
		require.Nil(t, codecClient)
	})
}

func TestGetKeyspaceIDRejectsV3Identity(t *testing.T) {
	re := require.New(t)
	client, cluster, _, err := testutils.NewMockTiKV("", nil)
	re.NoError(err)
	defer client.Close()
	meta := &keyspacepb.KeyspaceMeta{
		State: keyspacepb.KeyspaceState_ENABLED,
		Keyspace: &keyspacepb.KeyspaceMeta_KeyspaceIdentity{
			KeyspaceIdentity: &apipb.KeyspaceIdentity{NamespaceId: 1, KeyspaceId: 2},
		},
	}
	pdClient := &mockPDClient{Client: mocktikv.NewPDClient(cluster), meta: meta}
	_, err = GetKeyspaceID(pdClient, "foo")
	re.Error(err)
}
