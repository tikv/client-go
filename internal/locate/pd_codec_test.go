package locate

import (
	"context"
	"testing"

	"github.com/pingcap/kvproto/pkg/apipb"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"

	"github.com/tikv/client-go/v2/internal/mockstore/mocktikv"
	"github.com/tikv/client-go/v2/testutils"
)

// mockPDClient wraps a mock PD client and lets tests control LoadKeyspace.
type mockPDClient struct {
	pd.Client
	meta *keyspacepb.KeyspaceMeta
}

func (c *mockPDClient) LoadKeyspace(ctx context.Context, name string) (*keyspacepb.KeyspaceMeta, error) {
	return c.meta, nil
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
