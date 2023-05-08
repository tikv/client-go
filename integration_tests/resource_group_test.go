package tikv_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

var _ tikv.Client = &resourceGroupNameMockClient{}

type resourceGroupNameMockClient struct {
	tikv.Client

	t            *testing.T
	expectedTag  string
	requestCount int
}

func (c *resourceGroupNameMockClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	if req.GetResourceControlContext().GetResourceGroupName() == c.expectedTag {
		c.requestCount++
	}
	return c.Client.SendRequest(ctx, addr, req, timeout)
}

func TestResourceGroupName(t *testing.T) {
	testTag := "test"
	/* Get */
	store := NewTestStore(t)
	client := &resourceGroupNameMockClient{t: t, Client: store.GetTiKVClient(), expectedTag: testTag}
	store.SetTiKVClient(client)
	txn, err := store.Begin()
	assert.NoError(t, err)
	txn.SetResourceGroupName(testTag)
	_, _ = txn.Get(context.Background(), []byte{})
	assert.Equal(t, 1, client.requestCount)
	assert.NoError(t, store.Close())

	/* BatchGet */
	store = NewTestStore(t)
	client = &resourceGroupNameMockClient{t: t, Client: store.GetTiKVClient(), expectedTag: testTag}
	store.SetTiKVClient(client)
	txn, err = store.Begin()
	assert.NoError(t, err)
	txn.SetResourceGroupName(testTag)
	_, _ = txn.BatchGet(context.Background(), [][]byte{[]byte("k")})
	assert.Equal(t, 1, client.requestCount)
	assert.NoError(t, store.Close())

	/* Scan */
	store = NewTestStore(t)
	client = &resourceGroupNameMockClient{t: t, Client: store.GetTiKVClient(), expectedTag: testTag}
	store.SetTiKVClient(client)
	txn, err = store.Begin()
	assert.NoError(t, err)
	txn.SetResourceGroupName(testTag)
	_, _ = txn.Iter([]byte("abc"), []byte("def"))
	assert.Equal(t, 1, client.requestCount)
	assert.NoError(t, store.Close())
}
