package tikv_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

var _ tikv.Client = &resourceGroupTagMockClient{}

type resourceGroupTagMockClient struct {
	tikv.Client

	t            *testing.T
	expectedTag  []byte
	requestCount int
}

func (c *resourceGroupTagMockClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	if len(req.ResourceGroupTag) == 0 {
		return c.Client.SendRequest(ctx, addr, req, timeout)
	}
	c.requestCount++
	assert.Equal(c.t, c.expectedTag, req.ResourceGroupTag)
	return c.Client.SendRequest(ctx, addr, req, timeout)
}

func TestResourceGroupTag(t *testing.T) {
	testTag1 := []byte("TEST-TAG-1")
	testTag2 := []byte("TEST-TAG-2")
	testTagger := tikvrpc.ResourceGroupTagger(func(req *tikvrpc.Request) {
		req.ResourceGroupTag = testTag2
	})

	/* Get */

	// SetResourceGroupTag
	store := NewTestStore(t)
	client := &resourceGroupTagMockClient{t: t, Client: store.GetTiKVClient(), expectedTag: testTag1}
	store.SetTiKVClient(client)
	txn, err := store.Begin()
	assert.NoError(t, err)
	txn.SetResourceGroupTag(testTag1)
	_, _ = txn.Get(context.Background(), []byte{})
	assert.Equal(t, 1, client.requestCount)
	assert.NoError(t, store.Close())

	// SetResourceGroupTagger
	store = NewTestStore(t)
	client = &resourceGroupTagMockClient{t: t, Client: store.GetTiKVClient(), expectedTag: testTag2}
	store.SetTiKVClient(client)
	txn, err = store.Begin()
	assert.NoError(t, err)
	txn.SetResourceGroupTagger(testTagger)
	_, _ = txn.Get(context.Background(), []byte{})
	assert.Equal(t, 1, client.requestCount)
	assert.NoError(t, store.Close())

	// SetResourceGroupTag + SetResourceGroupTagger
	store = NewTestStore(t)
	client = &resourceGroupTagMockClient{t: t, Client: store.GetTiKVClient(), expectedTag: testTag1}
	store.SetTiKVClient(client)
	txn, err = store.Begin()
	assert.NoError(t, err)
	txn.SetResourceGroupTag(testTag1)
	txn.SetResourceGroupTagger(testTagger)
	_, _ = txn.Get(context.Background(), []byte{})
	assert.Equal(t, 1, client.requestCount)
	assert.NoError(t, store.Close())

	/* BatchGet */

	// SetResourceGroupTag
	store = NewTestStore(t)
	client = &resourceGroupTagMockClient{t: t, Client: store.GetTiKVClient(), expectedTag: testTag1}
	store.SetTiKVClient(client)
	txn, err = store.Begin()
	assert.NoError(t, err)
	txn.SetResourceGroupTag(testTag1)
	_, _ = txn.BatchGet(context.Background(), [][]byte{[]byte("k")})
	assert.Equal(t, 1, client.requestCount)
	assert.NoError(t, store.Close())

	// SetResourceGroupTagger
	store = NewTestStore(t)
	client = &resourceGroupTagMockClient{t: t, Client: store.GetTiKVClient(), expectedTag: testTag2}
	store.SetTiKVClient(client)
	txn, err = store.Begin()
	assert.NoError(t, err)
	txn.SetResourceGroupTagger(testTagger)
	_, _ = txn.BatchGet(context.Background(), [][]byte{[]byte("k")})
	assert.Equal(t, 1, client.requestCount)
	assert.NoError(t, store.Close())

	// SetResourceGroupTag + SetResourceGroupTagger
	store = NewTestStore(t)
	client = &resourceGroupTagMockClient{t: t, Client: store.GetTiKVClient(), expectedTag: testTag1}
	store.SetTiKVClient(client)
	txn, err = store.Begin()
	assert.NoError(t, err)
	txn.SetResourceGroupTag(testTag1)
	txn.SetResourceGroupTagger(testTagger)
	_, _ = txn.BatchGet(context.Background(), [][]byte{[]byte("k")})
	assert.Equal(t, 1, client.requestCount)
	assert.NoError(t, store.Close())

	/* Scan */

	// SetResourceGroupTag
	store = NewTestStore(t)
	client = &resourceGroupTagMockClient{t: t, Client: store.GetTiKVClient(), expectedTag: testTag1}
	store.SetTiKVClient(client)
	txn, err = store.Begin()
	assert.NoError(t, err)
	txn.SetResourceGroupTag(testTag1)
	_, _ = txn.Iter([]byte("abc"), []byte("def"))
	assert.Equal(t, 1, client.requestCount)
	assert.NoError(t, store.Close())

	// SetResourceGroupTagger
	store = NewTestStore(t)
	client = &resourceGroupTagMockClient{t: t, Client: store.GetTiKVClient(), expectedTag: testTag2}
	store.SetTiKVClient(client)
	txn, err = store.Begin()
	assert.NoError(t, err)
	txn.SetResourceGroupTagger(testTagger)
	_, _ = txn.Iter([]byte("abc"), []byte("def"))
	assert.Equal(t, 1, client.requestCount)
	assert.NoError(t, store.Close())

	// SetResourceGroupTag + SetResourceGroupTagger
	store = NewTestStore(t)
	client = &resourceGroupTagMockClient{t: t, Client: store.GetTiKVClient(), expectedTag: testTag1}
	store.SetTiKVClient(client)
	txn, err = store.Begin()
	assert.NoError(t, err)
	txn.SetResourceGroupTag(testTag1)
	txn.SetResourceGroupTagger(testTagger)
	_, _ = txn.Iter([]byte("abc"), []byte("def"))
	assert.Equal(t, 1, client.requestCount)
	assert.NoError(t, store.Close())
}
