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
	t            *testing.T
	expectedTag  []byte
	requestCount int
}

func (c *resourceGroupTagMockClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	c.requestCount++
	assert.Equal(c.t, c.expectedTag, req.ResourceGroupTag)
	return &tikvrpc.Response{}, nil
}

func (c *resourceGroupTagMockClient) Close() error {
	return nil
}

func TestResourceGroupTag(t *testing.T) {
	testTag1 := []byte("TEST-TAG-1")
	testTag2 := []byte("TEST-TAG-2")
	testTagger := tikvrpc.ResourceGroupTagger(func(req *tikvrpc.Request) {
		req.ResourceGroupTag = testTag2
	})

	/* Get */

	// SetResourceGroupTag
	client := &resourceGroupTagMockClient{t: t, expectedTag: testTag1}
	store := NewTestStore(t)
	store.SetTiKVClient(client)
	txn, err := store.Begin()
	assert.NoError(t, err)
	txn.SetResourceGroupTag(testTag1)
	_, _ = txn.Get(context.Background(), []byte{})
	assert.Equal(t, 1, client.requestCount)

	// SetResourceGroupTagger
	client = &resourceGroupTagMockClient{t: t, expectedTag: testTag2}
	store = NewTestStore(t)
	store.SetTiKVClient(client)
	txn, err = store.Begin()
	assert.NoError(t, err)
	txn.SetResourceGroupTagger(testTagger)
	_, _ = txn.Get(context.Background(), []byte{})
	assert.Equal(t, 1, client.requestCount)

	// SetResourceGroupTag + SetResourceGroupTagger
	client = &resourceGroupTagMockClient{t: t, expectedTag: testTag1}
	store = NewTestStore(t)
	store.SetTiKVClient(client)
	txn, err = store.Begin()
	assert.NoError(t, err)
	txn.SetResourceGroupTag(testTag1)
	txn.SetResourceGroupTagger(testTagger)
	_, _ = txn.Get(context.Background(), []byte{})
	assert.Equal(t, 1, client.requestCount)

	/* BatchGet */

	// SetResourceGroupTag
	client = &resourceGroupTagMockClient{t: t, expectedTag: testTag1}
	store = NewTestStore(t)
	store.SetTiKVClient(client)
	txn, err = store.Begin()
	assert.NoError(t, err)
	txn.SetResourceGroupTag(testTag1)
	_, _ = txn.BatchGet(context.Background(), [][]byte{[]byte("k")})
	assert.Equal(t, 1, client.requestCount)

	// SetResourceGroupTagger
	client = &resourceGroupTagMockClient{t: t, expectedTag: testTag2}
	store = NewTestStore(t)
	store.SetTiKVClient(client)
	txn, err = store.Begin()
	assert.NoError(t, err)
	txn.SetResourceGroupTagger(testTagger)
	_, _ = txn.BatchGet(context.Background(), [][]byte{[]byte("k")})
	assert.Equal(t, 1, client.requestCount)

	// SetResourceGroupTag + SetResourceGroupTagger
	client = &resourceGroupTagMockClient{t: t, expectedTag: testTag1}
	store = NewTestStore(t)
	store.SetTiKVClient(client)
	txn, err = store.Begin()
	assert.NoError(t, err)
	txn.SetResourceGroupTag(testTag1)
	txn.SetResourceGroupTagger(testTagger)
	_, _ = txn.BatchGet(context.Background(), [][]byte{[]byte("k")})
	assert.Equal(t, 1, client.requestCount)

	/* Scan */

	// SetResourceGroupTag
	client = &resourceGroupTagMockClient{t: t, expectedTag: testTag1}
	store = NewTestStore(t)
	store.SetTiKVClient(client)
	txn, err = store.Begin()
	assert.NoError(t, err)
	txn.SetResourceGroupTag(testTag1)
	_, _ = txn.Iter([]byte("abc"), []byte("def"))
	assert.Equal(t, 1, client.requestCount)

	// SetResourceGroupTagger
	client = &resourceGroupTagMockClient{t: t, expectedTag: testTag2}
	store = NewTestStore(t)
	store.SetTiKVClient(client)
	txn, err = store.Begin()
	assert.NoError(t, err)
	txn.SetResourceGroupTagger(testTagger)
	_, _ = txn.Iter([]byte("abc"), []byte("def"))
	assert.Equal(t, 1, client.requestCount)

	// SetResourceGroupTag + SetResourceGroupTagger
	client = &resourceGroupTagMockClient{t: t, expectedTag: testTag1}
	store = NewTestStore(t)
	store.SetTiKVClient(client)
	txn, err = store.Begin()
	assert.NoError(t, err)
	txn.SetResourceGroupTag(testTag1)
	txn.SetResourceGroupTagger(testTagger)
	_, _ = txn.Iter([]byte("abc"), []byte("def"))
	assert.Equal(t, 1, client.requestCount)
}
