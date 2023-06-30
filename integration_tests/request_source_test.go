package tikv_test

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/assert"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util"
)

var _ tikv.Client = &requestSourceMockClient{}

type requestSource struct {
	tag        string
	expectType kvrpcpb.RequestSourceType
}
type requestSourceMockClient struct {
	tikv.Client

	t            *testing.T
	expectedTag  requestSource
	requestCount int
}

func (c *requestSourceMockClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	re := assert.New(c.t)
	re.NotNil(req.RequestSource)
	re.Contains(req.RequestSource, c.expectedTag.tag)
	re.Equal(c.expectedTag.tag, req.RequestSourceV2.Tag)
	re.Equal(c.expectedTag.expectType, req.RequestSourceV2.SourceType)
	return c.Client.SendRequest(ctx, addr, req, timeout)
}

func TestRequestResourceV2(t *testing.T) {
	testcases := []requestSource{
		{"", kvrpcpb.RequestSourceType_Default},
		{"br", kvrpcpb.RequestSourceType_BR},
		{"lightning", kvrpcpb.RequestSourceType_Lightning},
	}

	for _, ca := range testcases {
		/* Get */
		store := NewTestStore(t)
		client := &requestSourceMockClient{t: t, Client: store.GetTiKVClient(), expectedTag: ca}
		store.SetTiKVClient(client)
		txn, err := store.Begin()
		assert.NoError(t, err)
		txn.SetRequestSourceType(ca.tag)
		_, _ = txn.Get(context.Background(), []byte{})

		/* Scan */
		txn, err = store.Begin()
		assert.NoError(t, err)
		txn.SetRequestSourceType(ca.tag)
		_, _ = txn.Iter([]byte("abc"), []byte("def"))

		/* Commit */
		k1 := []byte("k1")
		k2 := []byte("k2")
		err = txn.Set(k1, k1)
		assert.Nil(t, err)
		err = txn.Set(k2, k2)
		assert.Nil(t, err)
		ctx := context.Background()
		// async commit would not work if sessionID is 0(default value).
		util.SetSessionID(ctx, uint64(10001))
		txn.SetRequestSourceType(ca.tag)
		err = txn.Commit(ctx)
		assert.Nil(t, err)
		assert.NoError(t, store.Close())
	}
}
