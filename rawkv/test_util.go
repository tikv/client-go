package rawkv

import (
	"github.com/ergesun/client-go/v2/internal/client"
	"github.com/ergesun/client-go/v2/internal/locate"
	pd "github.com/tikv/pd/client"
)

func NewClientForMock(pdClient pd.Client, rpcClient client.Client) *Client {
	return &Client{
		clusterID:   0,
		regionCache: locate.NewRegionCache(pdClient),
		pdClient:    pdClient,
		rpcClient:   rpcClient,
	}
}
