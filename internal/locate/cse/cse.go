package cse

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util"
	"github.com/tikv/client-go/v2/util/codec"
	pd "github.com/tikv/pd/client"
	pdcore "github.com/tikv/pd/pkg/core"
	"go.uber.org/zap"
)

var (
	_ pd.Client = &Client{}
)

var (
	StoresRefreshInterval = time.Second * 5
	SyncRegionTimeout     = time.Second * 10
)

type Client struct {
	pd.Client

	done chan struct{}

	mu struct {
		sync.Mutex
		stores []*metapb.Store
	}

	httpClient *http.Client

	gp *util.Spool

	// id atomic.Int64
}

func NewCSEClient(origin pd.Client) (c *Client, err error) {
	c = &Client{
		Client: origin,

		done: make(chan struct{}, 1),

		httpClient: &http.Client{
			Transport: &http.Transport{},
		},

		gp: util.NewSpool(16, time.Second*10),
	}
	c.mu.stores, err = c.Client.GetAllStores(context.Background(), pd.WithExcludeTombstone())
	if err != nil {
		return nil, err
	}
	go c.refreshStoresLoop()
	return c, nil
}

func (c *Client) refreshStoresLoop() {
	ticker := time.NewTicker(StoresRefreshInterval)
	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
			stores, err := c.Client.GetAllStores(context.Background(), pd.WithExcludeTombstone())
			if err != nil {
				log.Warn("get all stores failed", zap.Error(err))
				continue
			}
			c.mu.Lock()
			c.mu.stores = stores
			c.mu.Unlock()
		}
	}
}

func (c *Client) Close() {
	// Cancel all the ongoing requests.
	c.gp.Close()
	// Close the idle connections.
	c.httpClient.CloseIdleConnections()
	// Close the stores refresh goroutine.
	c.done <- struct{}{}
	// Close the origin PDClient.
	c.Client.Close()
}

func (c *Client) getStores() []*metapb.Store {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mu.stores
}

func (c *Client) GetAllStores(context.Context, ...pd.GetStoreOption) ([]*metapb.Store, error) {
	return c.getStores(), nil
}

func (c *Client) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	for _, s := range c.getStores() {
		if s.Id == storeID {
			return s, nil
		}
	}
	return nil, errors.Errorf("store %d not found", storeID)
}

func (c *Client) getValidTiKVStores() []*metapb.Store {
	var tikvStores []*metapb.Store
	for _, s := range c.getStores() {
		if s.GetState() == metapb.StoreState_Up && tikvrpc.GetStoreTypeByMeta(s) == tikvrpc.TiKV {
			tikvStores = append(tikvStores, s)
		}
	}
	return tikvStores
}

type SyncRegionRequest struct {
	Start   string `json:"start"`
	End     string `json:"end"`
	Limit   uint64 `json:"limit"`
	Reverse bool   `json:"reverse"`
}

type SyncRegionByIDRequest struct {
	RegionID uint64 `json:"region-id"`
}

type result struct {
	ok  []*pdcore.RegionInfo
	err error

	index int
}

func mkOK(ok []*pdcore.RegionInfo) result {
	return result{ok: ok}
}

func mkErr(idx int, err error) result {
	return result{err: err, index: idx}
}

func (c *Client) fanout(ctx context.Context, method, endpoint string, req any) (*pdcore.RegionsInfo, error) {
	stores := c.getValidTiKVStores()
	rchan := make(chan result, len(stores))
	for i, s := range stores {
		store := *s
		index := i
		c.gp.Go(func() {
			ctx, cancel := context.WithTimeout(ctx, SyncRegionTimeout)
			defer cancel()
			buf := new(bytes.Buffer)
			err := json.NewEncoder(buf).Encode(req)
			if err != nil {
				rchan <- mkErr(index, err)
				return
			}
			req, err := http.NewRequestWithContext(ctx, method, fmt.Sprintf("http://%s/%s", store.StatusAddress, endpoint), buf)
			if err != nil {
				rchan <- mkErr(index, err)
				return
			}
			resp, err := c.httpClient.Do(req)
			if err != nil {
				rchan <- mkErr(index, err)
				return
			}

			if resp.StatusCode != http.StatusOK {
				rchan <- mkErr(index, fmt.Errorf("unexpected status code: %d", resp.StatusCode))
				return
			}

			syncResp := &pdpb.SyncRegionResponse{}
			func() {
				var buf []byte
				defer resp.Body.Close()
				buf, err = io.ReadAll(resp.Body)
				if err != nil {
					return
				}
				err = proto.Unmarshal(buf, syncResp)
				if err != nil {
					return
				}
			}()
			if err != nil {
				rchan <- mkErr(index, err)
				return
			}
			regionInfos := make([]*pdcore.RegionInfo, 0, len(syncResp.Regions))
			// TODO(iosmanthus): extract buckets and peer info from resp.
			for i, leader := range syncResp.RegionLeaders {
				if leader.GetId() != 0 {
					region := syncResp.Regions[i]
					log.Debug("sync region leader from cse", zap.Any("region", region))
					regionInfos = append(regionInfos, pdcore.NewRegionInfo(region, leader))
				}
			}
			rchan <- mkOK(regionInfos)
		})
	}

	regionsInfo := pdcore.NewRegionsInfo()
	for i := 0; i < len(stores); i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case r := <-rchan:
			if err := r.err; err != nil {
				log.Warn("failed to sync regions from CSE", zap.Error(err), zap.Any("store", stores[r.index]))
			}
			for _, regionInfo := range r.ok {
				regionsInfo.CheckAndPutRegion(regionInfo)
			}
		}
	}
	return regionsInfo, nil
}

func (c *Client) GetRegion(ctx context.Context, key []byte, pts ...pd.GetRegionOption) (*pd.Region, error) {
	_, start, err := codec.DecodeBytes(key, nil)
	if err != nil {
		return nil, err
	}
	regionsInfo, err := c.fanout(ctx, http.MethodGet, "sync_region", &SyncRegionRequest{
		Start: hex.EncodeToString(start),
		End:   hex.EncodeToString(append(start, 0)),
		Limit: 1,
	})
	if err != nil {
		return nil, err
	}
	region := regionsInfo.GetRegionByKey(key)
	if region == nil {
		return nil, errors.New("no region found")
	}
	resp := &pd.Region{
		Meta:   region.GetMeta(),
		Leader: region.GetLeader(),
	}
	return resp, nil
}

func (c *Client) GetPrevRegion(ctx context.Context, key []byte, pts ...pd.GetRegionOption) (*pd.Region, error) {
	_, start, err := codec.DecodeBytes(key, nil)
	if err != nil {
		return nil, err
	}
	regionsInfo, err := c.fanout(ctx, http.MethodGet, "sync_region", &SyncRegionRequest{
		End:     hex.EncodeToString(start),
		Limit:   2,
		Reverse: true,
	})
	if err != nil {
		return nil, err
	}
	region := regionsInfo.GetPrevRegionByKey(key)
	if region == nil {
		return nil, errors.New("no region found")
	}
	resp := &pd.Region{
		Meta:   region.GetMeta(),
		Leader: region.GetLeader(),
	}
	return resp, nil
}

func (c *Client) GetRegionByID(ctx context.Context, regionID uint64, opts ...pd.GetRegionOption) (*pd.Region, error) {
	regionsInfo, err := c.fanout(ctx, http.MethodGet, "sync_region_by_id", &SyncRegionByIDRequest{
		RegionID: regionID,
	})
	if err != nil {
		return nil, err
	}
	region := regionsInfo.GetRegion(regionID)
	if region == nil {
		return nil, errors.New("no region found")
	}
	resp := &pd.Region{
		Meta:   region.GetMeta(),
		Leader: region.GetLeader(),
	}
	return resp, nil
}

func (c *Client) ScanRegions(ctx context.Context, startKey, endKey []byte, limit int) ([]*pd.Region, error) {
	if limit <= 0 {
		limit = 0
	}
	_, start, err := codec.DecodeBytes(startKey, nil)
	if err != nil {
		return nil, err
	}
	_, end, err := codec.DecodeBytes(endKey, nil)
	if err != nil {
		return nil, err
	}
	regionsInfo, err := c.fanout(ctx, http.MethodGet, "sync_region", &SyncRegionRequest{
		Start: hex.EncodeToString(start),
		End:   hex.EncodeToString(end),
		Limit: uint64(limit),
	})
	if err != nil {
		return nil, err
	}
	regions := regionsInfo.ScanRange(startKey, endKey, limit)
	if len(regions) == 0 {
		return nil, errors.New("no region found")
	}
	resp := make([]*pd.Region, 0, len(regions))
	for _, region := range regions {
		resp = append(resp, &pd.Region{
			Meta:   region.GetMeta(),
			Leader: region.GetLeader(),
		})
	}
	return resp, nil
}
