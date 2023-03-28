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

func NewCSEClient(origin pd.Client) (*Client, error) {
	ctx := context.Background()
	stores, err := origin.GetAllStores(ctx)
	if err != nil {
		return nil, err
	}
	c := &Client{
		Client: origin,

		mu: struct {
			sync.Mutex
			stores []*metapb.Store
		}{
			stores: stores,
		},

		done: make(chan struct{}, 1),

		httpClient: &http.Client{
			Transport: &http.Transport{},
		},

		gp: util.NewSpool(16, time.Second*10),
	}
	go c.getAllStoresLoop()
	return c, nil
}

func (c *Client) getAllStoresLoop() {
	ticker := time.NewTicker(time.Second * 5)
	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
			stores, err := c.GetAllStores(context.Background())
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
	stores := func() []*metapb.Store {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.mu.stores
	}()
	ss := make([]*metapb.Store, 0, len(stores))
	for _, store := range stores {
		if tikvrpc.GetStoreTypeByMeta(store) != tikvrpc.TiKV {
			continue
		}
		ss = append(ss, store)
	}
	return ss
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

func mkOK(idx int, ok []*pdcore.RegionInfo) result {
	return result{ok: ok, index: idx}
}

func mkErr(idx int, err error) result {
	return result{err: err, index: idx}
}

func (c *Client) fanout(ctx context.Context, method, endpoint string, req any) (*pdcore.RegionsInfo, error) {
	stores := c.getStores()
	rchan := make(chan result, len(stores))
	for i, s := range stores {
		store := *s
		index := i
		c.gp.Go(func() {
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
			for i, leader := range syncResp.RegionLeaders {
				if leader.GetId() != 0 {
					region := syncResp.Regions[i]
					log.Info("sync region leader from cse", zap.Any("region", region))
					regionInfos = append(regionInfos, pdcore.NewRegionInfo(region, leader))
				}
			}
			rchan <- mkOK(index, regionInfos)
		})
	}

	regionsInfo := pdcore.NewRegionsInfo()
	for i := 0; i < len(stores); i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case r := <-rchan:
			if err := r.err; err != nil {
				log.Warn("failed to sync regions from CSE", zap.Error(err))
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
