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
	"github.com/tikv/client-go/v2/metrics"
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
	SyncRegionTimeout     = time.Second * 2
)

var (
	ErrRegionNotFound = errors.New("region not found")
)

type store struct {
	*metapb.Store
	breaker *asyncBreaker
}

type Client struct {
	pd.Client

	done chan struct{}

	mu struct {
		sync.RWMutex
		stores map[uint64]*store
	}

	httpClient *http.Client

	gp *util.Spool
}

func NewCSEClient(origin pd.Client) (c *Client, err error) {
	c = &Client{
		Client: origin,

		done: make(chan struct{}, 1),

		httpClient: &http.Client{
			Transport: &http.Transport{},
		},

		gp: util.NewSpool(32, time.Second*10),
	}
	c.mu.stores = make(map[uint64]*store)
	err = c.refreshStores()
	if err != nil {
		return nil, err
	}
	go c.refreshStoresLoop()
	return c, nil
}

func (c *Client) probeStoreStatus(name string, addr string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://%s", addr), nil)
	if err != nil {
		return err
	}
	_, err = c.httpClient.Do(req)
	if err != nil {
		log.Warn("failed to check store", zap.String("name", name), zap.Error(err))
		return err
	}
	log.Warn("mark store reachable", zap.String("name", name), zap.String("store", addr))
	return nil
}

func (c *Client) updateStores(stores []*metapb.Store) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, s := range stores {
		if origin, ok := c.mu.stores[s.Id]; !ok {
			s := &store{
				Store: s,
			}
			settings := settings{
				Name:          fmt.Sprintf("store-%d", s.GetId()),
				Interval:      5 * time.Second,
				Timeout:       1 * time.Second,
				ProbeInterval: 1 * time.Second,
				ReadyToTrip:   ifMostFailures,
				Probe: func(name string) error {
					addr := s.GetStatusAddress()
					log.Warn("store is marked as unavailable, probing",
						zap.String("name", name),
						zap.String("store", addr))
					return c.probeStoreStatus(name, addr, 1*time.Second)
				},
			}
			s.breaker = newAsyncBreaker(settings)
			c.mu.stores[s.GetId()] = s
		} else {
			if s.GetState() == metapb.StoreState_Tombstone {
				origin.breaker.Close()
				delete(c.mu.stores, s.GetId())
				continue
			}
			origin.Store = s
		}
	}
}

func (c *Client) getAllStores() []*metapb.Store {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var stores []*metapb.Store
	for _, s := range c.mu.stores {
		stores = append(stores, s.Store)
	}
	return stores
}

func (c *Client) refreshStores() error {
	stores, err := c.Client.GetAllStores(context.Background(), pd.WithExcludeTombstone())
	if err != nil {
		return err
	}
	c.updateStores(stores)
	return nil
}

func (c *Client) refreshStoresLoop() {
	ticker := time.NewTicker(StoresRefreshInterval)
	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
			err := c.refreshStores()
			if err != nil {
				log.Error("refresh stores failed", zap.Error(err))
			}
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
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, s := range c.mu.stores {
		s.breaker.Close()
		delete(c.mu.stores, s.GetId())
	}
}

func (c *Client) getAliveTiKVStores() []*store {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var tikvStores []*store
	for _, s := range c.mu.stores {
		if s.GetState() == metapb.StoreState_Up && tikvrpc.GetStoreTypeByMeta(s.Store) == tikvrpc.TiKV {
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
	start := time.Now()
	defer func() {
		metrics.TiKVSyncRegionDuration.
			WithLabelValues([]string{endpoint}...).
			Observe(time.Since(start).Seconds())
	}()

	stores := c.getAliveTiKVStores()
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
			r, err := store.breaker.Execute(func() (any, error) {
				return c.httpClient.Do(req)
			})
			if err != nil {
				rchan <- mkErr(index, err)
				return
			}
			resp := r.(*http.Response)

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
					downPeers := syncResp.DownPeers[i].Peers
					pendingPeers := syncResp.PendingPeers[i].Peers
					regionInfo := pdcore.NewRegionInfo(region, leader,
						pdcore.WithDownPeers(downPeers), pdcore.WithPendingPeers(pendingPeers))
					if bs := syncResp.GetBuckets()[i]; bs.RegionId > 0 {
						regionInfo.UpdateBuckets(bs, nil)
					}
					regionInfos = append(regionInfos, regionInfo)
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
				log.Warn("failed to sync regions from cse", zap.Error(err), zap.String("store", stores[r.index].StatusAddress))
			}
			for _, regionInfo := range r.ok {
				regionsInfo.CheckAndPutRegion(regionInfo)
			}
		}
	}
	return regionsInfo, nil
}

func mkPDRegions(regions ...*pdcore.RegionInfo) []*pd.Region {
	rs := make([]*pd.Region, 0, len(regions))
	for _, region := range regions {
		pdRegion := &pd.Region{
			Meta:         region.GetMeta(),
			Leader:       region.GetLeader(),
			Buckets:      region.GetBuckets(),
			PendingPeers: region.GetPendingPeers(),
		}
		for _, p := range region.GetDownPeers() {
			pdRegion.DownPeers = append(pdRegion.DownPeers, p.Peer)
		}
		rs = append(rs, pdRegion)
	}
	return rs
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
		return nil, ErrRegionNotFound
	}
	resp := mkPDRegions(region)[0]
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
		return nil, ErrRegionNotFound
	}
	return mkPDRegions(region)[0], nil
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
		return nil, ErrRegionNotFound
	}
	return mkPDRegions(region)[0], nil
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
		return nil, ErrRegionNotFound
	}
	return mkPDRegions(regions...), nil
}

func (c *Client) GetAllStores(context.Context, ...pd.GetStoreOption) ([]*metapb.Store, error) {
	return c.getAllStores(), nil
}

func (c *Client) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	s, ok := c.mu.stores[storeID]
	if !ok {
		return nil, errors.Errorf("store %d not found", storeID)
	}
	return s.Store, nil
}
