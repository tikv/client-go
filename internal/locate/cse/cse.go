package cse

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/golang/protobuf/proto" //nolint:staticcheck
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

type ctxKey string

var (
	reqIDKey ctxKey = "cse-req-id"
)

type store struct {
	*metapb.Store
	breaker *asyncBreaker
}

type Client struct {
	pd.Client

	done chan struct{}

	cbOpt *CBOptions

	mu struct {
		sync.RWMutex
		stores map[uint64]*store
	}

	httpClient *http.Client

	gp *util.Spool
}

func NewCSEClient(origin pd.Client, cbOpt *CBOptions) (c *Client, err error) {
	if cbOpt == nil {
		cbOpt = defaultCBOptions()
	}

	c = &Client{
		Client: origin,

		done: make(chan struct{}, 1),

		cbOpt: cbOpt,

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
	resp, err := c.httpClient.Do(req)
	if err != nil {
		log.Warn("failed to check store", zap.String("name", name), zap.Error(err))
		return err
	}
	defer resp.Body.Close()
	log.Warn("mark store reachable", zap.String("name", name), zap.String("store", addr))
	return nil
}

func (c *Client) updateStores(stores []*metapb.Store) {
	latestStores := make(map[uint64]*metapb.Store)
	for _, s := range stores {
		latestStores[s.Id] = s
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	// remove tombstone stores.
	for id, s := range c.mu.stores {
		if _, ok := latestStores[id]; !ok {
			s.breaker.Close()
			delete(c.mu.stores, id)
		}
	}

	// update stores.
	for id, latest := range latestStores {
		s, ok := c.mu.stores[id]
		if ok {
			s.Store = latest
			continue
		}

		s = &store{
			Store: latest,
		}

		settings := settings{
			Name:          fmt.Sprintf("store-%d", id),
			Interval:      c.cbOpt.Interval,
			Timeout:       c.cbOpt.Timeout,
			ProbeInterval: c.cbOpt.ProbeInterval,
			ReadyToTrip:   c.cbOpt.ReadyToTrip,
			Probe: func(name string) error {
				addr := s.GetStatusAddress()
				log.Warn("store is marked as unavailable, probing",
					zap.String("name", name),
					zap.String("store", addr))
				return c.probeStoreStatus(name, addr, 1*time.Second)
			},
		}
		// init circuit breaker for the store.
		s.breaker = newAsyncBreaker(settings)

		c.mu.stores[id] = s
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
	// the stores we cached are filtered out the tombstone store.
	for _, s := range c.mu.stores {
		if tikvrpc.GetStoreTypeByMeta(s.Store) == tikvrpc.TiKV {
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

func mkOK(idx int, ok []*pdcore.RegionInfo) result {
	return result{ok: ok, index: idx}
}

func mkErr(idx int, err error) result {
	return result{err: err, index: idx}
}

// fanout sends the request to all the "up" TiKV stores concurrently.
// Then, collect the region results, and merge them into a BTreeMap: RegionsInfo provided by the PD.
// Finally, return the RegionsInfo.
func (c *Client) fanout(ctx context.Context, tag, method, endpoint string, req any) (*pdcore.RegionsInfo, error) {
	reqID := getReqID(ctx)
	start := time.Now()
	defer func() {
		metrics.TiKVSyncRegionDuration.
			WithLabelValues([]string{tag}...).
			Observe(time.Since(start).Seconds())
	}()

	stores := c.getAliveTiKVStores()
	ss := make([]string, 0, len(stores))
	for _, s := range stores {
		ss = append(ss, s.GetStatusAddress())
	}
	log.Info("sync region from", zap.Strings("stores", ss), zap.Uint64("req", reqID))
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
			// If the breaker is open or the store is probed as unreachable,
			// the request will be rejected immediately.
			r, err := store.breaker.Execute(func() (any, error) {
				return c.httpClient.Do(req) //nolint:bodyclose
			})
			if err != nil {
				rchan <- mkErr(index, err)
				return
			}
			resp := r.(*http.Response)
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				rchan <- mkErr(index, fmt.Errorf("unexpected status code: %d", resp.StatusCode))
				return
			}

			syncResp := &pdpb.SyncRegionResponse{}
			func() {
				var buf []byte
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
				// Found a region with leader.
				if leader.GetId() != 0 {
					region := syncResp.Regions[i]
					downPeers := syncResp.DownPeers[i].Peers
					pendingPeers := syncResp.PendingPeers[i].Peers
					regionInfo := pdcore.NewRegionInfo(region, leader,
						pdcore.WithDownPeers(downPeers), pdcore.WithPendingPeers(pendingPeers))
					// if the region has buckets, update the region infos.
					if bs := syncResp.GetBuckets(); len(bs) > 0 && bs[i].RegionId > 0 {
						regionInfo.UpdateBuckets(bs[i], nil)
					}
					regionInfos = append(regionInfos, regionInfo)
				}
			}
			rchan <- mkOK(index, regionInfos)
		})
	}

	regionsInfo := pdcore.NewRegionsInfo()
	for i := 0; i < len(stores); i++ {
		select {
		case <-ctx.Done():
			log.Warn("context canceled when syncing regions from cse",
				zap.Error(ctx.Err()), zap.Uint64("req", reqID))
			return nil, ctx.Err()
		case r := <-rchan:
			if err := r.err; err != nil {
				log.Warn("failed to sync regions from cse",
					zap.Error(err),
					zap.String("store", stores[r.index].StatusAddress),
					zap.Uint64("req", reqID))
			}
			for _, regionInfo := range r.ok {
				log.Info("sync region from cse",
					zap.String("type", tag),
					zap.String("region", regionInfo.GetMeta().String()),
					zap.String("store", stores[r.index].StatusAddress),
					zap.Uint64("req", reqID),
				)
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

func withReqID(ctx context.Context) context.Context {
	return context.WithValue(ctx, reqIDKey, rand.Uint64())
}

func getReqID(ctx context.Context) uint64 {
	if id, ok := ctx.Value(reqIDKey).(uint64); ok {
		return id
	}
	return 0
}

func (c *Client) GetRegion(ctx context.Context, key []byte, _ ...pd.GetRegionOption) (*pd.Region, error) {
	ctx = withReqID(ctx)
	reqID := getReqID(ctx)
	_, start, err := codec.DecodeBytes(key, nil)
	if err != nil {
		return nil, err
	}
	regionsInfo, err := c.fanout(ctx, "GetRegion", http.MethodGet, "sync_region", &SyncRegionRequest{
		Start: hex.EncodeToString(start),
		End:   hex.EncodeToString(append(start, 0)),
		Limit: 1,
	})
	if err != nil {
		return nil, err
	}
	region := regionsInfo.GetRegionByKey(key)
	if region == nil {
		log.Warn("region not found",
			zap.String("key", hex.EncodeToString(key)),
			zap.Uint64("req", reqID))
		return nil, ErrRegionNotFound
	}
	resp := mkPDRegions(region)[0]
	return resp, nil
}

func (c *Client) GetPrevRegion(ctx context.Context, key []byte, _ ...pd.GetRegionOption) (*pd.Region, error) {
	ctx = withReqID(ctx)
	reqID := getReqID(ctx)
	_, start, err := codec.DecodeBytes(key, nil)
	if err != nil {
		return nil, err
	}
	regionsInfo, err := c.fanout(ctx, "GetPrevRegion", http.MethodGet, "sync_region", &SyncRegionRequest{
		// Must include the current region of the `key`.
		// Otherwise, the regionsInfo might return nil while the region is not found.
		// Check https://github.com/tikv/pd/blob/400e3bd30c563bdb6b18ee7d59e56d082afff6a6/pkg/core/region_tree.go#L188
		End:     hex.EncodeToString(append(start, 0)),
		Limit:   2,
		Reverse: true,
	})
	if err != nil {
		return nil, err
	}
	region := regionsInfo.GetPrevRegionByKey(key)
	if region == nil {
		log.Warn("region not found",
			zap.String("key", hex.EncodeToString(key)),
			zap.Uint64("req", reqID))
		return nil, ErrRegionNotFound
	}
	return mkPDRegions(region)[0], nil
}

func (c *Client) GetRegionByID(ctx context.Context, regionID uint64, _ ...pd.GetRegionOption) (*pd.Region, error) {
	ctx = withReqID(ctx)
	reqID := getReqID(ctx)
	regionsInfo, err := c.fanout(ctx, "GetRegionByID", http.MethodGet, "sync_region_by_id", &SyncRegionByIDRequest{
		RegionID: regionID,
	})
	if err != nil {
		return nil, err
	}
	region := regionsInfo.GetRegion(regionID)
	if region == nil {
		log.Warn("region not found",
			zap.Uint64("id", regionID),
			zap.Uint64("req", reqID))
		return nil, ErrRegionNotFound
	}
	return mkPDRegions(region)[0], nil
}

func (c *Client) ScanRegions(ctx context.Context, startKey, endKey []byte, limit int) ([]*pd.Region, error) {
	ctx = withReqID(ctx)
	reqID := getReqID(ctx)
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
	regionsInfo, err := c.fanout(ctx, "ScanRegions", http.MethodGet, "sync_region", &SyncRegionRequest{
		Start: hex.EncodeToString(start),
		End:   hex.EncodeToString(end),
		Limit: uint64(limit),
	})
	if err != nil {
		return nil, err
	}
	regions := regionsInfo.ScanRange(startKey, endKey, limit)
	if len(regions) == 0 {
		log.Warn("region not found",
			zap.String("start", hex.EncodeToString(startKey)),
			zap.String("end", hex.EncodeToString(endKey)),
			zap.Uint64("req", reqID))
		return nil, ErrRegionNotFound
	}
	return mkPDRegions(regions...), nil
}

// GetAllStores returns all stores in the cluster except tombstone stores.
func (c *Client) GetAllStores(context.Context, ...pd.GetStoreOption) ([]*metapb.Store, error) {
	return c.getAllStores(), nil
}

func (c *Client) GetStore(_ context.Context, storeID uint64) (*metapb.Store, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	s, ok := c.mu.stores[storeID]
	if !ok {
		return nil, errors.Errorf("store %d not found", storeID)
	}
	return s.Store, nil
}
