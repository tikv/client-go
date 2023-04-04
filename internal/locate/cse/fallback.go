package cse

import (
	"context"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pkg/errors"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/sony/gobreaker"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

var (
	_ pd.Client = &Fallback{}
)

type Fallback struct {
	pd.Client
	cse             pd.Client
	cb              *gobreaker.CircuitBreaker
	originAvailable uint32
	done            chan struct{}
}

func NewFallback(client pd.Client) (*Fallback, error) {
	f := &Fallback{
		Client:          client,
		originAvailable: 1,
		done:            make(chan struct{}, 1),
	}
	cse, err := NewCSEClient(client)
	if err != nil {
		return nil, err
	}
	f.cse = cse
	cb := gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:     "pd-fallback-client",
		Interval: 5 * time.Second,
		Timeout:  5 * time.Second,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
			return counts.Requests >= 5 && failureRatio >= 0.4
		},
		OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
			switch {
			case from == gobreaker.StateClosed && to == gobreaker.StateOpen:
				log.Warn("mark pd client as unavailable", zap.String("name", name))
				atomic.CompareAndSwapUint32(&f.originAvailable, 1, 0)
				go func() {
					ticker := time.NewTicker(1 * time.Second)
					for {
						select {
						case <-ticker.C:
							var success bool
							func() {
								ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
								defer cancel()
								_, err := client.GetRegionByID(ctx, 1)
								if err == nil {
									log.Warn("mark pd client as available", zap.String("name", name))
									atomic.CompareAndSwapUint32(&f.originAvailable, 0, 1)
									success = true
									return
								}
								log.Warn("pd client still unavailable")
							}()
							if success {
								return
							}
						case <-f.done:
							return
						default:
						}
					}
				}()
			}
		},
		IsSuccessful: nil,
	})
	f.cb = cb
	return f, nil
}

func (f *Fallback) GetRegion(ctx context.Context, key []byte, opts ...pd.GetRegionOption) (*pd.Region, error) {
	resp, err := f.cb.Execute(func() (interface{}, error) {
		if atomic.LoadUint32(&f.originAvailable) == 0 {
			return nil, errors.New("origin pd client is not available")
		}
		return f.Client.GetRegion(ctx, key, opts...)
	})
	if err == nil {
		return resp.(*pd.Region), nil
	}
	return f.cse.GetRegion(ctx, key, opts...)
}

func (f *Fallback) GetPrevRegion(ctx context.Context, key []byte, opts ...pd.GetRegionOption) (*pd.Region, error) {
	resp, err := f.cb.Execute(func() (interface{}, error) {
		if atomic.LoadUint32(&f.originAvailable) == 0 {
			return nil, errors.New("origin pd client is not available")
		}
		return f.Client.GetPrevRegion(ctx, key, opts...)
	})
	if err == nil {
		return resp.(*pd.Region), nil
	}
	return f.cse.GetPrevRegion(ctx, key, opts...)
}

func (f *Fallback) GetRegionByID(ctx context.Context, regionID uint64, opts ...pd.GetRegionOption) (*pd.Region, error) {
	resp, err := f.cb.Execute(func() (interface{}, error) {
		if atomic.LoadUint32(&f.originAvailable) == 0 {
			return nil, errors.New("origin pd client is not available")
		}
		return f.Client.GetRegionByID(ctx, regionID, opts...)
	})
	if err == nil {
		return resp.(*pd.Region), nil
	}
	return f.cse.GetRegionByID(ctx, regionID, opts...)
}

func (f *Fallback) ScanRegions(ctx context.Context, key, endKey []byte, limit int) ([]*pd.Region, error) {
	resp, err := f.cb.Execute(func() (interface{}, error) {
		if atomic.LoadUint32(&f.originAvailable) == 0 {
			return nil, errors.New("origin pd client is not available")
		}
		return f.Client.ScanRegions(ctx, key, endKey, limit)
	})
	if err == nil {
		return resp.([]*pd.Region), nil
	}
	return f.cse.ScanRegions(ctx, key, endKey, limit)
}

func (f *Fallback) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	resp, err := f.cb.Execute(func() (interface{}, error) {
		if atomic.LoadUint32(&f.originAvailable) == 0 {
			return nil, errors.New("origin pd client is not available")
		}
		return f.Client.GetStore(ctx, storeID)
	})
	if err == nil {
		return resp.(*metapb.Store), nil
	}
	return f.cse.GetStore(ctx, storeID)
}

func (f *Fallback) GetAllStores(ctx context.Context, opts ...pd.GetStoreOption) ([]*metapb.Store, error) {
	resp, err := f.cb.Execute(func() (interface{}, error) {
		if atomic.LoadUint32(&f.originAvailable) == 0 {
			return nil, errors.New("origin pd client is not available")
		}
		return f.Client.GetAllStores(ctx, opts...)
	})
	if err == nil {
		return resp.([]*metapb.Store), nil
	}
	return f.cse.GetAllStores(ctx, opts...)
}

func (f *Fallback) Close() {
	f.cse.Close()
	f.done <- struct{}{}
}
