package cse

import (
	"context"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

var (
	_ pd.Client = &Fallback{}
)

type Fallback struct {
	pd.Client
	cse     pd.Client
	breaker *asyncBreaker
}

func probePD(name string, client pd.Client, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, err := client.GetRegionByID(ctx, 1)
	if err == nil {
		log.Warn("mark pd client as available", zap.String("name", name))
		return nil
	}
	log.Warn("pd client still unavailable", zap.String("name", name))
	return err
}

func NewFallback(client pd.Client) (*Fallback, error) {
	f := &Fallback{
		Client: client,
	}
	cse, err := NewCSEClient(client)
	if err != nil {
		return nil, err
	}
	f.cse = cse

	s := settings{
		Name:          "pd-fallback-client",
		Interval:      5 * time.Second,
		Timeout:       1 * time.Second,
		ProbeInterval: 1 * time.Second,
		ReadyToTrip:   ifMostFailures,
		Probe: func(name string) error {
			log.Warn("origin pd client unavailable, start probing", zap.String("name", name))
			return probePD(name, client, 1*time.Second)
		},
	}

	breaker := newAsyncBreaker(s)
	f.breaker = breaker

	return f, nil
}

func (f *Fallback) GetRegion(ctx context.Context, key []byte, opts ...pd.GetRegionOption) (*pd.Region, error) {
	resp, err := f.breaker.Execute(func() (interface{}, error) {
		return f.Client.GetRegion(ctx, key, opts...)
	})
	if err == nil {
		return resp.(*pd.Region), nil
	}
	return f.cse.GetRegion(ctx, key, opts...)
}

func (f *Fallback) GetPrevRegion(ctx context.Context, key []byte, opts ...pd.GetRegionOption) (*pd.Region, error) {
	resp, err := f.breaker.Execute(func() (interface{}, error) {
		return f.Client.GetPrevRegion(ctx, key, opts...)
	})
	if err == nil {
		return resp.(*pd.Region), nil
	}
	return f.cse.GetPrevRegion(ctx, key, opts...)
}

func (f *Fallback) GetRegionByID(ctx context.Context, regionID uint64, opts ...pd.GetRegionOption) (*pd.Region, error) {
	resp, err := f.breaker.Execute(func() (interface{}, error) {
		return f.Client.GetRegionByID(ctx, regionID, opts...)
	})
	if err == nil {
		return resp.(*pd.Region), nil
	}
	return f.cse.GetRegionByID(ctx, regionID, opts...)
}

func (f *Fallback) ScanRegions(ctx context.Context, key, endKey []byte, limit int) ([]*pd.Region, error) {
	resp, err := f.breaker.Execute(func() (interface{}, error) {
		return f.Client.ScanRegions(ctx, key, endKey, limit)
	})
	if err == nil {
		return resp.([]*pd.Region), nil
	}
	return f.cse.ScanRegions(ctx, key, endKey, limit)
}

func (f *Fallback) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	resp, err := f.breaker.Execute(func() (interface{}, error) {
		return f.Client.GetStore(ctx, storeID)
	})
	if err == nil {
		return resp.(*metapb.Store), nil
	}
	return f.cse.GetStore(ctx, storeID)
}

func (f *Fallback) GetAllStores(ctx context.Context, opts ...pd.GetStoreOption) ([]*metapb.Store, error) {
	resp, err := f.breaker.Execute(func() (interface{}, error) {
		return f.Client.GetAllStores(ctx, opts...)
	})
	if err == nil {
		return resp.([]*metapb.Store), nil
	}
	return f.cse.GetAllStores(ctx, opts...)
}

func (f *Fallback) Close() {
	f.cse.Close()
	f.breaker.Close()
}
