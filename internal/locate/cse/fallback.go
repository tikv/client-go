package cse

import (
	"context"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/sony/gobreaker"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

var (
	_ pd.Client = &ClientWithFallback{}
)

type ClientWithFallback struct {
	pd.Client
	cse     pd.Client
	breaker *asyncBreaker
}

// CBOptions is a wrapper for gobreaker settings.
type CBOptions struct {
	Interval      time.Duration
	Timeout       time.Duration
	ProbeInterval time.Duration
	ReadyToTrip   func(counts gobreaker.Counts) bool
}

func defaultCBOptions() *CBOptions {
	return &CBOptions{
		Interval:      5 * time.Second,
		Timeout:       1 * time.Second,
		ProbeInterval: 1 * time.Second,
		ReadyToTrip:   ifMostFailures,
	}
}

func ifMostFailures(counts gobreaker.Counts) bool {
	failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
	return counts.Requests >= 5 && failureRatio >= 0.4
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

func NewClientWithFallback(client pd.Client, cbOpt *CBOptions) (*ClientWithFallback, error) {
	if cbOpt == nil {
		cbOpt = defaultCBOptions()
	}

	f := &ClientWithFallback{
		Client: client,
	}
	cse, err := NewCSEClient(client, cbOpt)
	if err != nil {
		return nil, err
	}
	f.cse = cse

	s := settings{
		Name:          "pd-fallback-client",
		Interval:      cbOpt.Interval,
		Timeout:       cbOpt.Timeout,
		ProbeInterval: cbOpt.ProbeInterval,
		ReadyToTrip:   cbOpt.ReadyToTrip,
		Probe: func(name string) error {
			log.Warn("origin pd client unavailable, start probing", zap.String("name", name))
			return probePD(name, client, 1*time.Second)
		},
	}

	breaker := newAsyncBreaker(s)
	f.breaker = breaker

	return f, nil
}

func (f *ClientWithFallback) GetRegion(ctx context.Context, key []byte, opts ...pd.GetRegionOption) (*pd.Region, error) {
	resp, err := f.breaker.Execute(func() (interface{}, error) {
		return f.Client.GetRegion(ctx, key, opts...)
	})
	if err == nil {
		return resp.(*pd.Region), nil
	}
	return f.cse.GetRegion(ctx, key, opts...)
}

func (f *ClientWithFallback) GetPrevRegion(ctx context.Context, key []byte, opts ...pd.GetRegionOption) (*pd.Region, error) {
	resp, err := f.breaker.Execute(func() (interface{}, error) {
		return f.Client.GetPrevRegion(ctx, key, opts...)
	})
	if err == nil {
		return resp.(*pd.Region), nil
	}
	return f.cse.GetPrevRegion(ctx, key, opts...)
}

func (f *ClientWithFallback) GetRegionByID(ctx context.Context, regionID uint64, opts ...pd.GetRegionOption) (*pd.Region, error) {
	resp, err := f.breaker.Execute(func() (interface{}, error) {
		return f.Client.GetRegionByID(ctx, regionID, opts...)
	})
	if err == nil {
		return resp.(*pd.Region), nil
	}
	return f.cse.GetRegionByID(ctx, regionID, opts...)
}

func (f *ClientWithFallback) ScanRegions(ctx context.Context, key, endKey []byte, limit int) ([]*pd.Region, error) {
	resp, err := f.breaker.Execute(func() (interface{}, error) {
		return f.Client.ScanRegions(ctx, key, endKey, limit)
	})
	if err == nil {
		return resp.([]*pd.Region), nil
	}
	return f.cse.ScanRegions(ctx, key, endKey, limit)
}

func (f *ClientWithFallback) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	resp, err := f.breaker.Execute(func() (interface{}, error) {
		return f.Client.GetStore(ctx, storeID)
	})
	if err == nil {
		return resp.(*metapb.Store), nil
	}
	return f.cse.GetStore(ctx, storeID)
}

func (f *ClientWithFallback) GetAllStores(ctx context.Context, opts ...pd.GetStoreOption) ([]*metapb.Store, error) {
	resp, err := f.breaker.Execute(func() (interface{}, error) {
		return f.Client.GetAllStores(ctx, opts...)
	})
	if err == nil {
		return resp.([]*metapb.Store), nil
	}
	return f.cse.GetAllStores(ctx, opts...)
}

func (f *ClientWithFallback) Close() {
	f.cse.Close()
	f.breaker.Close()
}
