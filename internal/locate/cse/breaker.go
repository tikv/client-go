package cse

import (
	"errors"
	"sync/atomic"
	"time"

	"github.com/sony/gobreaker"
)

const (
	open uint32 = iota
	closed
)

var (
	errUnavailable = errors.New("resource unavailable")
)

type asyncBreaker struct {
	cb    *gobreaker.CircuitBreaker
	state uint32
	done  chan struct{}

	probeInterval time.Duration
}

type settings struct {
	Name          string
	MaxRequests   uint32
	Interval      time.Duration
	Timeout       time.Duration
	ProbeInterval time.Duration
	ReadyToTrip   func(counts gobreaker.Counts) bool
	IsSuccessful  func(err error) bool
	Probe         func(string) error
}

func newAsyncBreaker(s settings) *asyncBreaker {
	breaker := &asyncBreaker{
		state: closed,
		done:  make(chan struct{}, 1),

		probeInterval: s.ProbeInterval,
	}
	cbs := gobreaker.Settings{
		Name:         s.Name,
		MaxRequests:  s.MaxRequests,
		Interval:     s.Interval,
		Timeout:      s.Timeout,
		ReadyToTrip:  s.ReadyToTrip,
		IsSuccessful: s.IsSuccessful,
	}
	cbs.OnStateChange = func(_ string, from gobreaker.State, to gobreaker.State) {
		if from == gobreaker.StateClosed && to == gobreaker.StateOpen {
			breaker.openWith(s.Probe)
		}
	}
	breaker.cb = gobreaker.NewCircuitBreaker(cbs)
	return breaker
}

func (b *asyncBreaker) Close() {
	b.done <- struct{}{}
	close(b.done)
}

func (b *asyncBreaker) openWith(probe func(string) error) bool {
	success := atomic.CompareAndSwapUint32(&b.state, closed, open)
	if success {
		go b.probeLoop(probe)
	}
	return success
}

func (b *asyncBreaker) probeLoop(probe func(string) error) {
	ticker := time.NewTicker(b.probeInterval)
	for {
		select {
		case <-ticker.C:
			err := probe(b.cb.Name())
			if err != nil {
				continue
			}
			atomic.CompareAndSwapUint32(&b.state, open, closed)
			return
		case <-b.done:
			return
		}
	}
}

func (b *asyncBreaker) Execute(f func() (any, error)) (any, error) {
	return b.cb.Execute(func() (any, error) {
		if atomic.LoadUint32(&b.state) == open {
			return nil, errUnavailable
		}
		return f()
	})
}
