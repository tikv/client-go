package client

import "sync/atomic"

const (
	Kp = 0.01
	Ki = 0.001
)

type WindowsLimit struct {
	cap    float64
	used   atomic.Int32
	sum    float64
	minCap float64
	kp     float64
}

func NewWindowsLimit(cap float64, kp float64) *WindowsLimit {
	return &WindowsLimit{
		cap:    cap,
		minCap: cap,
		kp:     kp,
	}
}

func (w *WindowsLimit) Available() uint {
	if w.cap > float64(w.used.Load()) {
		return uint(w.cap - float64(w.used.Load()))
	}
	return 0
}

func (w *WindowsLimit) Reset() {
	w.used.Store(0)
}

func (w *WindowsLimit) Feedback(err float64) {
	if w.Available() > uint(w.cap/2) && err > 0 {
		return
	}
	w.sum += err
	w.cap = float64(w.kp*w.sum + w.kp*0.1*err)
	if w.cap < w.minCap {
		w.cap = w.minCap
	}
}

func (w *WindowsLimit) Capacity() float64 {
	return w.cap
}

func (w *WindowsLimit) Used() int {
	return int(w.used.Load())
}
