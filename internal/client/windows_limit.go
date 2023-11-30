package client

const (
	Kp = 10
	Ki = 1
)

type WindowsLimit struct {
	cap  int
	used int
	sum  uint64
}

func NewWindowsLimit(cap int) *WindowsLimit {
	return &WindowsLimit{
		cap: cap,
	}
}

func (w *WindowsLimit) Available() int {
	return w.cap - w.used
}

func (w *WindowsLimit) Take(n int) {
	w.used += n
}

func (w *WindowsLimit) Ack(n int) {
	w.used -= n
}

func (w *WindowsLimit) Feedback(err uint64) {
	if w.Available() > 0 {
		return
	}
	w.sum += err
	w.cap = int(Kp*w.sum + Ki*err)
	if w.cap < 100 {
		w.cap = 100
	}
}

func (w *WindowsLimit) Capacity() int {
	return w.cap
}
