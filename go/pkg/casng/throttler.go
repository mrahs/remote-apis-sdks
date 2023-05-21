package casng

import (
	"context"
	"sync/atomic"
)

// throttler provides a simple interface to limit in-flight goroutines.
type throttler struct {
	ch chan struct{}
	n  atomic.Int32
}

// acquire blocks until there is slot for a goroutine to be in-flight.
//
// Returns false if ctx expires before a slot is available. Otherwise returns true.
func (t *throttler) acquire(ctx context.Context) bool {
	for {
		select {
		case t.ch <- struct{}{}:
			t.n.Add(1)
			return true
		case <-ctx.Done():
			return false
		}
	}
}

// release must be called after acquire. Otherwise, it will block until acquire is called.
func (t *throttler) release() {
	<-t.ch
	t.n.Add(-1)
}

// len is not synchronized and may not return a stale snapshot.
func (t *throttler) len() int {
	return int(t.n.Load())
}

// newThrottler creates a new instance that allows up to n goroutines to be in-flight.
func newThrottler(n int64) *throttler {
	return &throttler{ch: make(chan struct{}, n)}
}
