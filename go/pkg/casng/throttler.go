package casng

import (
	"context"
	"sync/atomic"

	"golang.org/x/sync/semaphore"
)

type throttler struct {
	s *semaphore.Weighted
	n atomic.Int32
}

func (t *throttler) acquire(ctx context.Context) bool {
	// err is always ctx.Err()
	err := t.s.Acquire(ctx, 1)
	if err != nil {
		return false
	}
	t.n.Add(1)
	return true
}

func (t *throttler) release() {
	t.s.Release(1)
	t.n.Add(-1)
}

// len is not synchronized and may not return a stale snapshot.
func (t *throttler) len() int {
	return int(t.n.Load())
}

func newThrottler(n int64) *throttler {
	return &throttler{s: semaphore.NewWeighted(n)}
}
