package cas

import (
	"context"
	"sync"
	"time"

	"github.com/pborman/uuid"
)

// tag identifies a pubsub channel for routing purposes.
type tag string

// pubsub provides a simple pubsub implementation to route messages and wait for them.
type pubsub struct {
	subs map[tag]chan any
	mu   sync.Mutex
	wg   sync.WaitGroup
}

// sub returns a routing tag and a channel to the subscriber to read messages from.
//
// Only requests associated with the returned tag are sent on the returned channel.
//
// The returned channel is unbufferred and is closed when the specified context is done.
// The subscriber must unsubscribe by cancelling the specified context.
// The subscriber must continue draining the returned channel until it's closed to avoid
// send-on-closed-channel and deadlock errors.
//
// A slow subscriber affects all other subscribers that share the same message.
func (ps *pubsub) sub(ctx context.Context) (tag, <-chan any) {
	t := tag(uuid.New())

	// Serialize this block to avoid concurrent map-read-write errors.
	ps.mu.Lock()
	subscriber, ok := ps.subs[t]
	if !ok {
		subscriber = make(chan any)
		ps.subs[t] = subscriber
	}
	ps.mu.Unlock()

	ps.wg.Add(1)
	go func() {
		defer ps.wg.Done()
		<-ctx.Done()

		// Serialize this block to avoid concurrent map-read-write and send-on-closed-channel errors.
		ps.mu.Lock()
		delete(ps.subs, t)
		ps.mu.Unlock()

		close(subscriber)
	}()

	return t, subscriber
}

// pub fans-out a response to all subscribers sequentially.
// Returns when all active subscribers have received their copies.
// May deadlock if a subscriber never reveives its copy.
// A busy subscriber does not block others from receiving their copies. It is instead
// rescheduled for another attempt once all others get a chance to receive.
// To prevent a temporarily infinite round-robin loop from consuming too much CPU, each subscriber
// gets at most 10ms to receive before getting rescheduled.
// Blocking 10ms for every subscriber amortizes much better than blocking 10ms for every
// iteration on the subscribers, even though both have the same worst-case cost.
// For example, if out of 10 subscribers 5 were busy for 1ms, the attempt will cost ~5ms instead of 10ms.
func (ps *pubsub) pub(r any, ts ...tag) {
	// Serialize this block to avoid concurrent map-read-write and send-on-closed-channel errors.
	ps.mu.Lock()
	defer ps.mu.Unlock()

	var toRetry []tag
	ticker := time.NewTicker(10 * time.Millisecond)
	for {
		for _, t := range ts {
			subscriber, ok := ps.subs[t]
			if !ok {
				continue
			}
			// Send now or reschedule if the subscriber is not ready.
			select {
			case subscriber <- r:
			case <-ticker.C:
				toRetry = append(toRetry, t)
			}
		}
		if len(toRetry) == 0 {
			break
		}
		// Reuse the underlying arrays by swapping slices and resetting one of them.
		ts, toRetry = toRetry, ts
		toRetry = toRetry[:0]
	}
}

// wait blocks until all subscribers have unsubscribed.
func (ps *pubsub) wait() {
	ps.wg.Wait()
}

func newPubSub() *pubsub {
	return &pubsub{subs: make(map[tag]chan any)}
}
