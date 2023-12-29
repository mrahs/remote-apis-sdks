package casng

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pborman/uuid"
)

// pubsub provides a simple pubsub implementation to route messages and wait for them.
type pubsub struct {
	subs map[string]chan any
	mu   sync.RWMutex
	// A signalling channel that gets closed every time the broker hits 0 subscriptions.
	// Unlike sync.WaitGroup, this allows the broker to accept more subs while a client is waiting for signal.
	done chan struct{}
}

// sub returns a routing id and a channel to the subscriber to read messages from.
//
// Only messages associated with the returned route are sent on the returned channel.
// This allows the subscriber to send a tagged message (request) that propagates across the system and eventually
// receive related messages (responses) from publishers on the returned channel.
//
// The subscriber must continue draining the returned channel until it's closed.
// The returned channel is unbuffered and closed only when unsub is called with the returned route.
//
// To properly terminate the subscription, the subscriber must wait until all expected responses are received
// on the returned channel before unsubscribing.
// Once unsubscribed, any tagged messages for this subscription are dropped.
func (ps *pubsub) sub(ctx context.Context) (string, <-chan any) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if len(ps.subs) == 0 {
		ps.done = make(chan struct{})
	}
	route := uuid.New()
	subscriber := make(chan any, 100000)
	ps.subs[route] = subscriber

	ctx = ctxWithValues(ctxWithLogDepthInc(ctx), ctxKeyRtID, route)
	infof(ctx, 4, "sub")
	return route, subscriber
}

// unsub schedules the subscription to be removed as soon as in-flight pubs are done.
// The subscriber must continue draining the channel until it's closed.
// It is an error to publish more messages for route after this call.
func (ps *pubsub) unsub(ctx context.Context, route string) {
	ctx = ctxWithValues(ctxWithLogDepthInc(ctx), ctxKeyRtID, route)
	infof(ctx, 4, "unsub.scheduled")
	// If unsub is called from the same goroutine that is listening on the subscription
	// channel, a deadlock might occur.
	// pub would be holding a read lock while this call wants to hold a write lock that must
	// wait for all reads to finish. However, that read will never finish because the corresponding goroutine
	// is blocked on this call.
	// Ideally, the user should call unsub after confirming all pub calls have returned. However, this
	// relieves the user from that burden with minimal overhead.
	go func() {
		// reset depth to 0 because goroutines do not retain call stacks.
		ctx := ctxWithValues(ctx, ctxKeyLogDepth, 0)
		ps.mu.Lock()
		defer ps.mu.Unlock()
		subscriber, ok := ps.subs[route]
		if !ok {
			return
		}
		delete(ps.subs, route)
		close(subscriber)
		if len(ps.subs) == 0 {
			close(ps.done)
			infof(ctx, 4, "done")
		}
		infof(ctx, 4, "unsub.done")
	}()
}

// pub is a blocking call that fans-out a response to all specified (by route) subscribers concurrently.
//
// Returns when all active subscribers have received their copies or timed out.
// Inactive subscribers (expired by cancelling their context) are skipped (their copies are dropped).
//
// A busy subscriber does not block others from receiving their copies. It is instead
// rescheduled for another attempt once all others get a chance to receive.
// To prevent a temporarily infinite round-robin loop from consuming too much CPU, each subscriber
// gets at most 10ms to receive before getting rescheduled.
// Blocking 10ms for every subscriber amortizes much better than blocking 10ms for every
// iteration on the subscribers, even though both have the same worst-case cost.
// For example, if out of 10 subscribers 5 were busy for 1ms, the attempt will cost ~5ms instead of 10ms.
func (ps *pubsub) pub(ctx context.Context, m any, routes ...string) {
	ctx = ctxWithLogDepthInc(ctx)
	_ = ps.pubN(ctx, m, len(routes), routes...)
}

func (ps *pubsub) pubZip(ctx context.Context, msgs []any, routes []string) error {
	ctx = ctxWithLogDepthInc(ctx)
	if len(msgs) != len(routes) {
		return fmt.Errorf("pubZip: slice length mismatch, msgs=%d, routes=%d, %s", len(msgs), len(routes), fmtCtx(ctx))
	}
	defer durationf(ctx, time.Now(), "pubzip", "count", len(msgs))

	ps.mu.RLock()
	defer ps.mu.RUnlock()

	wg := sync.WaitGroup{}
	wg.Add(len(msgs))
	for i := range msgs {
		go func(m any, r string){
			defer wg.Done()
			fctx := ctxWithValues(ctx, ctxKeyRtID, r)
			subscriber, ok := ps.subs[r]
			if !ok {
				warnf(fctx, "dropped orphaned message")
				return
			}
			subscriber <- m
			infof(fctx, 4, "sent")
		}(msgs[i], routes[i])
	}
	wg.Wait()
	return nil
}

// mpub (multi-publish) delivers the "once" message to a single subscriber then delivers the "rest" message to the rest of the subscribers.
// It's useful for cases where the message holds shared information that should not be duplicated among subscribers, such as stats.
func (ps *pubsub) mpub(ctx context.Context, once any, rest any, routes ...string) {
	ctx = ctxWithLogDepthInc(ctx)

	usedRoute := ""
	usedRoutes := ps.pubN(ctx, once, 1, routes...)
	if len(usedRoutes) > 0 {
		usedRoute = usedRoutes[0]
	}
	_ = ps.pubN(ctx, rest, len(routes)-1, excludeRoute(routes, usedRoute)...)
}

// pubN is like pub, but delivers the message to no more than n subscribers. The routes of the subscribers that got the message are returned.
func (ps *pubsub) pubN(ctx context.Context, m any, n int, routes ...string) []string {
	ctx = ctxWithLogDepthInc(ctx)
	defer durationf(ctx, time.Now(), "pub")

	if len(routes) == 0 {
		warnf(ctx, "called without routes, dropping message")
		infof(ctx, 5, "called without routes", "msg", m)
		return nil
	}
	if n <= 0 {
		warnf(ctx, "nothing published", "n", n)
		return nil
	}

	ps.mu.RLock()
	defer ps.mu.RUnlock()

	var toRetry []string
	var received []string
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	retryCount := 0
	for {
		for _, r := range routes {
			fctx := ctxWithValues(ctx, ctxKeyRtID, r)
			subscriber, ok := ps.subs[r]
			if !ok {
				warnf(fctx, "dropped orphaned message")
				continue
			}
			// Send now or reschedule if the subscriber is not ready.
			select {
			case subscriber <- m:
				infof(fctx, 4, "sent")
				received = append(received, r)
				if len(received) >= n {
					return received
				}
			case <-ticker.C:
				toRetry = append(toRetry, r)
			}
		}
		if len(toRetry) == 0 {
			break
		}
		retryCount++
		infof(ctxWithValues(ctx, ctxKeyRtID, toRetry), 4, "retry", "#", retryCount, "count", len(toRetry))

		// Avoid mutating routes because it's expected to remain the same set upstream.
		// Reslicing toRetry allows shifting retries to the left without without reallocating a new array.
		routes = toRetry
		toRetry = toRetry[:0]
	}
	return received
}

// wait blocks until all existing subscribers unsubscribe.
// The signal is a snapshot. The broker my get more subscribers after returning from this call.
func (ps *pubsub) wait() {
	ps.mu.RLock()
	done := ps.done
	ps.mu.RUnlock()
	if done == nil {
		return
	}
	<-done
}

// len returns the number of active subscribers.
func (ps *pubsub) len() int {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	return len(ps.subs)
}

// newPubSub initializes a new instance where subscribers must receive messages within timeout.
func newPubSub() *pubsub {
	return &pubsub{
		subs: make(map[string]chan any),
	}
}

// excludeRoute is used by mpub to filter out the route that received the "once" message.
func excludeRoute(routes []string, et string) []string {
	if len(routes) == 0 {
		return []string{}
	}
	// Remove by swapping the item with the last one and then excluding the last index.
	// This approach avoids allocating a new underlying array without losing any items
	// in the original unsorted array.
	i := -1
	for index, t := range routes {
		if t == et {
			i = index
			break
		}
	}
	if i < 0 {
		return routes
	}
	j := len(routes) - 1
	routes[i], routes[j] = routes[j], routes[i]
	return routes[:j]
}
