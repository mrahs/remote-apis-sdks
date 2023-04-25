package cas

import (
	"context"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/pborman/uuid"
)

// tag identifies a pubsub channel for routing purposes.
type tag string

// pubsub provides a simple pubsub implementation to route messages and wait for them.
type pubsub struct {
	subs map[tag]chan any
	mu   sync.RWMutex
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
	glog.V(3).Infof("pubsub.sub: tag=%s", t)

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
		glog.V(3).Infof("pubsub.unsub: tag=%s", t)
	}()

	return t, subscriber
}

// pub is a blocking call that fans-out a response to all specified (by tag) subscribers sequentially.
//
// Returns when all active subscribers have received their copies.
// May deadlock if an active subscriber never reveives its copy.
// Inactive subscribers (expired by cancelling their context) are skipped.
//
// A busy subscriber does not block others from receiving their copies. It is instead
// rescheduled for another attempt once all others get a chance to receive.
// To prevent a temporarily infinite round-robin loop from consuming too much CPU, each subscriber
// gets at most 10ms to receive before getting rescheduled.
// Blocking 10ms for every subscriber amortizes much better than blocking 10ms for every
// iteration on the subscribers, even though both have the same worst-case cost.
// For example, if out of 10 subscribers 5 were busy for 1ms, the attempt will cost ~5ms instead of 10ms.
func (ps *pubsub) pub(m any, tags ...tag) {
	_ = ps.pubN(m, len(tags), tags...)
}

// mpub (multi-publish) delivers the "once" message to a single consumer then delivers the "rest" message to the rest of the consumers.
// It's useful for cases where the message holds shared information that should not be duplicated among consumers, such as stats.
func (ps *pubsub) mpub(once any, rest any, tags ...tag) {
	t := ps.pubOnce(once, tags...)
	_ = ps.pubN(rest, len(tags)-1, excludeTag(tags, t)...)
}

// pubOnce is like pub, but delivers the message only once. The tag of the receiver that got
// the message is returned.
func (ps *pubsub) pubOnce(m any, tags ...tag) tag {
	received := ps.pubN(m, 1, tags...)
	if len(received) == 0 {
		return ""
	}
	return received[0]
}

// pubN is like pub, but delivers the message to exactly n consumers. The list of tags that
// got the message is returned.
func (ps *pubsub) pubN(m any, n int, tags ...tag) []tag {
	glog.V(3).Infof("pubsub.pub: tags=%v", tags)

	ps.mu.RLock()
	defer ps.mu.RUnlock()

	glog.V(3).Infof("pubsub.pub: tags=%v, subs=%v", tags, ps.subs)
	var toRetry []tag
	var received []tag
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		for _, t := range tags {
			subscriber, ok := ps.subs[t]
			if !ok {
				continue
			}
			// Send now or reschedule if the subscriber is not ready.
			select {
			case subscriber <- m:
				received = append(received, t)
				if len(received) >= n {
					return received
				}
			case <-ticker.C:
				toRetry = append(toRetry, t)
			}
		}
		if len(toRetry) == 0 {
			break
		}
		// Reuse the underlying arrays by swapping slices and resetting one of them.
		tags, toRetry = toRetry, tags
		toRetry = toRetry[:0]
	}
	return received
}

// wait blocks until all subscribers have unsubscribed.
func (ps *pubsub) wait() {
	ps.wg.Wait()
}

func newPubSub() *pubsub {
	return &pubsub{subs: make(map[tag]chan any)}
}

func excludeTag(tags []tag, et tag) []tag {
	ts := make([]tag, 0, len(tags)-1)
	for _, t := range tags {
		if t == et {
			continue
		}
		ts = append(ts, t)
	}
	return ts
}
