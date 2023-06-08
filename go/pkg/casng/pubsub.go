package casng

import (
	"context"
	"sync"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/contextmd"
	log "github.com/golang/glog"
	"github.com/pborman/uuid"
)

// tag identifies a pubsub channel for routing purposes.
// Producers tag messages and consumers subscribe to tags.
type tag string

// pubsub provides a simple pubsub implementation to route messages and wait for them.
type pubsub struct {
	subs map[tag]chan any
	mu   sync.RWMutex
	wg   sync.WaitGroup
}

// sub returns a routing tag and a channel to the subscriber to read messages from.
//
// Only messages associated with the returned tag are sent on the returned channel.
// This allows the subscriber to send a tagged message (request) that propagates across the system and eventually
// received related messages (responses) on the returned channel.
//
// ctx is only used to wait for a an unsubscription signal. It is not propagated with any messages.
// The subscriber must unsubscribe by cancelling the specified context.
// The subscriber must continue draining the returned channel until it's closed.
// The returned channel is unbuffered and only closed when the specified context is done.
//
// To properly terminate the subscription, the subscriber must wait until all expected responses are received
// on the returned channel before cancelling the context.
// Once the context is cancelled, any tagged messages for this subscription are dropped.
//
// A slow subscriber affects all other subscribers that are waiting for the same message.
func (ps *pubsub) sub(ctx context.Context) (tag, <-chan any) {
	t := tag(uuid.New())
	contextmd.Infof(ctx, log.Level(4), "[casng] pubsub.sub: tag=%s", t)

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

		contextmd.Infof(ctx, log.Level(4), "[casng] pubsub.unsub: tag=%s", t)

		// Serialize this block to avoid concurrent map-read-write and send-on-closed-channel errors.
		ps.mu.Lock()
		delete(ps.subs, t)
		ps.mu.Unlock()

		close(subscriber)
	}()

	return t, subscriber
}

// pub is a blocking call that fans-out a response to all specified (by tag) subscribers sequentially.
//
// Returns when all active subscribers have received their copies.
// Will deadlock if an active subscriber never reveives its copy.
// Inactive subscribers (expired by cancelling their context) are skipped (their copies are dropped).
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

// pubOnce is like pub, but delivers the message only once. The tag of the subscriber that got the message is returned.
func (ps *pubsub) pubOnce(m any, tags ...tag) tag {
	received := ps.pubN(m, 1, tags...)
	if len(received) == 0 {
		return ""
	}
	return received[0]
}

// pubN is like pub, but delivers the message to no more than n subscribers. The tags of the subscribers that got the message are returned.
func (ps *pubsub) pubN(m any, n int, tags ...tag) []tag {
	if log.V(3) {
		startTime := time.Now()
		defer func() {
			log.Infof("[casng] pubsub.duration: start=%d, end=%d", startTime.UnixNano(), time.Now().UnixNano())
		}()
	}
	if len(tags) == 0 {
		log.Warning("[casng] pubsub.pub: called without tags")
		log.V(4).Infof("[casng] pubsub.pub: called without tags: msg=%v", m)
	}
	if n <= 0 {
		log.Warningf("[casng] pubsub.pub: nothing published because n=%d", n)
		return nil
	}
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	log.V(4).Infof("[casng] pubsub.pub.msg: type=%[1]T, value=%[1]v", m)
	toRetry := make([]tag, 0, len(tags))
	var received []tag
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	attemptCount := 1
	for {
		for _, t := range tags {
			subscriber, ok := ps.subs[t]
			if !ok {
				log.V(3).Infof("[casng] pubsub.pub.drop: tag=%s", t)
				continue
			}
			// Send now or reschedule if the subscriber is not ready.
			select {
			case subscriber <- m:
				log.V(3).Infof("[casng] pubsub.pub.send: tag=%s", t)
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
		attemptCount++
		log.V(3).Infof("[casng] pubsub.retry: attempts=%d, tags=%d", attemptCount, len(toRetry))
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

func (ps *pubsub) len() int {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	return len(ps.subs)
}

func newPubSub() *pubsub {
	return &pubsub{subs: make(map[tag]chan any)}
}

func excludeTag(tags []tag, et tag) []tag {
	if len(tags) == 0 {
		return []tag{}
	}
	ts := make([]tag, 0, len(tags)-1)
	// Only exclude the tag once.
	excluded := false
	for _, t := range tags {
		if !excluded && t == et {
			excluded = true
			continue
		}
		ts = append(ts, t)
	}
	return ts
}
