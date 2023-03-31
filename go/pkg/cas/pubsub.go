package cas

import (
	"context"
	"sync"

	"github.com/pborman/uuid"
)

type tag string

type pubSub struct {
	subs map[tag]chan any
	mu   sync.Mutex
	wg   sync.WaitGroup
}

// subscribe returns a new channel to the subscriber to read messages from.
//
// Only requests associated with the returned tag are sent on the returned channel.
//
// The returned channel is closed when the specified context is done. The subscriber should
// ensure the context is canceled at the right time to avoid send-on-closed-channel errors
// and avoid deadlocks.
//
// The subscriber must continue to drain the returned channel until it is closed to avoid deadlocks.
func (ps *pubSub) subscribe(ctx context.Context) (tag, <-chan any) {
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

// publish fans-out a response to all subscribers.
func (ps *pubSub) publish(r any, ts ...tag) {
	// Serialize this block to avoid concurrent map-read-write and send-on-closed-channel errors.
	ps.mu.Lock()
	defer ps.mu.Unlock()
	for _, t := range ts {
		subscriber, ok := ps.subs[t]
		if ok {
			// Possible deadlock if the receiver had abandoned the channel.
			subscriber <- r
		}
	}
}

func (ps *pubSub) wait() {
	ps.wg.Wait()
}

func newPubSub() *pubSub {
	return &pubSub{subs: make(map[tag]chan any)}
}
