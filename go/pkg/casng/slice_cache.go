package casng

import (
	"sync"

	"google.golang.org/protobuf/proto"
)

// nodeSliceMap is a mutex-guarded map that supports an atomic append operation.
type nodeSliceMap struct {
	store map[string][]proto.Message
	mu    sync.RWMutex
}

// load returns the slice associated with the given key or nil.
func (c *nodeSliceMap) load(key string) []proto.Message {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.store[key]
}

// append appends the specified value to the slice associated with the specified key.
func (c *nodeSliceMap) append(key string, m proto.Message) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.store[key] = append(c.store[key], m)
}

// initSliceCache returns a properly initialized struct (not a pointer, hence, init rather than new).
func initSliceCache() nodeSliceMap {
	return nodeSliceMap{store: make(map[string][]proto.Message)}
}
