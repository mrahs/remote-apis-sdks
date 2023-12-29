package casng

import (
	"sync"

	"google.golang.org/protobuf/proto"
)

// nodeSliceMap is a mutex-guarded map that supports a synchronized append operation.
type nodeSliceMap struct {
	store map[string][]proto.Message
	mu    sync.RWMutex
}

// load returns the slice associated with the given key or nil.
func (nsm *nodeSliceMap) load(key string) []proto.Message {
	nsm.mu.RLock()
	defer nsm.mu.RUnlock()
	return nsm.store[key]
}

// append appends the specified value to the slice associated with the specified key.
func (nsm *nodeSliceMap) append(key string, m proto.Message) {
	nsm.mu.Lock()
	defer nsm.mu.Unlock()
	nsm.store[key] = append(nsm.store[key], m)
}

// len returns the number of entries in the map.
func (nsm *nodeSliceMap) len() int {
	nsm.mu.RLock()
	defer nsm.mu.RUnlock()
	return len(nsm.store)
}
