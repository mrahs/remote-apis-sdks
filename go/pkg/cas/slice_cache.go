package cas

import "sync"

// sliceCache is a mutex-guarded map that supports an atomic append operation.
type sliceCache struct {
	store map[interface{}][]interface{}
	mu    sync.RWMutex
}

// Load returns the slice associated with the given key or nil.
func (c *sliceCache) Load(key interface{}) []interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.store[key]
}

// Append appends the specified value to the slice associated with the specified key.
// If the key does not exist, a new empty slice is created and the value is appended to it.
func (c *sliceCache) Append(key interface{}, val interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.store[key] = append(c.store[key], val)
}

// initSliceCache returns a properly initialized struct (not a pointer, hence, init rather than new).
func initSliceCache() sliceCache {
	return sliceCache{store: make(map[interface{}][]interface{})}
}
