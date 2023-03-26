package cas

import "sync"

type sliceCache struct {
	store map[interface{}][]interface{}
	mu    sync.RWMutex
}

func (c *sliceCache) Load(key interface{}) []interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.store[key]
}

func (c *sliceCache) Store(key interface{}, val []interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.store[key] = val
}

func (c *sliceCache) Append(key interface{}, val interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.store[key] = append(c.store[key], val)
}

// initSliceCache returns a properly initialized struct (not a pointer, hence, init rather than new).
func initSliceCache() sliceCache {
	return sliceCache{store: make(map[interface{}][]interface{})}
}
