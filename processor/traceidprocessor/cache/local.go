package cache

// LocalCache uses an in-memory map for local testing
type LocalCache struct {
	m        map[string]string
	endpoint string
	ttl      int
}

// NewLocalCache returns a new LocalCache
func NewLocalCache(cacheEndpoint string, ttl int) (*LocalCache, error) {
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	lc := &LocalCache{
		m:        make(map[string]string),
		endpoint: cacheEndpoint,
		ttl:      ttl,
	}
	return lc, nil
}

// Start implements Start method. This is a No Op.
func (lc *LocalCache) Start() error {
	lc.m = make(map[string]string)
	return nil
}

// Stop implements Stop method. This is a No Op.
func (lc *LocalCache) Stop() error {
	for k := range lc.m {
		delete(lc.m, k)
	}
	lc.m = nil
	return nil
}

// GetOrSet returns stored value of key if exists, otherwise
// creates new mapping and returns value
func (lc *LocalCache) GetOrSet(key string, value string) (string, error) {
	stored := lc.m[key]
	if stored == "" {
		lc.m[key] = value
		return value, nil
	}
	return stored, nil
}

// Delete removes the mapping for key
func (lc *LocalCache) Delete(key string) {
	delete(lc.m, key)
}

// Health checks the health of the cache
func (lc *LocalCache) Health() func() error {
	return func() error {
		return nil
	}
}

// TTL returns cache ttl
func (lc *LocalCache) TTL() int {
	return lc.ttl
}
