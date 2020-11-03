// Copyright 2020 OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cache

import (
	"sync"
	"time"

	gocache "github.com/patrickmn/go-cache"
)

// LocalCache uses an in-memory map for local testing
type LocalCache struct {
	c        *gocache.Cache
	endpoint string
	ttl      int
	mutex    *sync.Mutex
}

// NewLocalCache returns a new LocalCache
func NewLocalCache(cacheEndpoint string, ttl int) (*LocalCache, error) {
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	lc := &LocalCache{
		c:        gocache.New(time.Duration(ttl)*time.Second, 10*time.Minute),
		endpoint: cacheEndpoint,
		ttl:      ttl,
		mutex:    &sync.Mutex{},
	}
	return lc, nil
}

// Start implements Start method. This is a No Op.
func (lc *LocalCache) Start() error {
	return nil
}

// Stop implements Stop method. Clears the cache.
func (lc *LocalCache) Stop() error {
	lc.c.Flush()
	return nil
}

// GetOrSet returns stored value of key if exists, otherwise
// creates new mapping and returns value
func (lc *LocalCache) GetOrSet(key string, value string) (string, error) {
	lc.mutex.Lock()
	defer lc.mutex.Unlock()
	stored, found := lc.c.Get(key)
	if found {
		return stored.(string), nil
	}
	lc.c.SetDefault(key, value)
	return value, nil
}

// Delete removes the mapping for key
func (lc *LocalCache) Delete(key string) {
	lc.mutex.Lock()
	defer lc.mutex.Unlock()
	lc.c.Delete(key)
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
