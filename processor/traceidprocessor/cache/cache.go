package cache

import (
	"fmt"
	"net/url"
)

// DefaultTTL is the default cache TTL for remote cache
const DefaultTTL = 7 * 24 * 60 * 60

// Cache interface to be implemented by cache implementations
type Cache interface {
	GetOrSet(key string, value string) (string, error)
	Delete(key string)
	Health() func() error
	Start() error
	Stop() error
	TTL() int
}

// Provider is a factory for cache implementations
type Provider func(string, int) (Cache, error)

// NewProvider create a new Provider
func NewProvider() Provider {
	return func(cacheEndpoint string, ttl int) (Cache, error) {
		u, err := url.Parse(cacheEndpoint)
		if err != nil {
			return nil, err
		}
		switch u.Scheme {
		case "redis":
			return NewRemoteCache(cacheEndpoint, ttl)
		case "local":
			return NewLocalCache(cacheEndpoint, ttl)
		default:
			return nil, fmt.Errorf("Unknown cache provider scheme: %q", u.Scheme)
		}
	}
}
