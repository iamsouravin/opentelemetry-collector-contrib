package cache

import (
	"context"
	"crypto/sha1"
	"fmt"
	"io"
	"strings"

	"github.com/go-redis/redis/v8"
)

const (
	scriptTemplate = `
local val = redis.call("get", KEYS[1])
if (val == false) then
  redis.call('setex', KEYS[1], %d, ARGV[1])
  return ARGV[1]
end
return val
`
)

// RemoteCache uses redis backend
type RemoteCache struct {
	context    context.Context
	endpoint   string
	ttl        int
	client     *redis.Client
	content    string
	contentSha string
}

// NewRemoteCache returns a new RemoteCache.
// Loads lua script and calculates script SHA1.
func NewRemoteCache(cacheEndpoint string, ttl int) (*RemoteCache, error) {
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	rc := &RemoteCache{
		endpoint: cacheEndpoint,
		ttl:      ttl,
	}

	content := fmt.Sprintf(scriptTemplate, ttl)
	rc.content = string(content)
	h := sha1.New()
	io.WriteString(h, rc.content)
	rc.contentSha = fmt.Sprintf("%x", h.Sum(nil))

	rc.context = context.Background()

	return rc, nil
}

// Start starts the redis client
func (rc *RemoteCache) Start() error {
	opt, err := redis.ParseURL(rc.endpoint)
	if err != nil {
		return err
	}
	client := redis.NewClient(opt)
	rc.client = client

	return nil
}

// Stop stops the redis client
func (rc *RemoteCache) Stop() error {
	return rc.client.Close()
}

// GetOrSet returns stored value of key if exists, otherwise
// creates new mapping and returns value.
// Tries `evalsha` first. On error calls `eval`.
func (rc *RemoteCache) GetOrSet(key string, newValue string) (string, error) {
	stored, err := rc.client.EvalSha(rc.context, rc.contentSha, []string{key}, newValue).Text()
	if err != nil {
		s := err.Error()
		if strings.HasPrefix(s, "NOSCRIPT ") {
			stored, err := rc.client.Eval(rc.context, rc.content, []string{key}, newValue).Text()
			if err != nil {
				return "", err
			}
			return stored, nil
		}
		return "", err
	}
	return stored, nil
}

// Delete removes the mapping for key
func (rc *RemoteCache) Delete(key string) {
	rc.client.Del(rc.context, key)
}

// Health checks the health of the cache
func (rc *RemoteCache) Health() func() error {
	return func() error {
		if _, err := rc.client.Ping(rc.context).Result(); err != nil {
			return fmt.Errorf("[redis.Ping]: %v", err)
		}
		return nil
	}
}

// TTL returns cache ttl
func (rc *RemoteCache) TTL() int {
	return rc.ttl
}
