package cache

import (
	"fmt"
	"reflect"
	"testing"
)

func TestNewLocalCache(t *testing.T) {
	cacheProvider := NewProvider()
	want := "NewLocalCache"

	cache, err := cacheProvider("local://", 0)
	if err != nil {
		t.Error(err)
	}

	_, ok := cache.(*LocalCache)
	if !ok {
		t.Errorf("TestNewLocalCache() = %q, want %q", reflect.TypeOf(cache).Name(), want)
	}
}

func TestNewRemoteCache(t *testing.T) {
	cacheProvider := NewProvider()
	want := "RemoteCache"

	cache, err := cacheProvider("redis://localhost:6379", 86400)
	if err != nil {
		t.Error(err)
	}

	_, ok := cache.(*RemoteCache)
	if !ok {
		t.Errorf("TestNewRemoteCache() = %q, want %q", reflect.TypeOf(cache).Name(), want)
	}
}

func TestUnknownCacheScheme(t *testing.T) {
	cacheProvider := NewProvider()
	want := fmt.Errorf("Unknown cache provider scheme: %q", "memcached")

	_, err := cacheProvider("memcached://localhost:6379", 10)
	if err == nil {
		t.Errorf("TestUnknownCacheScheme() = %q, %q", "nil", want)
	} else if err.Error() != want.Error() {
		t.Errorf("TestUnknownCacheScheme() = %q, %q", err, want)
	}
}
