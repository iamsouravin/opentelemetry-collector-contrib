package cache

import (
	"testing"
)

func TestLocalGetOrSetNewMapping(t *testing.T) {
	cache, err := createLocalCache(DefaultTTL)
	if err != nil {
		t.Errorf("TestLocalGetOrSetNewMapping() got err: %q", err)
	}
	defer cache.Delete("key1")
	defer cache.Stop()

	want := "val1"

	if got, _ := cache.GetOrSet("key1", "val1"); got != want {
		t.Errorf("TestLocalGetOrSetNewMapping() = %q, want %q", got, want)
	}
}

func TestLocalGetOrSetExistingMapping(t *testing.T) {
	cache, err := createLocalCache(DefaultTTL)
	if err != nil {
		t.Errorf("TestLocalGetOrSetExistingMapping() got err: %q", err)
	}
	defer cache.Delete("key1")
	defer cache.Stop()

	want := "val1"

	cache.GetOrSet("key1", "val1")
	if got, _ := cache.GetOrSet("key1", "val2"); got != want {
		t.Errorf("TestLocalGetOrSetExistingMapping() = %q, want %q", got, want)
	}
}

func TestLocalGetOrSetDeleteAndGetOrSetMapping(t *testing.T) {
	cache, err := createLocalCache(DefaultTTL)
	if err != nil {
		t.Errorf("TestLocalGetOrSetDeleteAndGetOrSetMapping() got err: %q", err)
	}
	defer cache.Delete("key1")
	defer cache.Stop()

	want := "val2"

	cache.GetOrSet("key1", "val1")
	cache.Delete("key1")
	if got, _ := cache.GetOrSet("key1", "val2"); got != want {
		t.Errorf("TestLocalGetOrSetDeleteAndGetOrSetMapping() = %q, want %q", got, want)
	}
}

func TestLocalDefaultTTL(t *testing.T) {
	cache, err := createLocalCache(0)
	if err != nil {
		t.Errorf("TestLocalDefaultTTL() got err: %q", err)
	}
	defer cache.Stop()

	want := DefaultTTL

	if got := cache.TTL(); got != want {
		t.Errorf("TestLocalDefaultTTL() TTL = %d, want %d", got, want)
	}
}

func createLocalCache(ttl int) (*LocalCache, error) {
	cache, err := NewLocalCache("local://", ttl)
	if err != nil {
		return nil, err
	}
	err = cache.Start()
	if err != nil {
		return nil, err
	}
	err = cache.Health()()
	if err != nil {
		return nil, err
	}
	return cache, nil
}
