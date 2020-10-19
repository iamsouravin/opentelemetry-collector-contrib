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
	"testing"
)

func TestRemoteGetOrSetNewMapping(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	cache, err := createRemoteCache(DefaultTTL)
	if err != nil {
		t.Errorf("TestRemoteGetOrSetNewMapping() got err: %q", err)
	}
	defer cache.Delete("key1")
	defer cache.Stop()

	want := "val1"

	cache.Delete("key1")
	if got, _ := cache.GetOrSet("key1", "val1"); got != want {
		t.Errorf("TestRemoteGetOrSetNewMapping() = %q, want %q", got, want)
	}
}

func TestRemoteGetOrSetExistingMapping(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	cache, err := createRemoteCache(DefaultTTL)
	if err != nil {
		t.Errorf("TestRemoteGetOrSetExistingMapping() got err: %q", err)
	}
	defer cache.Delete("key1")
	defer cache.Stop()

	want := "val1"

	cache.Delete("key1")
	cache.GetOrSet("key1", want)
	if got, _ := cache.GetOrSet("key1", "val2"); got != want {
		t.Errorf("TestRemoteGetOrSetExistingMapping() = %q, want %q", got, want)
	}
}

func TestRemoteGetOrSetDeleteAndGetOrSetMapping(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	cache, err := createRemoteCache(DefaultTTL)
	if err != nil {
		t.Errorf("TestRemoteGetOrSetDeleteAndGetOrSetMapping() got err: %q", err)
	}
	defer cache.Delete("key1")
	defer cache.Stop()

	want := "val2"

	cache.GetOrSet("key1", "val1")
	cache.Delete("key1")
	if got, _ := cache.GetOrSet("key1", want); got != want {
		t.Errorf("TestRemoteGetOrSetDeleteAndGetOrSetMapping() = %q, want %q", got, want)
	}
}

func TestRemoteDefaultTTL(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	cache, err := createRemoteCache(0)
	if err != nil {
		t.Errorf("TestRemoteDefaultTTL() got err: %q", err)
	}
	defer cache.Stop()

	want := DefaultTTL

	if got := cache.TTL(); got != want {
		t.Errorf("TestRemoteDefaultTTL() TTL = %d, want %d", got, want)
	}
}

func createRemoteCache(ttl int) (*RemoteCache, error) {
	cache, err := NewRemoteCache("redis://localhost:6379", ttl)
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
