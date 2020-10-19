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
