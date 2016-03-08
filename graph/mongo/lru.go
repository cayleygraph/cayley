// Copyright 2014 The Cayley Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mongo

import (
	"container/list"
	"sync"
)

// TODO(kortschak) Reimplement without container/list.

// cache implements an LRU cache.
type cache struct {
	sync.RWMutex
	cache    map[string]*list.Element
	priority *list.List
	maxSize  int
}

type kv struct {
	key   string
	value interface{}
}

func newCache(size int) *cache {
	var lru cache
	lru.maxSize = size
	lru.priority = list.New()
	lru.cache = make(map[string]*list.Element)
	return &lru
}

func (lru *cache) Put(key string, value interface{}) {
	if _, ok := lru.Get(key); ok {
		return
	}

	lru.Lock()
	defer lru.Unlock()
	if len(lru.cache) == lru.maxSize {
		lru.removeOldest()
	}
	lru.priority.PushFront(kv{key: key, value: value})
	lru.cache[key] = lru.priority.Front()
}

func (lru *cache) Get(key string) (interface{}, bool) {
	lru.RLock()
	defer lru.RUnlock()
	if element, ok := lru.cache[key]; ok {
		lru.priority.MoveToFront(element)
		return element.Value.(kv).value, true
	}

	return nil, false
}

func (lru *cache) removeOldest() {
	last := lru.priority.Remove(lru.priority.Back())
	delete(lru.cache, last.(kv).key)

}
