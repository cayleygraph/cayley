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

// cache implements an LRU cache.
type cache struct {
	cache   map[string]*kv
	head    *kv
	tail    *kv
	maxSize int
}

type kv struct {
	key   string
	value interface{}
	next  *kv
	prev  *kv
}

func newCache(size int) *cache {
	return &cache{
		maxSize: size,
		cache:   make(map[string]*kv),
	}
}

func (lru *cache) Put(key string, value interface{}) {
	if element, ok := lru.cache[key]; ok {
		element.value = value
		lru.moveToFront(element)
		return
	}
	if len(lru.cache) == lru.maxSize {
		lru.removeOldest()
	}
	newItem := &kv{key: key, value: value}
	lru.cache[key] = newItem
	if lru.head == nil {
		lru.head = newItem
		lru.tail = newItem
	} else {
		newItem.next = lru.head
		lru.head.prev = newItem
		lru.head = newItem
	}
}

func (lru *cache) Get(key string) (interface{}, bool) {
	if element, ok := lru.cache[key]; ok {
		lru.moveToFront(element)
		return element.value, true
	}
	return nil, false
}

func (lru *cache) removeOldest() {
	last := lru.tail
	if lru.head == last {
		lru.tail = nil
		lru.head = nil
	} else {
		lru.tail = last.prev
		if lru.tail != nil {
			lru.tail.next = nil
		}
	}
	last.next = nil
	last.prev = nil
	delete(lru.cache, last.key)
}

func (lru *cache) moveToFront(element *kv) {
	if element.next != nil {
		element.next.prev = element.prev
	}
	if element.prev != nil {
		element.prev.next = element.next
	}
	element.next = lru.head
	element.prev = nil
	lru.head.prev = element
	lru.head = element
}

