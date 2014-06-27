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
)

type IDLru struct {
	cache    map[string]*list.Element
	priority *list.List
	maxSize  int
}

type KV struct {
	key   string
	value string
}

func NewIDLru(size int) *IDLru {
	var lru IDLru
	lru.maxSize = size
	lru.priority = list.New()
	lru.cache = make(map[string]*list.Element)
	return &lru
}

func (lru *IDLru) Put(key string, value string) {
	if _, ok := lru.Get(key); ok {
		return
	}
	if len(lru.cache) == lru.maxSize {
		lru.removeOldest()
	}
	lru.priority.PushFront(KV{key: key, value: value})
	lru.cache[key] = lru.priority.Front()
}

func (lru *IDLru) Get(key string) (string, bool) {
	if element, ok := lru.cache[key]; ok {
		lru.priority.MoveToFront(element)
		return element.Value.(KV).value, true
	}
	return "", false
}

func (lru *IDLru) removeOldest() {
	last := lru.priority.Remove(lru.priority.Back())
	delete(lru.cache, last.(KV).key)
}
