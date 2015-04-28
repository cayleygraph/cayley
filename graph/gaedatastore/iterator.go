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

// +build appengine

package gaedatastore

import (
	"fmt"
	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/quad"

	"appengine/datastore"
	"github.com/barakmich/glog"
)

type Iterator struct {
	uid    uint64
	size   int64
	tags   graph.Tagger
	dir    quad.Direction
	qs     *QuadStore
	name   string
	isAll  bool
	kind   string
	hash   string
	done   bool
	buffer []string
	offset int
	last   string
	result graph.Value
	err    error
}

var (
	bufferSize = 50
)

func NewIterator(qs *QuadStore, k string, d quad.Direction, val graph.Value) *Iterator {
	t := val.(*Token)
	if t == nil {
		glog.Error("Token == nil")
	}
	if t.Kind != nodeKind {
		glog.Error("Cannot create an iterator from a non-node value")
		return &Iterator{done: true}
	}
	if k != nodeKind && k != quadKind {
		glog.Error("Cannot create iterator for unknown kind")
		return &Iterator{done: true}
	}
	if qs.context == nil {
		glog.Error("Cannot create iterator without a valid context")
		return &Iterator{done: true}
	}
	name := qs.NameOf(t)

	// The number of references to this node is held in the nodes entity
	key := qs.createKeyFromToken(t)
	foundNode := new(NodeEntry)
	err := datastore.Get(qs.context, key, foundNode)
	if err != nil && err != datastore.ErrNoSuchEntity {
		glog.Errorf("Error: %v", err)
		return &Iterator{done: true}
	}
	size := foundNode.Size

	return &Iterator{
		uid:   iterator.NextUID(),
		name:  name,
		dir:   d,
		qs:    qs,
		size:  size,
		isAll: false,
		kind:  k,
		hash:  t.Hash,
		done:  false,
	}
}

func NewAllIterator(qs *QuadStore, kind string) *Iterator {
	if kind != nodeKind && kind != quadKind {
		glog.Error("Cannot create iterator for an unknown kind")
		return &Iterator{done: true}
	}
	if qs.context == nil {
		glog.Error("Cannot create iterator without a valid context")
		return &Iterator{done: true}
	}

	var size int64
	if kind == nodeKind {
		size = qs.NodeSize()
	} else {
		size = qs.Size()
	}

	return &Iterator{
		uid:   iterator.NextUID(),
		qs:    qs,
		size:  size,
		dir:   quad.Any,
		isAll: true,
		kind:  kind,
		done:  false,
	}
}

func (it *Iterator) UID() uint64 {
	return it.uid
}

func (it *Iterator) Reset() {
	it.buffer = nil
	it.offset = 0
	it.done = false
	it.last = ""
	it.result = nil
}

func (it *Iterator) Close() error {
	it.buffer = nil
	it.offset = 0
	it.done = true
	it.last = ""
	it.result = nil
	return nil
}

func (it *Iterator) Tagger() *graph.Tagger {
	return &it.tags
}
func (it *Iterator) Contains(v graph.Value) bool {
	graph.ContainsLogIn(it, v)
	if it.isAll {
		// The result needs to be set, so when contains is called, the result can be retrieved
		it.result = v
		return graph.ContainsLogOut(it, v, true)
	}
	t := v.(*Token)
	if t == nil {
		glog.Error("Could not cast to token")
		return graph.ContainsLogOut(it, v, false)
	}
	if t.Kind == nodeKind {
		glog.Error("Contains does not work with node values")
		return graph.ContainsLogOut(it, v, false)
	}
	// Contains is for when you want to know that an iterator refers to a quad
	var offset int
	switch it.dir {
	case quad.Subject:
		offset = 0
	case quad.Predicate:
		offset = (hashSize * 2)
	case quad.Object:
		offset = (hashSize * 2) * 2
	case quad.Label:
		offset = (hashSize * 2) * 3
	}
	val := t.Hash[offset : offset+(hashSize*2)]
	if val == it.hash {
		return graph.ContainsLogOut(it, v, true)
	}
	return graph.ContainsLogOut(it, v, false)
}

func (it *Iterator) TagResults(dst map[string]graph.Value) {
	for _, tag := range it.tags.Tags() {
		dst[tag] = it.Result()
	}
	for tag, value := range it.tags.Fixed() {
		dst[tag] = value
	}
}

func (it *Iterator) Clone() graph.Iterator {
	if it.isAll {
		m := NewAllIterator(it.qs, it.kind)
		m.tags.CopyFrom(it)
		return m
	}

	// Create a token, the tokens kind is ignored in creation of the iterator
	t := &Token{nodeKind, it.hash}
	m := NewIterator(it.qs, it.kind, it.dir, t)
	m.tags.CopyFrom(it)
	return m
}

func (it *Iterator) NextPath() bool {
	return false
}

// No subiterators.
func (it *Iterator) SubIterators() []graph.Iterator {
	return nil
}

func (it *Iterator) Result() graph.Value {
	return it.result
}

func (it *Iterator) Next() bool {
	if it.offset+1 < len(it.buffer) {
		it.offset++
		it.result = &Token{Kind: it.kind, Hash: it.buffer[it.offset]}
		return true
	}
	if it.done {
		return false
	}
	// Reset buffer and offset
	it.offset = 0
	it.buffer = make([]string, 0, bufferSize)
	// Create query
	// TODO (panamafrancis) Keys only query?
	q := datastore.NewQuery(it.kind).Limit(bufferSize)
	if !it.isAll {
		// Filter on the direction {subject,objekt...}
		q = q.Filter(it.dir.String()+" =", it.name)
	}
	// Get last cursor position
	cursor, err := datastore.DecodeCursor(it.last)
	if err == nil {
		q = q.Start(cursor)
	}
	// Buffer the keys of the next 50 matches
	t := q.Run(it.qs.context)
	for {
		// Quirk of the datastore, you cannot pass a nil value to to Next()
		// even if you just want the keys
		var k *datastore.Key
		skip := false
		if it.kind == quadKind {
			temp := new(QuadEntry)
			k, err = t.Next(temp)
			// Skip if quad has been deleted
			if len(temp.Added) <= len(temp.Deleted) {
				skip = true
			}
		} else {
			temp := new(NodeEntry)
			k, err = t.Next(temp)
			// Skip if node has been deleted
			if temp.Size == 0 {
				skip = true
			}
		}
		if err == datastore.Done {
			it.done = true
			break
		}
		if err != nil {
			glog.Errorf("Error fetching next entry %v", err)
			it.err = err
			return false
		}
		if !skip {
			it.buffer = append(it.buffer, k.StringID())
		}
	}
	// Save cursor position
	cursor, err = t.Cursor()
	if err == nil {
		it.last = cursor.String()
	}
	// Protect against bad queries
	if it.done && len(it.buffer) == 0 {
		glog.Warningf("Query did not return any results")
		return false
	}
	// First result
	it.result = &Token{Kind: it.kind, Hash: it.buffer[it.offset]}
	return true
}

func (it *Iterator) Err() error {
	return it.err
}

func (it *Iterator) Size() (int64, bool) {
	return it.size, true
}

var gaedatastoreType graph.Type

func init() {
	gaedatastoreType = graph.RegisterIterator("gaedatastore")
}

func Type() graph.Type                                { return gaedatastoreType }
func (it *Iterator) Type() graph.Type                 { return gaedatastoreType }
func (it *Iterator) Sorted() bool                     { return false }
func (it *Iterator) Optimize() (graph.Iterator, bool) { return it, false }
func (it *Iterator) Describe() graph.Description {
	size, _ := it.Size()
	return graph.Description{
		UID:       it.UID(),
		Name:      fmt.Sprintf("%s/%s", it.name, it.hash),
		Type:      it.Type(),
		Size:      size,
		Tags:      it.tags.Tags(),
		Direction: it.dir,
	}
}

// TODO (panamafrancis) calculate costs
func (it *Iterator) Stats() graph.IteratorStats {
	size, _ := it.Size()
	return graph.IteratorStats{
		ContainsCost: 1,
		NextCost:     5,
		Size:         size,
	}
}

var _ graph.Nexter = &Iterator{}
