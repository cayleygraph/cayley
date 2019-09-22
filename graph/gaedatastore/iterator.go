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

package gaedatastore

import (
	"context"
	"fmt"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/quad"

	"google.golang.org/appengine/datastore"
)

type Iterator struct {
	size   int64
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
	result graph.Ref
	err    error
}

var (
	bufferSize = 50
)

func NewIterator(qs *QuadStore, k string, d quad.Direction, val graph.Ref) *Iterator {
	t := val.(*Token)
	if t == nil {
		clog.Errorf("Token == nil")
	}
	if t.Kind != nodeKind {
		clog.Errorf("Cannot create an iterator from a non-node value")
		return &Iterator{done: true}
	}
	if k != nodeKind && k != quadKind {
		clog.Errorf("Cannot create iterator for unknown kind")
		return &Iterator{done: true}
	}
	if qs.context == nil {
		clog.Errorf("Cannot create iterator without a valid context")
		return &Iterator{done: true}
	}
	name := quad.StringOf(qs.NameOf(t))

	// The number of references to this node is held in the nodes entity
	key := qs.createKeyFromToken(t)
	foundNode := new(NodeEntry)
	err := datastore.Get(qs.context, key, foundNode)
	if err != nil && err != datastore.ErrNoSuchEntity {
		clog.Errorf("Error: %v", err)
		return &Iterator{done: true}
	}
	size := foundNode.Size

	return &Iterator{
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
		clog.Errorf("Cannot create iterator for an unknown kind")
		return &Iterator{done: true}
	}
	if qs.context == nil {
		clog.Errorf("Cannot create iterator without a valid context")
		return &Iterator{done: true}
	}

	st, err := qs.Stats(context.Background(), true)
	var size int64
	if kind == nodeKind {
		size = st.Nodes.Size
	} else {
		size = st.Quads.Size
	}
	return &Iterator{
		qs:    qs,
		size:  size,
		err:   err,
		dir:   quad.Any,
		isAll: true,
		kind:  kind,
		done:  err != nil,
	}
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

func (it *Iterator) Contains(ctx context.Context, v graph.Ref) bool {
	if it.isAll {
		// The result needs to be set, so when contains is called, the result can be retrieved
		it.result = v
		return true
	}
	t := v.(*Token)
	if t == nil {
		clog.Errorf("Could not cast to token")
		return false
	}
	if t.Kind == nodeKind {
		clog.Errorf("Contains does not work with node values")
		return false
	}
	// Contains is for when you want to know that an iterator refers to a quad
	var offset int
	switch it.dir {
	case quad.Subject:
		offset = 0
	case quad.Predicate:
		offset = (quad.HashSize * 2)
	case quad.Object:
		offset = (quad.HashSize * 2) * 2
	case quad.Label:
		offset = (quad.HashSize * 2) * 3
	}
	val := t.Hash[offset : offset+(quad.HashSize*2)]
	if val == it.hash {
		return true
	}
	return false
}

func (it *Iterator) TagResults(dst map[string]graph.Ref) {}

func (it *Iterator) NextPath(ctx context.Context) bool {
	return false
}

// No subiterators.
func (it *Iterator) SubIterators() []graph.Iterator {
	return nil
}

func (it *Iterator) Result() graph.Ref {
	return it.result
}

func (it *Iterator) Next(ctx context.Context) bool {
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
			clog.Errorf("Error fetching next entry %v", err)
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
		clog.Warningf("Query did not return any results")
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

func (it *Iterator) Sorted() bool                     { return false }
func (it *Iterator) Optimize() (graph.Iterator, bool) { return it, false }
func (it *Iterator) String() string {
	return fmt.Sprintf("GAE(%s/%s)", it.name, it.hash)
}

// TODO (panamafrancis) calculate costs
func (it *Iterator) Stats() graph.IteratorStats {
	size, exact := it.Size()
	return graph.IteratorStats{
		ContainsCost: 1,
		NextCost:     5,
		Size:         size,
		ExactSize:    exact,
	}
}

var _ graph.Iterator = &Iterator{}
