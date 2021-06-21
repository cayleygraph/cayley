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
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/graph/refs"
	"github.com/cayleygraph/quad"

	"google.golang.org/appengine/datastore"
)

var _ iterator.Shape = &Iterator{}

const (
	bufferSize = 50
)

type Iterator struct {
	size  int64
	dir   quad.Direction
	qs    *QuadStore
	t     *Token
	isAll bool
	kind  string
}

func (it *Iterator) Iterate() iterator.Scanner {
	if it.isAll {
		return newAllIteratorNext(it.qs, it.kind)
	}
	return newIteratorNext(it.qs, it.kind, it.dir, it.t)
}

func (it *Iterator) Lookup() iterator.Index {
	if it.isAll {
		return newAllIteratorContains(it.qs, it.kind)
	}
	return newIteratorContains(it.qs, it.kind, it.dir, it.t)
}

func (qs *QuadStore) newIterator(k string, d quad.Direction, val graph.Ref) *Iterator {
	t := val.(*Token)
	if t == nil {
		clog.Errorf("Token == nil")
	}
	return &Iterator{
		dir:   d,
		qs:    qs,
		isAll: false,
		t:     t,
		kind:  k,
	}
}

func (qs *QuadStore) newAllIterator(kind string) *Iterator {
	return &Iterator{
		qs:    qs,
		dir:   quad.Any,
		isAll: true,
		kind:  kind,
	}
}

// No subiterators.
func (it *Iterator) SubIterators() []iterator.Shape {
	return nil
}

func (it *Iterator) Sorted() bool                                        { return false }
func (it *Iterator) Optimize(ctx context.Context) (iterator.Shape, bool) { return it, false }
func (it *Iterator) String() string {
	name := ""
	if it.t != nil {
		tn, err := it.qs.NameOf(it.t)
		if err != nil {
			name = "ERROR(" + err.Error() + ")"
		} else {
			name = quad.StringOf(tn) + "/" + it.t.Hash
		}
	}
	return fmt.Sprintf("GAE(%s)", name)
}

func (it *Iterator) getSize(ctx context.Context) (refs.Size, error) {
	if it.size != 0 {
		return refs.Size{
			Value: it.size,
			Exact: true,
		}, nil
	}
	if !it.isAll {
		// The number of references to this node is held in the nodes entity
		key := it.qs.createKeyFromToken(it.t)
		foundNode := new(NodeEntry)
		err := datastore.Get(it.qs.context, key, foundNode)
		if err != datastore.ErrNoSuchEntity {
			err = nil
		}
		size := foundNode.Size
		it.size = size
		return refs.Size{
			Value: it.size,
			Exact: err == nil,
		}, err
	}
	var size int64
	st, err := it.qs.Stats(context.Background(), true)
	if it.kind == nodeKind {
		size = st.Nodes.Value
	} else {
		size = st.Quads.Value
	}
	it.size = size
	return refs.Size{
		Value: it.size,
		Exact: err == nil,
	}, err
}

func (it *Iterator) Stats(ctx context.Context) (iterator.Costs, error) {
	sz, err := it.getSize(ctx)
	// TODO (panamafrancis) calculate costs
	return iterator.Costs{
		ContainsCost: 1,
		NextCost:     5,
		Size:         sz,
	}, err
}

type iteratorNext struct {
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

func newIteratorNext(qs *QuadStore, k string, d quad.Direction, t *Token) *iteratorNext {
	if t == nil {
		clog.Errorf("Token == nil")
	}
	if t.Kind != nodeKind {
		clog.Errorf("Cannot create an iterator from a non-node value")
		return &iteratorNext{done: true}
	}
	if k != nodeKind && k != quadKind {
		clog.Errorf("Cannot create iterator for unknown kind")
		return &iteratorNext{done: true}
	}
	if qs.context == nil {
		clog.Errorf("Cannot create iterator without a valid context")
		return &iteratorNext{done: true}
	}
	tn, err := qs.NameOf(t)
	if err != nil {
		clog.Errorf("Creating iterator token lookup error: %v", err)
		return &iteratorNext{done: true, err: err}
	}
	name := quad.StringOf(tn)
	return &iteratorNext{
		name:  name,
		dir:   d,
		qs:    qs,
		isAll: false,
		kind:  k,
		hash:  t.Hash,
		done:  false,
	}
}

func newAllIteratorNext(qs *QuadStore, kind string) *iteratorNext {
	if kind != nodeKind && kind != quadKind {
		clog.Errorf("Cannot create iterator for an unknown kind")
		return &iteratorNext{done: true}
	}
	if qs.context == nil {
		clog.Errorf("Cannot create iterator without a valid context")
		return &iteratorNext{done: true}
	}
	return &iteratorNext{
		qs:    qs,
		dir:   quad.Any,
		isAll: true,
		kind:  kind,
	}
}

func (it *iteratorNext) Close() error {
	it.buffer = nil
	it.offset = 0
	it.done = true
	it.last = ""
	it.result = nil
	return nil
}

func (it *iteratorNext) TagResults(dst map[string]graph.Ref) {}

func (it *iteratorNext) NextPath(ctx context.Context) bool {
	return false
}

func (it *iteratorNext) Result() graph.Ref {
	return it.result
}

func (it *iteratorNext) Next(ctx context.Context) bool {
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

func (it *iteratorNext) Err() error {
	return it.err
}

func (it *iteratorNext) Sorted() bool { return false }
func (it *iteratorNext) String() string {
	return fmt.Sprintf("GAE(%s/%s)", it.name, it.hash)
}

type iteratorContains struct {
	dir    quad.Direction
	qs     *QuadStore
	name   string
	isAll  bool
	kind   string
	hash   string
	done   bool
	result graph.Ref
	err    error
}

func newIteratorContains(qs *QuadStore, k string, d quad.Direction, t *Token) *iteratorContains {
	if t == nil {
		clog.Errorf("Token == nil")
	}
	if t.Kind != nodeKind {
		clog.Errorf("Cannot create an iterator from a non-node value")
		return &iteratorContains{done: true}
	}
	if k != nodeKind && k != quadKind {
		clog.Errorf("Cannot create iterator for unknown kind")
		return &iteratorContains{done: true}
	}
	if qs.context == nil {
		clog.Errorf("Cannot create iterator without a valid context")
		return &iteratorContains{done: true}
	}
	tn, err := qs.NameOf(t)
	if err != nil {
		clog.Errorf("Creating iterator token lookup error: %v", err)
		return &iteratorContains{done: true, err: err}
	}
	name := quad.StringOf(tn)
	return &iteratorContains{
		name:  name,
		dir:   d,
		qs:    qs,
		isAll: false,
		kind:  k,
		hash:  t.Hash,
		done:  false,
	}
}

func newAllIteratorContains(qs *QuadStore, kind string) *iteratorContains {
	if kind != nodeKind && kind != quadKind {
		clog.Errorf("Cannot create iterator for an unknown kind")
		return &iteratorContains{done: true}
	}
	if qs.context == nil {
		clog.Errorf("Cannot create iterator without a valid context")
		return &iteratorContains{done: true}
	}
	return &iteratorContains{
		qs:    qs,
		dir:   quad.Any,
		isAll: true,
		kind:  kind,
	}
}

func (it *iteratorContains) Close() error {
	it.done = true
	it.result = nil
	return nil
}

func (it *iteratorContains) Contains(ctx context.Context, v graph.Ref) bool {
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

func (it *iteratorContains) TagResults(dst map[string]graph.Ref) {}

func (it *iteratorContains) NextPath(ctx context.Context) bool {
	return false
}

func (it *iteratorContains) Result() graph.Ref {
	return it.result
}

func (it *iteratorContains) Err() error {
	return it.err
}

func (it *iteratorContains) Sorted() bool { return false }
func (it *iteratorContains) String() string {
	return fmt.Sprintf("GAE(%s/%s)", it.name, it.hash)
}
