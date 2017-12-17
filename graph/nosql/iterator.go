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

package nosql

import (
	"context"
	"fmt"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/quad"
)

type Iterator struct {
	uid        uint64
	tags       graph.Tagger
	qs         *QuadStore
	dir        quad.Direction
	iter       DocIterator
	hash       NodeHash
	size       int64
	isAll      bool
	constraint []FieldFilter
	collection string
	result     graph.Value
	err        error
}

func NewIterator(qs *QuadStore, collection string, d quad.Direction, val graph.Value) *Iterator {
	h := val.(NodeHash)

	constraints := []FieldFilter{{
		Path:   []string{d.String()},
		Filter: Equal,
		Value:  String(h),
	}}

	return &Iterator{
		uid:        iterator.NextUID(),
		constraint: constraints,
		collection: collection,
		qs:         qs,
		dir:        d,
		iter:       nil,
		size:       -1,
		hash:       h,
		isAll:      false,
	}
}

func (it *Iterator) makeIterator() DocIterator {
	q := it.qs.db.Query(it.collection)
	if !it.isAll {
		q = q.WithFields(it.constraint...)
	}
	return q.Iterate()
}

func NewAllIterator(qs *QuadStore, collection string) *Iterator {
	return &Iterator{
		uid:        iterator.NextUID(),
		qs:         qs,
		dir:        quad.Any,
		constraint: nil,
		collection: collection,
		iter:       nil,
		size:       -1,
		hash:       "",
		isAll:      true,
	}
}

func NewIteratorWithConstraints(qs *QuadStore, collection string, constraints []FieldFilter) *Iterator {
	return &Iterator{
		uid:        iterator.NextUID(),
		qs:         qs,
		dir:        quad.Any,
		constraint: constraints,
		collection: collection,
		iter:       nil,
		size:       -1,
		hash:       "",
		isAll:      false,
	}
}

func (it *Iterator) UID() uint64 {
	return it.uid
}

func (it *Iterator) Reset() {
	it.Close()
	it.iter = it.makeIterator()

}

func (it *Iterator) Close() error {
	if it.iter != nil {
		return it.iter.Close()
	}
	return nil
}

func (it *Iterator) Tagger() *graph.Tagger {
	return &it.tags
}

func (it *Iterator) TagResults(dst map[string]graph.Value) {
	it.tags.TagResult(dst, it.Result())
}

func (it *Iterator) Clone() graph.Iterator {
	var m *Iterator
	if it.isAll {
		m = NewAllIterator(it.qs, it.collection)
	} else {
		m = NewIterator(it.qs, it.collection, it.dir, NodeHash(it.hash))
	}
	m.tags.CopyFrom(it)
	return m
}

func (it *Iterator) Next() bool {
	if it.iter == nil {
		it.iter = it.makeIterator()
	}
	if !it.iter.Next(context.TODO()) {
		err := it.iter.Err()
		if err != nil {
			it.err = err
			clog.Errorf("Error Nexting Iterator: %v", err)
		}
		return false
	}
	doc := it.iter.Doc()
	if it.collection == colQuads && !checkQuadValid(doc) {
		return it.Next()
	}
	if it.collection == colQuads {
		sh, _ := doc[fldSubject].(String)
		ph, _ := doc[fldPredicate].(String)
		oh, _ := doc[fldObject].(String)
		lh, _ := doc[fldLabel].(String)
		it.result = QuadHash{
			string(sh), string(ph), string(oh), string(lh),
		}
	} else {
		id, _ := doc[fldHash].(String)
		it.result = NodeHash(id)
	}
	return true
}

func (it *Iterator) Err() error {
	return it.err
}

func (it *Iterator) Result() graph.Value {
	return it.result
}

func (it *Iterator) NextPath() bool {
	return false
}

// SubIterators returns no subiterators for a Mongo iterator.
func (it *Iterator) SubIterators() []graph.Iterator {
	return nil
}

func (it *Iterator) Contains(v graph.Value) bool {
	graph.ContainsLogIn(it, v)
	if it.isAll {
		it.result = v
		return graph.ContainsLogOut(it, v, true)
	}
	val := NodeHash(v.(QuadHash).Get(it.dir))
	if val == it.hash {
		it.result = v
		return graph.ContainsLogOut(it, v, true)
	}
	return graph.ContainsLogOut(it, v, false)
}

func (it *Iterator) Size() (int64, bool) {
	if it.size == -1 {
		var err error
		it.size, err = it.qs.getSize(it.collection, it.constraint)
		if err != nil {
			it.err = err
		}
	}
	return it.size, true
}

func (it *Iterator) Type() graph.Type {
	if it.isAll {
		return graph.All
	}
	return "nosql"
}

func (it *Iterator) Sorted() bool                     { return true }
func (it *Iterator) Optimize() (graph.Iterator, bool) { return it, false }

func (it *Iterator) String() string {
	return fmt.Sprintf("NoSQL(%v)", it.collection)
}

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
