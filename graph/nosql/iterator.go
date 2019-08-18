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

	"github.com/hidal-go/hidalgo/legacy/nosql"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/quad"
)

var _ graph.Iterator = (*Iterator)(nil)

type Linkage struct {
	Dir quad.Direction
	Val NodeHash
}

type Iterator struct {
	qs         *QuadStore
	collection string
	limit      int64
	constraint []nosql.FieldFilter
	links      []Linkage // used in Contains

	iter   nosql.DocIterator
	result graph.Ref
	size   int64
	err    error
}

func linkageToFilters(links []Linkage) []nosql.FieldFilter {
	filters := make([]nosql.FieldFilter, 0, len(links))
	for _, l := range links {
		filters = append(filters, nosql.FieldFilter{
			Path:   []string{l.Dir.String()},
			Filter: nosql.Equal,
			Value:  nosql.String(l.Val),
		})
	}
	return filters
}

func NewLinksToIterator(qs *QuadStore, collection string, links []Linkage) *Iterator {
	filters := linkageToFilters(links)
	it := NewIterator(qs, collection, filters...)
	it.links = links
	return it
}

func (it *Iterator) makeIterator() nosql.DocIterator {
	q := it.qs.db.Query(it.collection)
	if len(it.constraint) != 0 {
		q = q.WithFields(it.constraint...)
	}
	if it.limit > 0 {
		q = q.Limit(int(it.limit))
	}
	return q.Iterate()
}

func NewIterator(qs *QuadStore, collection string, constraints ...nosql.FieldFilter) *Iterator {
	return &Iterator{
		qs:         qs,
		constraint: constraints,
		collection: collection,
		size:       -1,
	}
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

func (it *Iterator) TagResults(dst map[string]graph.Ref) {}

func (it *Iterator) Next(ctx context.Context) bool {
	if it.iter == nil {
		it.iter = it.makeIterator()
	}
	var doc nosql.Document
	for {
		if !it.iter.Next(ctx) {
			if err := it.iter.Err(); err != nil {
				it.err = err
				clog.Errorf("error nexting iterator: %v", err)
			}
			return false
		}
		doc = it.iter.Doc()
		if it.collection == colQuads && !checkQuadValid(doc) {
			continue
		}
		break
	}
	if it.collection == colQuads {
		sh, _ := doc[fldSubject].(nosql.String)
		ph, _ := doc[fldPredicate].(nosql.String)
		oh, _ := doc[fldObject].(nosql.String)
		lh, _ := doc[fldLabel].(nosql.String)
		it.result = QuadHash{
			string(sh), string(ph), string(oh), string(lh),
		}
	} else {
		id, _ := doc[fldHash].(nosql.String)
		it.result = NodeHash(id)
	}
	return true
}

func (it *Iterator) Err() error {
	return it.err
}

func (it *Iterator) Result() graph.Ref {
	return it.result
}

func (it *Iterator) NextPath(ctx context.Context) bool {
	return false
}

func (it *Iterator) SubIterators() []graph.Iterator {
	return nil
}

func (it *Iterator) Contains(ctx context.Context, v graph.Ref) bool {
	if len(it.links) != 0 {
		qh := v.(QuadHash)
		for _, l := range it.links {
			if l.Val != NodeHash(qh.Get(l.Dir)) {
				return false
			}
		}
		it.result = v
		return true
	}
	if len(it.constraint) == 0 {
		it.result = v
		return true
	}
	qv := it.qs.NameOf(v)
	if qv == nil {
		return false
	}
	d := toDocumentValue(&it.qs.opt, qv)
	for _, f := range it.constraint {
		if !f.Matches(d) {
			return false
		}
	}
	it.result = v
	return true
}

func (it *Iterator) Size() (int64, bool) {
	if it.size == -1 {
		var err error
		it.size, err = it.qs.getSize(it.collection, it.constraint)
		if err != nil {
			it.err = err
		}
	}
	if it.limit > 0 && it.size > it.limit {
		it.size = it.limit
	}
	if it.size < 0 {
		return it.qs.Size(), false
	}
	return it.size, true
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
