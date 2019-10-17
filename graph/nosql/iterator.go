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
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/graph/refs"
	"github.com/cayleygraph/quad"
)

type Linkage struct {
	Dir quad.Direction
	Val NodeHash
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

type Iterator struct {
	qs         *QuadStore
	collection string
	limit      int64
	constraint []nosql.FieldFilter
	links      []Linkage // used in Contains

	size refs.Size
	err  error
}

func (qs *QuadStore) newLinksToIterator(collection string, links []Linkage) *Iterator {
	filters := linkageToFilters(links)
	it := qs.newIterator(collection, filters...)
	it.links = links
	return it
}

func (qs *QuadStore) newIterator(collection string, constraints ...nosql.FieldFilter) *Iterator {
	return &Iterator{
		qs:         qs,
		constraint: constraints,
		collection: collection,
		size:       refs.Size{Value: -1},
	}
}

func (it *Iterator) Iterate() iterator.Scanner {
	return it.qs.newIteratorNext(it.collection, it.constraint, it.limit)
}

func (it *Iterator) Lookup() iterator.Index {
	return it.qs.newIteratorContains(it.collection, it.constraint, it.links, it.limit)
}

func (it *Iterator) SubIterators() []iterator.Shape {
	return nil
}

func (it *Iterator) getSize(ctx context.Context) (refs.Size, error) {
	if it.size.Value == -1 {
		size, err := it.qs.getSize(it.collection, it.constraint)
		if err != nil {
			it.err = err
		}
		it.size = refs.Size{
			Value: size,
			Exact: true,
		}
	}
	if it.limit > 0 && it.size.Value > it.limit {
		it.size.Value = it.limit
	}
	if it.size.Value < 0 {
		return refs.Size{
			Value: it.qs.Size(),
			Exact: false,
		}, it.err
	}
	return it.size, nil
}

func (it *Iterator) Sorted() bool                                        { return true }
func (it *Iterator) Optimize(ctx context.Context) (iterator.Shape, bool) { return it, false }

func (it *Iterator) String() string {
	return fmt.Sprintf("NoSQL(%v)", it.collection)
}

func (it *Iterator) Stats(ctx context.Context) (iterator.Costs, error) {
	size, err := it.getSize(ctx)
	return iterator.Costs{
		ContainsCost: 1,
		NextCost:     5,
		Size:         size,
	}, err
}

type iteratorNext struct {
	qs         *QuadStore
	collection string
	limit      int64
	constraint []nosql.FieldFilter

	iter   nosql.DocIterator
	result graph.Ref
	err    error
}

func (qs *QuadStore) newIteratorNext(collection string, constraints []nosql.FieldFilter, limit int64) *iteratorNext {
	return &iteratorNext{
		qs:         qs,
		constraint: constraints,
		collection: collection,
		limit:      limit,
	}
}

func (it *iteratorNext) makeIterator() nosql.DocIterator {
	q := it.qs.db.Query(it.collection)
	if len(it.constraint) != 0 {
		q = q.WithFields(it.constraint...)
	}
	if it.limit > 0 {
		q = q.Limit(int(it.limit))
	}
	return q.Iterate()
}

func (it *iteratorNext) Close() error {
	if it.iter != nil {
		return it.iter.Close()
	}
	return nil
}

func (it *iteratorNext) TagResults(dst map[string]graph.Ref) {}

func (it *iteratorNext) Next(ctx context.Context) bool {
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

func (it *iteratorNext) Err() error {
	return it.err
}

func (it *iteratorNext) Result() graph.Ref {
	return it.result
}

func (it *iteratorNext) NextPath(ctx context.Context) bool {
	return false
}

func (it *iteratorNext) Sorted() bool { return true }

func (it *iteratorNext) String() string {
	return fmt.Sprintf("NoSQLNext(%v)", it.collection)
}

type iteratorContains struct {
	qs         *QuadStore
	collection string
	limit      int64 // FIXME(dennwc): doesn't work right now
	constraint []nosql.FieldFilter
	links      []Linkage

	iter   nosql.DocIterator
	result graph.Ref
	err    error
}

func (qs *QuadStore) newIteratorContains(collection string, constraints []nosql.FieldFilter, links []Linkage, limit int64) *iteratorContains {
	return &iteratorContains{
		qs:         qs,
		collection: collection,
		constraint: constraints,
		links:      links,
		limit:      limit,
	}
}

func (it *iteratorContains) makeIterator() nosql.DocIterator {
	q := it.qs.db.Query(it.collection)
	if len(it.constraint) != 0 {
		q = q.WithFields(it.constraint...)
	}
	if it.limit > 0 {
		q = q.Limit(int(it.limit))
	}
	return q.Iterate()
}

func (it *iteratorContains) Close() error {
	if it.iter != nil {
		return it.iter.Close()
	}
	return nil
}

func (it *iteratorContains) TagResults(dst map[string]graph.Ref) {}

func (it *iteratorContains) Err() error {
	return it.err
}

func (it *iteratorContains) Result() graph.Ref {
	return it.result
}

func (it *iteratorContains) NextPath(ctx context.Context) bool {
	return false
}

func (it *iteratorContains) Contains(ctx context.Context, v graph.Ref) bool {
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

func (it *iteratorContains) Sorted() bool { return true }

func (it *iteratorContains) String() string {
	return fmt.Sprintf("NoSQLContains(%v)", it.collection)
}
