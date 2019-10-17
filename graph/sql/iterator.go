// Copyright 2017 The Cayley Authors. All rights reserved.
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

package sql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/graph/refs"
	"github.com/cayleygraph/cayley/query/shape"
	"github.com/cayleygraph/quad"
)

var _ shape.Optimizer = (*QuadStore)(nil)

func (qs *QuadStore) OptimizeShape(ctx context.Context, s shape.Shape) (shape.Shape, bool) {
	return qs.opt.OptimizeShape(ctx, s)
}

func (qs *QuadStore) prepareQuery(s Shape) (string, []interface{}) {
	args := s.Args()
	vals := make([]interface{}, 0, len(args))
	for _, a := range args {
		vals = append(vals, a.SQLValue())
	}
	b := NewBuilder(qs.flavor.QueryDialect)
	qu := s.SQL(b)
	return qu, vals
}

func (qs *QuadStore) QueryRow(ctx context.Context, s Shape) *sql.Row {
	qu, vals := qs.prepareQuery(s)
	return qs.db.QueryRowContext(ctx, qu, vals...)
}

func (qs *QuadStore) Query(ctx context.Context, s Shape) (*sql.Rows, error) {
	qu, vals := qs.prepareQuery(s)
	rows, err := qs.db.QueryContext(ctx, qu, vals...)
	if err != nil {
		return nil, fmt.Errorf("sql query failed: %v\nquery: %v", err, qu)
	}
	return rows, nil
}

func (qs *QuadStore) newIterator(s Select) *Iterator {
	return &Iterator{
		qs:    qs,
		query: s,
	}
}

type Iterator struct {
	qs    *QuadStore
	query Select
	err   error
}

func (it *Iterator) Iterate() iterator.Scanner {
	return it.qs.newIteratorNext(it.query)
}

func (it *Iterator) Lookup() iterator.Index {
	return it.qs.newIteratorContains(it.query)
}

func (it *Iterator) Stats(ctx context.Context) (iterator.Costs, error) {
	sz, err := it.getSize(ctx)
	return iterator.Costs{
		NextCost:     1,
		ContainsCost: 10,
		Size:         sz,
	}, err
}

func (it *Iterator) estimateSize(ctx context.Context) int64 {
	if it.query.Limit > 0 {
		return it.query.Limit
	}
	st, err := it.qs.Stats(ctx, false)
	if err != nil && it.err == nil {
		it.err = err
	}
	return st.Quads.Value
}

func (it *Iterator) getSize(ctx context.Context) (refs.Size, error) {
	sz, err := it.qs.querySize(ctx, it.query)
	if err != nil {
		it.err = err
		return refs.Size{Value: it.estimateSize(ctx), Exact: false}, err
	}
	return sz, nil
}

func (it *Iterator) Optimize(ctx context.Context) (iterator.Shape, bool) {
	return it, false
}

func (it *Iterator) SubIterators() []iterator.Shape {
	return nil
}

func (it *Iterator) String() string {
	return it.query.SQL(NewBuilder(it.qs.flavor.QueryDialect))
}

func newIteratorBase(qs *QuadStore, s Select) iteratorBase {
	return iteratorBase{
		qs:    qs,
		query: s,
	}
}

type iteratorBase struct {
	qs    *QuadStore
	query Select

	cols []string
	cind map[quad.Direction]int

	err  error
	res  graph.Ref
	tags map[string]graph.Ref
}

func (it *iteratorBase) TagResults(m map[string]graph.Ref) {
	for tag, val := range it.tags {
		m[tag] = val
	}
}

func (it *iteratorBase) Result() graph.Ref {
	return it.res
}

func (it *iteratorBase) ensureColumns() {
	if it.cols != nil {
		return
	}
	it.cols = it.query.Columns()
	it.cind = make(map[quad.Direction]int, len(quad.Directions)+1)
	for i, name := range it.cols {
		if !strings.HasPrefix(name, tagPref) {
			continue
		}
		if name == tagNode {
			it.cind[quad.Any] = i
			continue
		}
		name = name[len(tagPref):]
		for _, d := range quad.Directions {
			if name == d.String() {
				it.cind[d] = i
				break
			}
		}
	}
}

func (it *iteratorBase) scanValue(r *sql.Rows) bool {
	it.ensureColumns()
	nodes := make([]NodeHash, len(it.cols))
	pointers := make([]interface{}, len(nodes))
	for i := range pointers {
		pointers[i] = &nodes[i]
	}
	if err := r.Scan(pointers...); err != nil {
		it.err = err
		return false
	}
	it.tags = make(map[string]graph.Ref)
	for i, name := range it.cols {
		if !strings.Contains(name, tagPref) {
			it.tags[name] = nodes[i].ValueHash
		}
	}
	if len(it.cind) > 1 {
		var q QuadHashes
		for _, d := range quad.Directions {
			i, ok := it.cind[d]
			if !ok {
				it.err = fmt.Errorf("cannot find quad %v in query output (columns: %v)", d, it.cols)
				return false
			}
			q.Set(d, nodes[i].ValueHash)
		}
		it.res = q
		return true
	}
	i, ok := it.cind[quad.Any]
	if !ok {
		it.err = fmt.Errorf("cannot find node hash in query output (columns: %v, cind: %v)", it.cols, it.cind)
		return false
	}
	it.res = nodes[i]
	return true
}

func (it *iteratorBase) Err() error {
	return it.err
}

func (it *iteratorBase) String() string {
	return it.query.SQL(NewBuilder(it.qs.flavor.QueryDialect))
}

func (qs *QuadStore) newIteratorNext(s Select) *iteratorNext {
	return &iteratorNext{
		iteratorBase: newIteratorBase(qs, s),
	}
}

type iteratorNext struct {
	iteratorBase
	cursor *sql.Rows
	// TODO(dennwc): nextPath workaround; remove when we get rid of NextPath in general
	nextPathRes  graph.Ref
	nextPathTags map[string]graph.Ref
}

func (it *iteratorNext) Next(ctx context.Context) bool {
	if it.err != nil {
		return false
	}
	if it.cursor == nil {
		it.cursor, it.err = it.qs.Query(ctx, it.query)
	}
	// TODO(dennwc): this loop exists only because of nextPath workaround
	for {
		if it.err != nil {
			return false
		}
		if it.nextPathRes != nil {
			it.res = it.nextPathRes
			it.tags = it.nextPathTags
			it.nextPathRes = nil
			it.nextPathTags = nil
			return true
		}
		if !it.cursor.Next() {
			it.err = it.cursor.Err()
			it.cursor.Close()
			return false
		}

		prev := it.res
		if !it.scanValue(it.cursor) {
			return false
		}
		if !it.query.nextPath {
			return true
		}
		if prev == nil || prev.Key() != it.res.Key() {
			return true
		}
		// skip the same main key if in nextPath mode
		// the user should receive accept those results via NextPath of the iterator
	}
}

func (it *iteratorNext) NextPath(ctx context.Context) bool {
	if it.err != nil {
		return false
	}
	if !it.query.nextPath {
		return false
	}
	if !it.cursor.Next() {
		it.err = it.cursor.Err()
		it.cursor.Close()
		return false
	}
	prev := it.res
	if !it.scanValue(it.cursor) {
		return false
	}
	if prev.Key() == it.res.Key() {
		return true
	}
	// different main keys - return false, but keep this results for the Next
	it.nextPathRes = it.res
	it.nextPathTags = it.tags
	it.res = nil
	it.tags = nil
	return false
}

func (it *iteratorNext) Close() error {
	if it.cursor != nil {
		it.cursor.Close()
		it.cursor = nil
	}
	return nil
}

func (qs *QuadStore) newIteratorContains(s Select) *iteratorContains {
	return &iteratorContains{
		iteratorBase: newIteratorBase(qs, s),
	}
}

type iteratorContains struct {
	iteratorBase
	// TODO(dennwc): nextPath workaround; remove when we get rid of NextPath in general
	nextPathRows *sql.Rows
}

func (it *iteratorContains) Contains(ctx context.Context, v graph.Ref) bool {
	it.ensureColumns()
	sel := it.query
	sel.Where = append([]Where{}, sel.Where...)
	switch v := v.(type) {
	case NodeHash:
		i, ok := it.cind[quad.Any]
		if !ok {
			return false
		}
		f := it.query.Fields[i]
		sel.WhereEq(f.Table, f.Name, v)
	case QuadHashes:
		for _, d := range quad.Directions {
			i, ok := it.cind[d]
			if !ok {
				return false
			}
			h := v.Get(d)
			if !h.Valid() {
				continue
			}
			f := it.query.Fields[i]
			sel.WhereEq(f.Table, f.Name, NodeHash{h})
		}
	default:
		return false
	}

	rows, err := it.qs.Query(ctx, sel)
	if err != nil {
		it.err = err
		return false
	}
	if it.query.nextPath {
		if it.nextPathRows != nil {
			_ = it.nextPathRows.Close()
		}
		it.nextPathRows = rows
	} else {
		defer rows.Close()
	}
	if !rows.Next() {
		it.err = rows.Err()
		return false
	}
	return it.scanValue(rows)
}

func (it *iteratorContains) NextPath(ctx context.Context) bool {
	if it.err != nil {
		return false
	}
	if !it.query.nextPath {
		return false
	}
	if !it.nextPathRows.Next() {
		it.err = it.nextPathRows.Err()
		return false
	}
	return it.scanValue(it.nextPathRows)
}

func (it *iteratorContains) Close() error {
	if it.nextPathRows != nil {
		return it.nextPathRows.Close()
	}
	return nil
}
