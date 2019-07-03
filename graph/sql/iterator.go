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
	"github.com/cayleygraph/cayley/graph/shape"
	"github.com/cayleygraph/cayley/quad"
)

var _ shape.Optimizer = (*QuadStore)(nil)

func (qs *QuadStore) OptimizeShape(s shape.Shape) (shape.Shape, bool) {
	return qs.opt.OptimizeShape(s)
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

var _ graph.Iterator = (*Iterator)(nil)

func (qs *QuadStore) NewIterator(s Select) *Iterator {
	return &Iterator{
		qs:    qs,
		uid:   iterator.NextUID(),
		query: s,
	}
}

type Iterator struct {
	qs    *QuadStore
	uid   uint64
	query Select

	cols []string
	cind map[quad.Direction]int

	err    error
	res    graph.Ref
	tags   map[string]graph.Ref
	cursor *sql.Rows
}

func (it *Iterator) UID() uint64 {
	return it.uid
}

func (it *Iterator) TagResults(m map[string]graph.Ref) {
	for tag, val := range it.tags {
		m[tag] = val
	}
}

func (it *Iterator) Result() graph.Ref {
	return it.res
}

func (it *Iterator) ensureColumns() {
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

func (it *Iterator) scanValue(r *sql.Rows) bool {
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

func (it *Iterator) Next(ctx context.Context) bool {
	if it.err != nil {
		return false
	}
	if it.cursor == nil {
		it.cursor, it.err = it.qs.Query(ctx, it.query)
	}
	if it.err != nil {
		return false
	}
	if !it.cursor.Next() {
		it.err = it.cursor.Err()
		it.cursor.Close()
		return false
	}
	return it.scanValue(it.cursor)
}

func (it *Iterator) NextPath(ctx context.Context) bool {
	return false
}

func (it *Iterator) Contains(ctx context.Context, v graph.Ref) bool {
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
	defer rows.Close()
	if !rows.Next() {
		it.err = rows.Err()
		return false
	}
	return it.scanValue(rows)
}

func (it *Iterator) Err() error {
	return it.err
}

func (it *Iterator) Reset() {
	it.cols = nil
	it.cind = nil
	it.res = nil
	it.err = nil
	if it.cursor != nil {
		it.cursor.Close()
		it.cursor = nil
	}
}

func (it *Iterator) Stats() graph.IteratorStats {
	sz, exact := it.Size()
	return graph.IteratorStats{
		NextCost:     1,
		ContainsCost: 10,
		Size:         sz, ExactSize: exact,
	}
}

func (it *Iterator) estimateSize() int64 {
	if it.query.Limit > 0 {
		return it.query.Limit
	}
	st, err := it.qs.Stats(context.TODO(), false)
	if err != nil && it.err == nil {
		it.err = err
	}
	return st.Quads.Size
}

func (it *Iterator) Size() (int64, bool) {
	sz, err := it.qs.querySize(context.TODO(), it.query)
	if err != nil {
		it.err = err
		return it.estimateSize(), false
	}
	return sz.Size, sz.Exact
}

func (it *Iterator) Optimize() (graph.Iterator, bool) {
	return it, false
}

func (it *Iterator) SubIterators() []graph.Iterator {
	return nil
}

func (it *Iterator) String() string {
	return it.query.SQL(NewBuilder(it.qs.flavor.QueryDialect))
}

func (it *Iterator) Close() error {
	if it.cursor != nil {
		it.cursor.Close()
		it.cursor = nil
	}
	return nil
}
