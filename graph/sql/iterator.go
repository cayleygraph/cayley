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
	"github.com/cayleygraph/cayley/graph/shape"
	"github.com/cayleygraph/quad"
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

var _ graph.IteratorFuture = (*Iterator)(nil)

func (qs *QuadStore) NewIterator(s Select) *Iterator {
	it := &Iterator{
		it: qs.newIterator(s),
	}
	it.Iterator = graph.NewLegacy(it.it, it)
	return it
}

type Iterator struct {
	it *iterator2
	graph.Iterator
}

func (it *Iterator) AsShape() graph.IteratorShape {
	it.Close()
	return it.it
}

var _ graph.IteratorShapeCompat = (*iterator2)(nil)

func (qs *QuadStore) newIterator(s Select) *iterator2 {
	return &iterator2{
		qs:    qs,
		query: s,
	}
}

type iterator2 struct {
	qs    *QuadStore
	query Select
	err   error
}

func (it *iterator2) Iterate() graph.Scanner {
	return newIteratorNext(it.qs, it.query)
}

func (it *iterator2) Lookup() graph.Index {
	return newIteratorContains(it.qs, it.query)
}

func (it *iterator2) AsLegacy() graph.Iterator {
	it2 := &Iterator{it: it}
	it2.Iterator = graph.NewLegacy(it, it2)
	return it2
}

func (it *iterator2) Stats(ctx context.Context) (graph.IteratorCosts, error) {
	sz, err := it.getSize(ctx)
	return graph.IteratorCosts{
		NextCost:     1,
		ContainsCost: 10,
		Size:         sz,
	}, err
}

func (it *iterator2) estimateSize(ctx context.Context) int64 {
	if it.query.Limit > 0 {
		return it.query.Limit
	}
	st, err := it.qs.Stats(ctx, false)
	if err != nil && it.err == nil {
		it.err = err
	}
	return st.Quads.Size
}

func (it *iterator2) getSize(ctx context.Context) (graph.Size, error) {
	sz, err := it.qs.querySize(ctx, it.query)
	if err != nil {
		it.err = err
		return graph.Size{Size: it.estimateSize(ctx), Exact: false}, err
	}
	return sz, nil
}

func (it *iterator2) Optimize(ctx context.Context) (graph.IteratorShape, bool) {
	return it, false
}

func (it *iterator2) SubIterators() []graph.IteratorShape {
	return nil
}

func (it *iterator2) String() string {
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

func newIteratorNext(qs *QuadStore, s Select) *iteratorNext {
	return &iteratorNext{
		iteratorBase: newIteratorBase(qs, s),
	}
}

type iteratorNext struct {
	iteratorBase
	cursor *sql.Rows
}

func (it *iteratorNext) TagResults(m map[string]graph.Ref) {
	for tag, val := range it.tags {
		m[tag] = val
	}
}

func (it *iteratorNext) Result() graph.Ref {
	return it.res
}

func (it *iteratorNext) ensureColumns() {
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

func (it *iteratorNext) scanValue(r *sql.Rows) bool {
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

func (it *iteratorNext) Next(ctx context.Context) bool {
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

func (it *iteratorNext) NextPath(ctx context.Context) bool {
	return false
}

func (it *iteratorNext) Err() error {
	return it.err
}

func (it *iteratorNext) String() string {
	return it.query.SQL(NewBuilder(it.qs.flavor.QueryDialect))
}

func (it *iteratorNext) Close() error {
	if it.cursor != nil {
		it.cursor.Close()
		it.cursor = nil
	}
	return nil
}

func newIteratorContains(qs *QuadStore, s Select) *iteratorContains {
	return &iteratorContains{
		iteratorBase: newIteratorBase(qs, s),
	}
}

type iteratorContains struct {
	iteratorBase
}

func (it *iteratorContains) TagResults(m map[string]graph.Ref) {
	for tag, val := range it.tags {
		m[tag] = val
	}
}

func (it *iteratorContains) Result() graph.Ref {
	return it.res
}

func (it *iteratorContains) ensureColumns() {
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

func (it *iteratorContains) scanValue(r *sql.Rows) bool {
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

func (it *iteratorContains) NextPath(ctx context.Context) bool {
	return false
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
	defer rows.Close()
	if !rows.Next() {
		it.err = rows.Err()
		return false
	}
	return it.scanValue(rows)
}

func (it *iteratorContains) Err() error {
	return it.err
}

func (it *iteratorContains) String() string {
	return it.query.SQL(NewBuilder(it.qs.flavor.QueryDialect))
}

func (it *iteratorContains) Close() error {
	return nil
}
