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
	"fmt"
	"sort"
	"strings"

	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/graph/shape"
	"github.com/cayleygraph/quad"
)

func NewOptimizer() *Optimizer {
	return &Optimizer{}
}

type Optimizer struct {
	tableInd int

	regexpOp             CmpOp
	noOffsetWithoutLimit bool // blame mysql
}

func (opt *Optimizer) SetRegexpOp(op CmpOp) {
	opt.regexpOp = op
}

func (opt *Optimizer) NoOffsetWithoutLimit() {
	opt.noOffsetWithoutLimit = true
}

func (opt *Optimizer) nextTable() string {
	opt.tableInd++
	return fmt.Sprintf("t_%d", opt.tableInd)
}

func (opt *Optimizer) ensureAliases(s *Select) {
	for i, src := range s.From {
		if t, ok := src.(Table); ok && t.Alias == "" {
			t.Alias = opt.nextTable()
			s.From[i] = t
			// TODO: copy slice
			for j := range s.Fields {
				f := &s.Fields[j]
				if f.Table == "" {
					f.Table = t.Alias
				}
			}
			for j := range s.Where {
				w := &s.Where[j]
				if w.Table == "" {
					w.Table = t.Alias
				}
			}
		}
	}
}

func sortDirs(dirs []quad.Direction) {
	sort.Slice(dirs, func(i, j int) bool {
		return dirs[i] < dirs[j]
	})
}

func (opt *Optimizer) OptimizeShape(s shape.Shape) (shape.Shape, bool) {
	switch s := s.(type) {
	case shape.AllNodes:
		return AllNodes(), true
	case shape.Lookup:
		return opt.optimizeLookup(s)
	case shape.Filter:
		return opt.optimizeFilters(s)
	case shape.Intersect:
		return opt.optimizeIntersect(s)
	case shape.Quads:
		return opt.optimizeQuads(s)
	case shape.NodesFrom:
		return opt.optimizeNodesFrom(s)
	case shape.QuadsAction:
		return opt.optimizeQuadsAction(s)
	case shape.Save:
		return opt.optimizeSave(s)
	case shape.Page:
		return opt.optimizePage(s)
	default:
		return s, false
	}
}

func selectValueQuery(v quad.Value, op CmpOp) ([]Where, []Value, bool) {
	if op == OpEqual {
		// we can use hash to check equality
		return []Where{
				{Field: "hash", Op: op, Value: Placeholder{}},
			}, []Value{
				HashOf(v),
			}, true
	}
	var (
		where  []Where
		params []Value
	)
	switch v := v.(type) {
	case quad.IRI:
		where = []Where{
			{Field: "value_string", Op: op, Value: Placeholder{}},
			{Field: "iri", Op: OpIsTrue},
		}
		params = []Value{
			StringVal(v),
		}
	case quad.BNode:
		where = []Where{
			{Field: "value_string", Op: op, Value: Placeholder{}},
			{Field: "bnode", Op: OpIsTrue},
		}
		params = []Value{
			StringVal(v),
		}
	case quad.String:
		where = []Where{
			{Field: "value_string", Op: op, Value: Placeholder{}},
			{Field: "iri", Op: OpIsNull},
			{Field: "bnode", Op: OpIsNull},
			{Field: "datatype", Op: OpIsNull},
			{Field: "language", Op: OpIsNull},
		}
		params = []Value{
			StringVal(v),
		}
	case quad.LangString:
		where = []Where{
			{Field: "value_string", Op: op, Value: Placeholder{}},
			{Field: "language", Op: OpEqual, Value: Placeholder{}},
		}
		params = []Value{
			StringVal(v.Value),
			StringVal(v.Lang),
		}
	case quad.TypedString:
		where = []Where{
			{Field: "value_string", Op: op, Value: Placeholder{}},
			{Field: "datatype", Op: OpEqual, Value: Placeholder{}},
		}
		params = []Value{
			StringVal(v.Value),
			StringVal(v.Type),
		}
	case quad.Int:
		where = []Where{
			{Field: "value_int", Op: op, Value: Placeholder{}},
		}
		params = []Value{
			IntVal(v),
		}
	case quad.Float:
		where = []Where{
			{Field: "value_float", Op: op, Value: Placeholder{}},
		}
		params = []Value{
			FloatVal(v),
		}
	case quad.Bool:
		where = []Where{
			{Field: "value_bool", Op: op, Value: Placeholder{}},
		}
		params = []Value{
			BoolVal(v),
		}
	case quad.Time:
		where = []Where{
			{Field: "value_time", Op: op, Value: Placeholder{}},
		}
		params = []Value{
			TimeVal(v),
		}
	default:
		return nil, nil, false
	}
	return where, params, true
}

func SelectValue(v quad.Value, op CmpOp) *Select {
	where, params, ok := selectValueQuery(v, op)
	if !ok {
		return nil
	}
	sel := Nodes(where, params)
	return &sel
}

func (opt *Optimizer) optimizeLookup(s shape.Lookup) (shape.Shape, bool) {
	if len(s) != 1 {
		// TODO: support for IN
		return s, false
	}
	sel := SelectValue(s[0], OpEqual)
	if sel == nil {
		return s, false
	}
	return *sel, true
}

func convRegexp(re string) string {
	return re // TODO: convert regular expression
}

func (opt *Optimizer) optimizeFilter(from shape.Shape, f shape.ValueFilter) ([]Where, []Value, bool) {
	switch f := f.(type) {
	case shape.Comparison:
		var cmp CmpOp
		switch f.Op {
		case iterator.CompareGT:
			cmp = OpGT
		case iterator.CompareGTE:
			cmp = OpGTE
		case iterator.CompareLT:
			cmp = OpLT
		case iterator.CompareLTE:
			cmp = OpLTE
		default:
			return nil, nil, false
		}
		return selectValueQuery(f.Val, cmp)
	case shape.Wildcard:
		if opt.regexpOp == "" {
			return nil, nil, false
		}
		return []Where{
				{Field: "value_string", Op: opt.regexpOp, Value: Placeholder{}},
			}, []Value{
				StringVal(convRegexp(f.Regexp())),
			}, true
	case shape.Regexp:
		if opt.regexpOp == "" {
			return nil, nil, false
		}
		where := []Where{
			{Field: "value_string", Op: opt.regexpOp, Value: Placeholder{}},
		}
		if !f.Refs {
			where = append(where, []Where{
				{Field: "iri", Op: OpIsNull},
				{Field: "bnode", Op: OpIsNull},
			}...)
		}
		return where, []Value{
			StringVal(convRegexp(f.Re.String())),
		}, true
	default:
		return nil, nil, false
	}
}
func (opt *Optimizer) optimizeFilters(s shape.Filter) (shape.Shape, bool) {
	switch from := s.From.(type) {
	case shape.AllNodes:
	case Select:
		if !from.isAll() {
			return s, false
		}
		t, ok := from.From[0].(Table)
		if !ok || t.Name != "nodes" {
			return s, false
		}
	default:
		return s, false
	}
	var (
		where  []Where
		params []Value
	)
	left := shape.Filter{
		From: s.From,
	}
	for _, f := range s.Filters {
		if w, p, ok := opt.optimizeFilter(s.From, f); ok {
			where = append(where, w...)
			params = append(params, p...)
		} else {
			left.Filters = append(left.Filters, f)
		}
	}
	if len(where) == 0 {
		return s, false
	}
	sel := Nodes(where, params)
	if len(left.Filters) == 0 {
		return sel, true
	}
	left.From = sel
	return left, true
}

func (opt *Optimizer) optimizeQuads(s shape.Quads) (shape.Shape, bool) {
	t1 := opt.nextTable()
	sel := AllQuads(t1)
	for _, f := range s {
		wr := Where{
			Table: t1,
			Field: dirField(f.Dir),
			Op:    OpEqual,
		}
		switch fv := f.Values.(type) {
		case shape.Fixed:
			if len(fv) != 1 {
				// TODO: support IN, or generate SELECT equivalent
				return s, false
			}
			wr.Value = sel.AppendParam(fv[0].(Value))
			sel.Where = append(sel.Where, wr)
		case Select:
			if len(fv.Fields) == 1 {
				// simple case - just add subquery to FROM
				tbl := opt.nextTable()
				sel.From = append(sel.From, Subquery{
					Query: fv,
					Alias: tbl,
				})
				wr.Value = FieldName{
					Name:  fv.Fields[0].NameOrAlias(),
					Table: tbl,
				}
				sel.Where = append(sel.Where, wr)
				continue
			} else if fv.onlyAsSubquery() {
				// TODO: generic subquery: pass all tags to main query, set WHERE on specific direction, drop __* tags
				return s, false
			}
			opt.ensureAliases(&fv)
			// add all tables from subquery to the main one, but skip __node field - we should add it to WHERE
			var head Field
			for _, f := range fv.Fields {
				if f.Alias == tagNode {
					for _, w := range fv.Where {
						if w.Table == f.Table && w.Field == f.Alias {
							// TODO: if __node was used in WHERE of subquery, we should rewrite it
							return s, false
						}
					}
					f.Alias = ""
					head = f
					continue
				}
				sel.Fields = append(sel.Fields, f)
			}
			if head.Table == "" {
				// something is wrong
				return s, false
			}
			sel.From = append(sel.From, fv.From...)
			sel.Where = append(sel.Where, fv.Where...)
			sel.Params = append(sel.Params, fv.Params...)
			wr.Value = FieldName{
				Name:  head.Name,
				Table: head.Table,
			}
			sel.Where = append(sel.Where, wr)
		default:
			return s, false
		}
	}
	return sel, true
}

func (opt *Optimizer) optimizeNodesFrom(s shape.NodesFrom) (shape.Shape, bool) {
	sel, ok := s.Quads.(Select)
	if !ok {
		return s, false
	}
	sel.Fields = append([]Field{}, sel.Fields...)

	// all we need is to remove all quad-related tags and preserve one with matching direction
	dir := dirTag(s.Dir)
	found := false
	for i := 0; i < len(sel.Fields); i++ {
		f := &sel.Fields[i]
		if f.Alias == dir {
			f.Alias = tagNode
			found = true
		} else if strings.HasPrefix(f.Alias, tagPref) {
			sel.Fields = append(sel.Fields[:i], sel.Fields[i+1:]...)
			i--
		}
	}
	if !found {
		return s, false
	}
	return sel, true
}

func (opt *Optimizer) optimizeQuadsAction(s shape.QuadsAction) (shape.Shape, bool) {
	sel := Select{
		Fields: []Field{
			{Name: dirField(s.Result), Alias: tagNode},
		},
		From: []Source{
			Table{Name: "quads"},
		},
	}
	var dirs []quad.Direction
	for d := range s.Save {
		dirs = append(dirs, d)
	}
	sortDirs(dirs)
	for _, d := range dirs {
		for _, t := range s.Save[d] {
			sel.Fields = append(sel.Fields, Field{
				Name: dirField(d), Alias: t,
			})
		}
	}
	dirs = nil
	for d := range s.Filter {
		dirs = append(dirs, d)
	}
	sortDirs(dirs)
	for _, d := range dirs {
		v := s.Filter[d]
		sel.WhereEq("", dirField(d), v.(Value))
	}
	return sel, true
}

func (opt *Optimizer) optimizeSave(s shape.Save) (shape.Shape, bool) {
	sel, ok := s.From.(Select)
	if !ok {
		return s, false
	}
	// find primary value used by iterators
	fi := -1
	for i, f := range sel.Fields {
		if f.Alias == tagNode {
			fi = i
			break
		}
	}
	if fi < 0 {
		return s, false
	}
	// add SELECT fields as aliases for primary field
	f := sel.Fields[fi]
	fields := make([]Field, 0, len(s.Tags)+len(sel.Fields))
	for _, tag := range s.Tags {
		f.Alias = tag
		fields = append(fields, f)
	}
	// add other fields
	fields = append(fields, sel.Fields...)
	sel.Fields = fields
	return sel, true
}

func (opt *Optimizer) optimizePage(s shape.Page) (shape.Shape, bool) {
	sel, ok := s.From.(Select)
	if !ok {
		return s, false
	}
	// do not optimize if db only can use offset with limit, and we have no limits set
	if opt.noOffsetWithoutLimit && sel.Limit == 0 && s.Limit == 0 {
		return s, false
	}
	// call shapes optimizer to calculate correct skip and limit
	p := shape.Page{
		Skip:  sel.Offset,
		Limit: sel.Limit,
	}.ApplyPage(s)
	if p == nil {
		// no intersection - no results
		return nil, true
	}
	sel.Limit = p.Limit
	sel.Offset = p.Skip
	return sel, true
}

func (opt *Optimizer) optimizeIntersect(s shape.Intersect) (shape.Shape, bool) {
	var (
		sels  []Select
		other shape.Intersect
	)
	// we will add our merged Select to this slot
	other = append(other, nil)
	for _, sub := range s {
		// TODO: sort by onlySubquery flag first
		if sel, ok := sub.(Select); ok && !sel.onlyAsSubquery() {
			sels = append(sels, sel)
		} else {
			other = append(other, sub)
		}
	}
	if len(sels) <= 1 {
		return s, false
	}
	for i := range sels {
		sels[i] = sels[i].Clone()
		opt.ensureAliases(&sels[i])
	}
	pri := sels[0]
	var head *Field
	for i, f := range pri.Fields {
		if f.Alias == tagNode {
			head = &pri.Fields[i]
			break
		}
	}
	if head == nil {
		return s, false
	}
	sec := sels[1:]

	for _, s2 := range sec {
		// merge From, Where and Params
		pri.From = append(pri.From, s2.From...)
		pri.Where = append(pri.Where, s2.Where...)
		pri.Params = append(pri.Params, s2.Params...)
		// also find and remove primary tag, but add the same field to WHERE
		ok := false
		for _, f := range s2.Fields {
			if f.Alias == tagNode {
				ok = true
				pri.Where = append(pri.Where, Where{
					Table: head.Table,
					Field: head.Name,
					Op:    OpEqual,
					Value: FieldName{
						Table: f.Table,
						Name:  f.Name,
					},
				})
			} else {
				pri.Fields = append(pri.Fields, f)
			}
		}
		if !ok {
			return s, false
		}
	}
	if len(other) == 1 {
		return pri, true
	}
	other[0] = pri
	return other, true
}
