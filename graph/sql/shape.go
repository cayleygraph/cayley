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
	"strconv"
	"strings"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/graph/shape"
	"github.com/cayleygraph/quad"
)

var DefaultDialect = QueryDialect{
	FieldQuote: func(s string) string {
		return strconv.Quote(s)
	},
	Placeholder: func(_ int) string {
		return "?"
	},
}

type QueryDialect struct {
	RegexpOp    CmpOp
	FieldQuote  func(string) string
	Placeholder func(int) string
}

func NewBuilder(d QueryDialect) *Builder {
	return &Builder{d: d}
}

type Builder struct {
	d  QueryDialect
	pi int
}

func needQuotes(s string) bool {
	for i, r := range s {
		if (r < 'a' || r > 'z') && r != '_' && (i == 0 || r < '0' || r > '9') {
			return true
		}
	}
	return false
}
func (b *Builder) EscapeField(s string) string {
	if !needQuotes(s) {
		return s
	}
	return b.d.FieldQuote(s)
}
func (b *Builder) Placeholder() string {
	b.pi++
	return b.d.Placeholder(b.pi)
}

const (
	tagPref = "__"
	tagNode = tagPref + "node"
)

func dirField(d quad.Direction) string {
	return d.String() + "_hash"
}

func dirTag(d quad.Direction) string {
	return tagPref + d.String()
}

type Value interface {
	SQLValue() interface{}
}

type Shape interface {
	SQL(b *Builder) string
	Args() []Value
	Columns() []string
}

func AllNodes() Select {
	return Nodes(nil, nil)
}

func Nodes(where []Where, params []Value) Select {
	return Select{
		Fields: []Field{
			{Name: "hash", Alias: tagNode},
		},
		From: []Source{
			Table{Name: "nodes"},
		},
		Where:  where,
		Params: params,
	}
}

func AllQuads(alias string) Select {
	sel := Select{
		From: []Source{
			Table{Name: "quads", Alias: alias},
		},
	}
	for _, d := range quad.Directions {
		sel.Fields = append(sel.Fields, Field{
			Table: alias,
			Name:  dirField(d),
			Alias: dirTag(d),
		})
	}
	return sel
}

type FieldName struct {
	Name  string
	Table string
}

func (FieldName) isExpr() {}
func (f FieldName) SQL(b *Builder) string {
	name := b.EscapeField(f.Name)
	if f.Table != "" {
		name = f.Table + "." + name
	}
	return name
}

type Field struct {
	Name  string
	Raw   bool // do not quote Name
	Alias string
	Table string
}

func (f Field) SQL(b *Builder) string {
	name := f.Name
	if !f.Raw {
		name = b.EscapeField(name)
	}
	if f.Table != "" {
		name = f.Table + "." + name
	}
	if f.Alias == "" {
		return name
	}
	return name + " AS " + b.EscapeField(f.Alias)
}
func (f Field) NameOrAlias() string {
	if f.Alias != "" {
		return f.Alias
	}
	return f.Name
}

type Source interface {
	SQL(b *Builder) string
	Args() []Value
	isSource()
}

type Table struct {
	Name  string
	Alias string
}

func (Table) isSource() {}

type Subquery struct {
	Query Select
	Alias string
}

func (Subquery) isSource() {}
func (s Subquery) SQL(b *Builder) string {
	q := "(" + s.Query.SQL(b) + ")"
	if s.Alias != "" {
		q += " AS " + b.EscapeField(s.Alias)
	}
	return q
}
func (s Subquery) Args() []Value {
	return s.Query.Args()
}

func (f Table) SQL(b *Builder) string {
	if f.Alias == "" {
		return f.Name
	}
	return f.Name + " AS " + b.EscapeField(f.Alias)
}

func (f Table) Args() []Value {
	return nil
}
func (f Table) NameSQL() string {
	if f.Alias != "" {
		return f.Alias
	}
	return f.Name
}

type CmpOp string

const (
	OpEqual  = CmpOp("=")
	OpGT     = CmpOp(">")
	OpGTE    = CmpOp(">=")
	OpLT     = CmpOp("<")
	OpLTE    = CmpOp("<=")
	OpIsNull = CmpOp("IS NULL")
	OpIsTrue = CmpOp("IS true")
)

type Expr interface {
	isExpr()
	SQL(b *Builder) string
}

type Placeholder struct{}

func (Placeholder) isExpr() {}

func (Placeholder) SQL(b *Builder) string {
	return b.Placeholder()
}

type Where struct {
	Field string
	Table string
	Op    CmpOp
	Value Expr
}

func (w Where) SQL(b *Builder) string {
	name := w.Field
	if w.Table != "" {
		name = w.Table + "." + b.EscapeField(name)
	}
	parts := []string{name, string(w.Op)}
	if w.Value != nil {
		parts = append(parts, w.Value.SQL(b))
	}
	return strings.Join(parts, " ")
}

var _ Shape = Select{}

// Select is a simplified representation of SQL SELECT query.
type Select struct {
	Fields []Field
	From   []Source
	Where  []Where
	Params []Value
	Limit  int64
	Offset int64
}

func (s Select) Clone() Select {
	s.Fields = append([]Field{}, s.Fields...)
	s.From = append([]Source{}, s.From...)
	s.Where = append([]Where{}, s.Where...)
	s.Params = append([]Value{}, s.Params...)
	return s
}

func (s Select) isAll() bool {
	return len(s.From) == 1 && len(s.Where) == 0 && len(s.Params) == 0 && !s.onlyAsSubquery()
}

// onlyAsSubquery indicates that query cannot be merged into existing SELECT because of some specific properties of query.
// An example of such properties might be LIMIT, DISTINCT, etc.
func (s Select) onlyAsSubquery() bool {
	return s.Limit > 0 || s.Offset > 0
}

func (s Select) Columns() []string {
	names := make([]string, 0, len(s.Fields))
	for _, f := range s.Fields {
		name := f.Alias
		if name == "" {
			name = f.Name
		}
		names = append(names, name)
	}
	return names
}

func (s Select) BuildIterator(qs graph.QuadStore) graph.Iterator {
	sq, ok := qs.(*QuadStore)
	if !ok {
		return iterator.NewError(fmt.Errorf("not a SQL quadstore: %T", qs))
	}
	return sq.NewIterator(s)
}

func (s Select) Optimize(r shape.Optimizer) (shape.Shape, bool) {
	// TODO: call optimize on sub-tables? but what if it decides to de-optimize our SQL shape?
	return s, false
}

func (s *Select) AppendParam(o Value) Expr {
	s.Params = append(s.Params, o)
	return Placeholder{}
}

func (s *Select) WhereEq(tbl, field string, v Value) {
	s.Where = append(s.Where, Where{
		Table: tbl,
		Field: field,
		Op:    OpEqual,
		Value: s.AppendParam(v),
	})
}

func (s Select) SQL(b *Builder) string {
	var parts []string

	var fields []string
	for _, f := range s.Fields {
		fields = append(fields, f.SQL(b))
	}
	parts = append(parts, "SELECT "+strings.Join(fields, ", "))

	var tables []string
	for _, t := range s.From {
		tables = append(tables, t.SQL(b))
	}
	parts = append(parts, "FROM "+strings.Join(tables, ", "))

	if len(s.Where) != 0 {
		var wheres []string
		for _, w := range s.Where {
			wheres = append(wheres, w.SQL(b))
		}
		parts = append(parts, "WHERE "+strings.Join(wheres, " AND "))
	}
	if s.Limit > 0 {
		parts = append(parts, "LIMIT "+strconv.FormatInt(s.Limit, 10))
	}
	if s.Offset > 0 {
		parts = append(parts, "OFFSET "+strconv.FormatInt(s.Offset, 10))
	}
	sep := " "
	if len(fields) > 1 {
		sep = "\n\t"
	}
	return strings.Join(parts, sep)
}
func (s Select) Args() []Value {
	var args []Value
	// first add args for FROM subqueries
	for _, q := range s.From {
		args = append(args, q.Args()...)
	}
	// and add params for WHERE
	args = append(args, s.Params...)
	return args
}
