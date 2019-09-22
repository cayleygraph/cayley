package nosql

import (
	"fmt"
	"math"
	"strconv"

	"github.com/hidal-go/hidalgo/legacy/nosql"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/graph/shape"
	"github.com/cayleygraph/quad"
)

var _ shape.Optimizer = (*QuadStore)(nil)

func (qs *QuadStore) OptimizeShape(s shape.Shape) (shape.Shape, bool) {
	switch s := s.(type) {
	case shape.Quads:
		return qs.optimizeQuads(s)
	case shape.Filter:
		return qs.optimizeFilter(s)
	case shape.Page:
		return qs.optimizePage(s)
	case shape.Composite:
		if s2, opt := s.Simplify().Optimize(qs); opt {
			return s2, true
		}
	}
	return s, false
}

// Shape is a shape representing a documents query with filters
type Shape struct {
	Collection string              // name of the collection
	Filters    []nosql.FieldFilter // filters to select documents
	Limit      int64               // limits a number of documents
}

func (s Shape) BuildIterator(qs graph.QuadStore) graph.Iterator {
	db, ok := qs.(*QuadStore)
	if !ok {
		return iterator.NewError(fmt.Errorf("not a nosql database: %T", qs))
	}
	return NewIterator(db, s.Collection, s.Filters...)
}

func (s Shape) Optimize(r shape.Optimizer) (shape.Shape, bool) {
	return s, false
}

// Quads is a shape representing a quads query
type Quads struct {
	Links []Linkage // filters to select quads
	Limit int64     // limits a number of documents
}

func (s Quads) BuildIterator(qs graph.QuadStore) graph.Iterator {
	db, ok := qs.(*QuadStore)
	if !ok {
		return iterator.NewError(fmt.Errorf("not a nosql database: %T", qs))
	}
	return NewLinksToIterator(db, colQuads, s.Links)
}

func (s Quads) Optimize(r shape.Optimizer) (shape.Shape, bool) {
	return s, false
}

const int64Adjust = 1 << 63

// itos serializes int64 into a sortable string 13 chars long.
func itos(i int64) string {
	s := strconv.FormatUint(uint64(i)+int64Adjust, 32)
	const z = "0000000000000"
	return z[len(s):] + s
}

// stoi de-serializes int64 from a sortable string 13 chars long.
func stoi(s string) int64 {
	ret, err := strconv.ParseUint(s, 32, 64)
	if err != nil {
		//TODO handle error?
		return 0
	}
	return int64(ret - int64Adjust)
}

func toFieldFilter(opt *Traits, c shape.Comparison) ([]nosql.FieldFilter, bool) {
	var op nosql.FilterOp
	switch c.Op {
	case iterator.CompareGT:
		op = nosql.GT
	case iterator.CompareGTE:
		op = nosql.GTE
	case iterator.CompareLT:
		op = nosql.LT
	case iterator.CompareLTE:
		op = nosql.LTE
	default:
		return nil, false
	}
	fieldPath := func(s string) []string {
		return []string{fldValue, s}
	}

	var filters []nosql.FieldFilter
	switch v := c.Val.(type) {
	case quad.String:
		filters = []nosql.FieldFilter{
			{Path: fieldPath(fldValData), Filter: op, Value: nosql.String(v)},
			{Path: fieldPath(fldIRI), Filter: nosql.NotEqual, Value: nosql.Bool(true)},
			{Path: fieldPath(fldBNode), Filter: nosql.NotEqual, Value: nosql.Bool(true)},
		}
	case quad.IRI:
		filters = []nosql.FieldFilter{
			{Path: fieldPath(fldValData), Filter: op, Value: nosql.String(v)},
			{Path: fieldPath(fldIRI), Filter: nosql.Equal, Value: nosql.Bool(true)},
		}
	case quad.BNode:
		filters = []nosql.FieldFilter{
			{Path: fieldPath(fldValData), Filter: op, Value: nosql.String(v)},
			{Path: fieldPath(fldBNode), Filter: nosql.Equal, Value: nosql.Bool(true)},
		}
	case quad.Int:
		if opt.Number32 && (v < math.MinInt32 || v > math.MaxInt32) {
			// switch to range on string values
			filters = []nosql.FieldFilter{
				{Path: fieldPath(fldValStrInt), Filter: op, Value: nosql.String(itos(int64(v)))},
			}
		} else {
			filters = []nosql.FieldFilter{
				{Path: fieldPath(fldValInt), Filter: op, Value: nosql.Int(v)},
			}
		}
	case quad.Float:
		filters = []nosql.FieldFilter{
			{Path: fieldPath(fldValFloat), Filter: op, Value: nosql.Float(v)},
		}
	case quad.Time:
		filters = []nosql.FieldFilter{
			{Path: fieldPath(fldValTime), Filter: op, Value: nosql.Time(v)},
		}
	default:
		return nil, false
	}
	return filters, true
}

func (qs *QuadStore) optimizeFilter(s shape.Filter) (shape.Shape, bool) {
	if _, ok := s.From.(shape.AllNodes); !ok {
		return s, false
	}
	var (
		filters []nosql.FieldFilter
		left    []shape.ValueFilter
	)
	fieldPath := func(s string) []string {
		return []string{fldValue, s}
	}
	for _, f := range s.Filters {
		switch f := f.(type) {
		case shape.Comparison:
			if fld, ok := toFieldFilter(&qs.opt, f); ok {
				filters = append(filters, fld...)
				continue
			}
		case shape.Wildcard:
			filters = append(filters, []nosql.FieldFilter{
				{Path: fieldPath(fldValData), Filter: nosql.Regexp, Value: nosql.String(f.Regexp())},
			}...)
			continue
		case shape.Regexp:
			filters = append(filters, []nosql.FieldFilter{
				{Path: fieldPath(fldValData), Filter: nosql.Regexp, Value: nosql.String(f.Re.String())},
			}...)
			if !f.Refs {
				filters = append(filters, []nosql.FieldFilter{
					{Path: fieldPath(fldIRI), Filter: nosql.NotEqual, Value: nosql.Bool(true)},
					{Path: fieldPath(fldBNode), Filter: nosql.NotEqual, Value: nosql.Bool(true)},
				}...)
			}
			continue
		}
		left = append(left, f)
	}
	if len(filters) == 0 {
		return s, false
	}
	var ns shape.Shape = Shape{Collection: colNodes, Filters: filters}
	if len(left) != 0 {
		ns = shape.Filter{From: ns, Filters: left}
	}
	return ns, true
}

func (qs *QuadStore) optimizeQuads(s shape.Quads) (shape.Shape, bool) {
	var (
		links []Linkage
		left  []shape.QuadFilter
	)
	for _, f := range s {
		if v, ok := shape.One(f.Values); ok {
			if h, ok := v.(NodeHash); ok {
				links = append(links, Linkage{Dir: f.Dir, Val: h})
				continue
			}
		}
		left = append(left, f)
	}
	if len(links) == 0 {
		return s, false
	}
	var ns shape.Shape = Quads{Links: links}
	if len(left) != 0 {
		ns = shape.Intersect{ns, shape.Quads(left)}
	}
	return s, true
}

func (qs *QuadStore) optimizePage(s shape.Page) (shape.Shape, bool) {
	if s.Skip != 0 {
		return s, false
	}
	switch f := s.From.(type) {
	case shape.AllNodes:
		return Shape{Collection: colNodes, Limit: s.Limit}, false
	case Shape:
		s.ApplyPage(shape.Page{Limit: f.Limit})
		f.Limit = s.Limit
		return f, true
	case Quads:
		s.ApplyPage(shape.Page{Limit: f.Limit})
		f.Limit = s.Limit
		return f, true
	}
	return s, false
}
