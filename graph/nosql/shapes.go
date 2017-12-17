package nosql

import (
	"fmt"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/graph/shape"
	"github.com/cayleygraph/cayley/quad"
)

func (qs *QuadStore) OptimizeIterator(it graph.Iterator) (graph.Iterator, bool) {
	return it, false // everything done is shapes
}

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
	Collection string        // name of the collection
	Filters    []FieldFilter // filters to select documents
	Limit      int64         // limits a number of documents
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

func toFieldFilter(c shape.Comparison) ([]FieldFilter, bool) {
	var op FilterOp
	switch c.Op {
	case iterator.CompareGT:
		op = GT
	case iterator.CompareGTE:
		op = GTE
	case iterator.CompareLT:
		op = LT
	case iterator.CompareLTE:
		op = LTE
	default:
		return nil, false
	}
	fieldPath := func(s string) []string {
		return []string{fldValue, s}
	}

	var filters []FieldFilter
	switch v := c.Val.(type) {
	case quad.String:
		filters = []FieldFilter{
			{Path: fieldPath(fldValData), Filter: op, Value: String(v)},
			{Path: fieldPath(fldIRI), Filter: NotEqual, Value: Bool(true)},
			{Path: fieldPath(fldBNode), Filter: NotEqual, Value: Bool(true)},
			{Path: fieldPath(fldRaw), Filter: NotEqual, Value: Bool(true)},
		}
	case quad.IRI:
		filters = []FieldFilter{
			{Path: fieldPath(fldValData), Filter: op, Value: String(v)},
			{Path: fieldPath(fldIRI), Filter: Equal, Value: Bool(true)},
		}
	case quad.BNode:
		filters = []FieldFilter{
			{Path: fieldPath(fldValData), Filter: op, Value: String(v)},
			{Path: fieldPath(fldBNode), Filter: Equal, Value: Bool(true)},
		}
	case quad.Int:
		filters = []FieldFilter{
			{Path: fieldPath(fldValInt), Filter: op, Value: Int(v)},
		}
	case quad.Float:
		filters = []FieldFilter{
			{Path: fieldPath(fldValFloat), Filter: op, Value: Float(v)},
		}
	case quad.Time:
		filters = []FieldFilter{
			{Path: fieldPath(fldValTime), Filter: op, Value: Time(v)},
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
		filters []FieldFilter
		left    []shape.ValueFilter
	)
	for _, f := range s.Filters {
		switch f := f.(type) {
		case shape.Comparison:
			if fld, ok := toFieldFilter(f); ok {
				filters = append(filters, fld...)
				continue
			}
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
