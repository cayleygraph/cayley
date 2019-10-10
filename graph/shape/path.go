package shape

import (
	"context"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/quad"
)

func IntersectShapes(s1, s2 Shape) Shape {
	switch s1 := s1.(type) {
	case AllNodes:
		return s2
	case Intersect:
		if s2, ok := s2.(Intersect); ok {
			return append(s1, s2...)
		}
		return append(s1, s2)
	}
	return Intersect{s1, s2}
}

func IntersectOptional(s, opt Shape) Shape {
	var optional []Shape
	switch opt := opt.(type) {
	case Intersect:
		optional = []Shape(opt)
	case IntersectOpt:
		optional = make([]Shape, 0, len(opt.Sub)+len(opt.Opt))
		optional = append(optional, opt.Sub...)
		optional = append(optional, opt.Opt...)
	default:
		optional = []Shape{opt}
	}
	if len(optional) == 0 {
		return s
	}
	switch s := s.(type) {
	case Intersect:
		return IntersectOpt{Sub: s, Opt: optional}
	case IntersectOpt:
		s.Opt = append(s.Opt, optional...)
		return s
	}
	return IntersectOpt{Sub: Intersect{s}, Opt: optional}
}

func UnionShapes(s1, s2 Shape) Union {
	if s1, ok := s1.(Union); ok {
		if s2, ok := s2.(Union); ok {
			return append(s1, s2...)
		}
		return append(s1, s2)
	}
	return Union{s1, s2}
}

func buildOut(from, via, labels Shape, tags []string, in bool) Shape {
	start, goal := quad.Subject, quad.Object
	if in {
		start, goal = goal, start
	}
	if len(tags) != 0 {
		via = Save{From: via, Tags: tags}
	}

	quads := make(Quads, 0, 3)
	if _, ok := from.(AllNodes); !ok {
		quads = append(quads, QuadFilter{
			Dir: start, Values: from,
		})
	}
	if _, ok := via.(AllNodes); !ok {
		quads = append(quads, QuadFilter{
			Dir: quad.Predicate, Values: via,
		})
	}
	if labels != nil {
		if _, ok := labels.(AllNodes); !ok {
			quads = append(quads, QuadFilter{
				Dir: quad.Label, Values: labels,
			})
		}
	}
	return NodesFrom{Quads: quads, Dir: goal}
}

func Out(from, via, labels Shape, tags ...string) Shape {
	return buildOut(from, via, labels, tags, false)
}

func In(from, via, labels Shape, tags ...string) Shape {
	return buildOut(from, via, labels, tags, true)
}

// InWithTags, OutWithTags, Both, BothWithTags

func Predicates(from Shape, in bool) Shape {
	dir := quad.Subject
	if in {
		dir = quad.Object
	}
	return Unique{NodesFrom{
		Quads: Quads{
			{Dir: dir, Values: from},
		},
		Dir: quad.Predicate,
	}}
}

func SavePredicates(from Shape, in bool, tag string) Shape {
	preds := Save{
		From: AllNodes{},
		Tags: []string{tag},
	}
	start := quad.Subject
	if in {
		start = quad.Object
	}

	var save Shape = NodesFrom{
		Quads: Quads{
			{Dir: quad.Predicate, Values: preds},
		},
		Dir: start,
	}
	return IntersectShapes(from, save)
}

func Labels(from Shape) Shape {
	return Unique{NodesFrom{
		Quads: Union{
			Quads{
				{Dir: quad.Subject, Values: from},
			},
			Quads{
				{Dir: quad.Object, Values: from},
			},
		},
		Dir: quad.Label,
	}}
}

func SaveVia(from, via Shape, tag string, rev, opt bool) Shape {
	return SaveViaLabels(from, via, AllNodes{}, tag, rev, opt)
}

func SaveViaLabels(from, via, labels Shape, tag string, rev, opt bool) Shape {
	nodes := Save{
		From: AllNodes{},
		Tags: []string{tag},
	}
	start, goal := quad.Subject, quad.Object
	if rev {
		start, goal = goal, start
	}

	quads := Quads{
		{Dir: goal, Values: nodes},
		{Dir: quad.Predicate, Values: via},
	}
	if labels != nil {
		if _, ok := labels.(AllNodes); !ok {
			quads = append(quads, QuadFilter{
				Dir: quad.Label, Values: labels,
			})
		}
	}

	var save Shape = NodesFrom{
		Quads: quads,
		Dir:   start,
	}
	if opt {
		return IntersectOptional(from, save)
	}
	return IntersectShapes(from, save)
}

func Has(from, via, nodes Shape, rev bool) Shape {
	return HasLabels(from, via, AllNodes{}, nodes, rev)
}

func HasLabels(from, via, nodes, labels Shape, rev bool) Shape {
	start, goal := quad.Subject, quad.Object
	if rev {
		start, goal = goal, start
	}

	quads := make(Quads, 0, 3)
	if _, ok := nodes.(AllNodes); !ok {
		quads = append(quads, QuadFilter{
			Dir: goal, Values: nodes,
		})
	}
	if _, ok := via.(AllNodes); !ok {
		quads = append(quads, QuadFilter{
			Dir: quad.Predicate, Values: via,
		})
	}
	if labels != nil {
		if _, ok := labels.(AllNodes); !ok {
			quads = append(quads, QuadFilter{
				Dir: quad.Label, Values: labels,
			})
		}
	}
	if len(quads) == 0 {
		panic("empty has")
	}
	return IntersectShapes(from, NodesFrom{
		Quads: quads, Dir: start,
	})
}

func AddFilters(nodes Shape, filters ...ValueFilter) Shape {
	if len(filters) == 0 {
		return nodes
	}
	if s, ok := nodes.(Filter); ok {
		arr := make([]ValueFilter, 0, len(s.Filters)+len(filters))
		arr = append(arr, s.Filters...)
		arr = append(arr, filters...)
		return Filter{From: s.From, Filters: arr}
	}
	if nodes == nil {
		nodes = AllNodes{}
	}
	return Filter{
		From:    nodes,
		Filters: filters,
	}
}

func Compare(nodes Shape, op iterator.Operator, v quad.Value) Shape {
	return AddFilters(nodes, Comparison{Op: op, Val: v})
}

func Iterate(ctx context.Context, qs graph.QuadStore, s Shape) *graph.IterateChain {
	it := BuildIterator(qs, s)
	return graph.Iterate(ctx, it).On(qs)
}