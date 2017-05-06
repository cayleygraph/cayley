package shape

import (
	"context"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/quad"
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
	nodes := Save{
		From: AllNodes{},
		Tags: []string{tag},
	}
	start, goal := quad.Subject, quad.Object
	if rev {
		start, goal = goal, start
	}

	var save Shape = NodesFrom{
		Quads: Quads{
			{Dir: goal, Values: nodes},
			{Dir: quad.Predicate, Values: via},
		},
		Dir: start,
	}
	if opt {
		save = Optional{save}
	}
	return IntersectShapes(from, save)
}

func Has(from, via, nodes Shape, rev bool) Shape {
	start, goal := quad.Subject, quad.Object
	if rev {
		start, goal = goal, start
	}

	quads := make(Quads, 0, 2)
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
	if len(quads) == 0 {
		panic("empty has")
	}
	return IntersectShapes(from, NodesFrom{
		Quads: quads, Dir: start,
	})
}

func Iterate(ctx context.Context, qs graph.QuadStore, s Shape) *graph.IterateChain {
	it := BuildIterator(qs, s)
	return graph.Iterate(ctx, it).On(qs)
}
