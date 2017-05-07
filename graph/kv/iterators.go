package kv

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/quad"
)

func (qs *QuadStore) NodesAllIterator() graph.Iterator {
	return NewAllIterator(true, qs, nil)
}

func (qs *QuadStore) QuadsAllIterator() graph.Iterator {
	return NewAllIterator(false, qs, nil)
}

func (qs *QuadStore) FixedIterator() graph.FixedIterator {
	return iterator.NewFixed(iterator.Identity)
}

func (qs *QuadStore) QuadIterator(dir quad.Direction, v graph.Value) graph.Iterator {
	if dir == quad.Subject || dir == quad.Object {
		return NewQuadIterator(dir, v.(Int64Value), qs)
	}
	cons := constraint{
		dir: dir,
		val: v.(Int64Value),
	}
	return NewAllIterator(false, qs, &cons)
}
