package kv

import (
	"fmt"

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
	if v == nil {
		return iterator.NewNull()
	}
	vi, ok := v.(Int64Value)
	if !ok {
		return iterator.NewError(fmt.Errorf("unexpected node type: %T", v))
	}
	if dir == quad.Subject || dir == quad.Object {
		return NewQuadIterator(dir, vi, qs)
	}
	return NewAllIterator(false, qs, &constraint{
		dir: dir,
		val: vi,
	})
}
