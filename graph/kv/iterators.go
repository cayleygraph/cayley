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

	qs.indexes.RLock()
	all := qs.indexes.all
	qs.indexes.RUnlock()
	for _, ind := range all {
		if len(ind.Dirs) == 1 && ind.Dirs[0] == dir {
			return NewQuadIterator(qs, ind, []uint64{uint64(vi)})
		}
	}
	return NewAllIterator(false, qs, &constraint{
		dir: dir,
		val: vi,
	})
}
