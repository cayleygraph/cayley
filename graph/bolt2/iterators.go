package bolt2

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
)

func (qs *QuadStore) NodesAllIterator() graph.Iterator {
	panic("todo: nodesalliterator")
}

func (qs *QuadStore) QuadsAllIterator() graph.Iterator {
	panic("todo: quadsalliterator")
}

func (qs *QuadStore) FixedIterator() graph.FixedIterator {
	return iterator.NewFixed(iterator.Identity)
}
