package iterator

import (
	"context"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/quad"
)

var _ graph.Iterator = &Count{}

// Count iterator returns one element with size of underlying iterator.
type Count struct {
	it     graph.Iterator
	done   bool
	result quad.Value
	qs     graph.Namer
}

// NewCount creates a new iterator to count a number of results from a provided subiterator.
// qs may be nil - it's used to check if count Contains (is) a given value.
func NewCount(it graph.Iterator, qs graph.Namer) *Count {
	return &Count{
		it: it, qs: qs,
	}
}

// Reset resets the internal iterators and the iterator itself.
func (it *Count) Reset() {
	it.done = false
	it.result = nil
	it.it.Reset()
}

func (it *Count) TagResults(dst map[string]graph.Ref) {}

// SubIterators returns a slice of the sub iterators.
func (it *Count) SubIterators() []graph.Iterator {
	return []graph.Iterator{it.it}
}

// Next counts a number of results in underlying iterator.
func (it *Count) Next(ctx context.Context) bool {
	if it.done {
		return false
	}
	size, exact := it.it.Size()
	if !exact {
		for size = 0; it.it.Next(ctx); size++ {
			for ; it.it.NextPath(ctx); size++ {
			}
		}
	}
	it.result = quad.Int(size)
	it.done = true
	return true
}

func (it *Count) Err() error {
	return it.it.Err()
}

func (it *Count) Result() graph.Ref {
	if it.result == nil {
		return nil
	}
	return graph.PreFetched(it.result)
}

func (it *Count) Contains(ctx context.Context, val graph.Ref) bool {
	if !it.done {
		it.Next(ctx)
	}
	if v, ok := val.(graph.PreFetchedValue); ok {
		return v.NameOf() == it.result
	}
	if it.qs != nil {
		return it.qs.NameOf(val) == it.result
	}
	return false
}

func (it *Count) NextPath(ctx context.Context) bool {
	return false
}

func (it *Count) Close() error {
	return it.it.Close()
}

func (it *Count) Optimize() (graph.Iterator, bool) {
	sub, optimized := it.it.Optimize()
	it.it = sub
	return it, optimized
}

func (it *Count) Stats() graph.IteratorStats {
	stats := graph.IteratorStats{
		NextCost:  1,
		Size:      1,
		ExactSize: true,
	}
	if sub := it.it.Stats(); !sub.ExactSize {
		stats.NextCost = sub.NextCost * sub.Size
	}
	stats.ContainsCost = stats.NextCost
	return stats
}

func (it *Count) Size() (int64, bool) {
	return 1, true
}

func (it *Count) String() string { return "Count" }
