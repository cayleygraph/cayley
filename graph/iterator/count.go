package iterator

import (
	"context"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/quad"
)

var _ graph.IteratorFuture = &Count{}

// Count iterator returns one element with size of underlying iterator.
type Count struct {
	it *count
	graph.Iterator
}

// NewCount creates a new iterator to count a number of results from a provided subiterator.
// qs may be nil - it's used to check if count Contains (is) a given value.
func NewCount(sub graph.Iterator, qs graph.Namer) *Count {
	it := &Count{
		it: newCount(graph.As2(sub), qs),
	}
	it.Iterator = graph.NewLegacy(it.it)
	return it
}

func (it *Count) As2() graph.Iterator2 {
	it.Close()
	return it.it
}

var _ graph.Iterator2Compat = &count{}

// Count iterator returns one element with size of underlying iterator.
type count struct {
	it graph.Iterator2
	qs graph.Namer
}

// NewCount creates a new iterator to count a number of results from a provided subiterator.
// qs may be nil - it's used to check if count Contains (is) a given value.
func newCount(it graph.Iterator2, qs graph.Namer) *count {
	return &count{
		it: it, qs: qs,
	}
}

func (it *count) Iterate() graph.Iterator2Next {
	return newCountNext(it.it)
}

func (it *count) Lookup() graph.Iterator2Contains {
	return newCountContains(it.it, it.qs)
}

func (it *count) AsLegacy() graph.Iterator {
	it2 := &Count{it: it}
	it2.Iterator = graph.NewLegacy(it)
	return it2
}

// SubIterators returns a slice of the sub iterators.
func (it *count) SubIterators() []graph.Iterator2 {
	return []graph.Iterator2{it.it}
}

func (it *count) Optimize() (graph.Iterator2, bool) {
	sub, optimized := it.it.Optimize()
	it.it = sub
	return it, optimized
}

func (it *count) Stats() graph.IteratorStats {
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

func (it *count) Size() (int64, bool) {
	return 1, true
}

func (it *count) String() string { return "Count" }

// Count iterator returns one element with size of underlying iterator.
type countNext struct {
	it     graph.Iterator2
	done   bool
	result quad.Value
	err    error
}

// NewCount creates a new iterator to count a number of results from a provided subiterator.
// qs may be nil - it's used to check if count Contains (is) a given value.
func newCountNext(it graph.Iterator2) *countNext {
	return &countNext{
		it: it,
	}
}

func (it *countNext) TagResults(dst map[string]graph.Ref) {}

// Next counts a number of results in underlying iterator.
func (it *countNext) Next(ctx context.Context) bool {
	if it.done {
		return false
	}
	// TODO(dennwc): this most likely won't include the NextPath
	size, exact := it.it.Size()
	if !exact {
		sit := it.it.Iterate()
		defer sit.Close()
		for size = 0; sit.Next(ctx); size++ {
			// TODO(dennwc): it's unclear if we should call it here or not
			for ; sit.NextPath(ctx); size++ {
			}
		}
		it.err = sit.Err()
	}
	it.result = quad.Int(size)
	it.done = true
	return true
}

func (it *countNext) Err() error {
	return it.err
}

func (it *countNext) Result() graph.Ref {
	if it.result == nil {
		return nil
	}
	return graph.PreFetched(it.result)
}

func (it *countNext) NextPath(ctx context.Context) bool {
	return false
}

func (it *countNext) Close() error {
	return nil
}

func (it *countNext) String() string { return "CountNext" }

// Count iterator returns one element with size of underlying iterator.
type countContains struct {
	it *countNext
	qs graph.Namer
}

// NewCount creates a new iterator to count a number of results from a provided subiterator.
// qs may be nil - it's used to check if count Contains (is) a given value.
func newCountContains(it graph.Iterator2, qs graph.Namer) *countContains {
	return &countContains{
		it: newCountNext(it),
		qs: qs,
	}
}

func (it *countContains) TagResults(dst map[string]graph.Ref) {}

func (it *countContains) Err() error {
	return it.it.Err()
}

func (it *countContains) Result() graph.Ref {
	return it.it.Result()
}

func (it *countContains) Contains(ctx context.Context, val graph.Ref) bool {
	if !it.it.done {
		it.it.Next(ctx)
	}
	if v, ok := val.(graph.PreFetchedValue); ok {
		return v.NameOf() == it.it.result
	}
	if it.qs != nil {
		return it.qs.NameOf(val) == it.it.result
	}
	return false
}

func (it *countContains) NextPath(ctx context.Context) bool {
	return false
}

func (it *countContains) Close() error {
	return it.it.Close()
}

func (it *countContains) String() string { return "CountContains" }
