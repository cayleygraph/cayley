package iterator

import (
	"context"

	"github.com/cayleygraph/cayley/graph/refs"
	"github.com/cayleygraph/quad"
)

// Count iterator returns one element with size of underlying iterator.
type Count struct {
	it Shape
	qs refs.Namer
}

// NewCount creates a new iterator to count a number of results from a provided subiterator.
// qs may be nil - it's used to check if count Contains (is) a given value.
func NewCount(it Shape, qs refs.Namer) *Count {
	return &Count{
		it: it, qs: qs,
	}
}

func (it *Count) Iterate() Scanner {
	return newCountNext(it.it)
}

func (it *Count) Lookup() Index {
	return newCountContains(it.it, it.qs)
}

// SubIterators returns a slice of the sub iterators.
func (it *Count) SubIterators() []Shape {
	return []Shape{it.it}
}

func (it *Count) Optimize(ctx context.Context) (Shape, bool) {
	sub, optimized := it.it.Optimize(ctx)
	it.it = sub
	return it, optimized
}

func (it *Count) Stats(ctx context.Context) (Costs, error) {
	stats := Costs{
		NextCost: 1,
		Size: refs.Size{
			Value: 1,
			Exact: true,
		},
	}
	if sub, err := it.it.Stats(ctx); err == nil && !sub.Size.Exact {
		stats.NextCost = sub.NextCost * sub.Size.Value
	}
	stats.ContainsCost = stats.NextCost
	return stats, nil
}

func (it *Count) String() string { return "Count" }

// Count iterator returns one element with size of underlying iterator.
type countNext struct {
	it     Shape
	done   bool
	result quad.Value
	err    error
}

// NewCount creates a new iterator to count a number of results from a provided subiterator.
// qs may be nil - it's used to check if count Contains (is) a given value.
func newCountNext(it Shape) *countNext {
	return &countNext{
		it: it,
	}
}

func (it *countNext) TagResults(dst map[string]refs.Ref) {}

// Next counts a number of results in underlying iterator.
func (it *countNext) Next(ctx context.Context) bool {
	if it.done {
		return false
	}
	// TODO(dennwc): this most likely won't include the NextPath
	st, err := it.it.Stats(ctx)
	if err != nil {
		it.err = err
		return false
	}
	if !st.Size.Exact {
		sit := it.it.Iterate()
		defer sit.Close()
		for st.Size.Value = 0; sit.Next(ctx); st.Size.Value++ {
			// TODO(dennwc): it's unclear if we should call it here or not
			for ; sit.NextPath(ctx); st.Size.Value++ {
			}
		}
		it.err = sit.Err()
	}
	it.result = quad.Int(st.Size.Value)
	it.done = true
	return true
}

func (it *countNext) Err() error {
	return it.err
}

func (it *countNext) Result() refs.Ref {
	if it.result == nil {
		return nil
	}
	return refs.PreFetched(it.result)
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
	qs refs.Namer
}

// NewCount creates a new iterator to count a number of results from a provided subiterator.
// qs may be nil - it's used to check if count Contains (is) a given value.
func newCountContains(it Shape, qs refs.Namer) *countContains {
	return &countContains{
		it: newCountNext(it),
		qs: qs,
	}
}

func (it *countContains) TagResults(dst map[string]refs.Ref) {}

func (it *countContains) Err() error {
	return it.it.Err()
}

func (it *countContains) Result() refs.Ref {
	return it.it.Result()
}

func (it *countContains) Contains(ctx context.Context, val refs.Ref) bool {
	if !it.it.done {
		it.it.Next(ctx)
	}
	if v, ok := val.(refs.PreFetchedValue); ok {
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
