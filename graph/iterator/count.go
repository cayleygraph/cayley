package iterator

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/quad"
)

var _ graph.Iterator = &Count{}

// Count iterator returns one element with size of underlying iterator.
type Count struct {
	uid    uint64
	it     graph.Iterator
	done   bool
	tags   graph.Tagger
	result quad.Value
	qs     graph.QuadStore
}

// NewCount creates a new iterator to count a number of results from a provided subiterator.
// qs may be nil - it's used to check if count Contains (is) a given value.
func NewCount(it graph.Iterator, qs graph.QuadStore) *Count {
	return &Count{
		uid: NextUID(),
		it:  it, qs: qs,
	}
}

func (it *Count) UID() uint64 {
	return it.uid
}

// Reset resets the internal iterators and the iterator itself.
func (it *Count) Reset() {
	it.done = false
	it.result = nil
	it.it.Reset()
}

func (it *Count) Tagger() *graph.Tagger {
	return &it.tags
}

func (it *Count) TagResults(dst map[string]graph.Value) {
	it.tags.TagResult(dst, it.Result())
}

func (it *Count) Clone() graph.Iterator {
	it2 := NewCount(it.it.Clone(), it.qs)
	it2.Tagger().CopyFrom(it)
	return it2
}

// SubIterators returns a slice of the sub iterators.
func (it *Count) SubIterators() []graph.Iterator {
	return []graph.Iterator{it.it}
}

// Next counts a number of results in underlying iterator.
func (it *Count) Next() bool {
	if it.done {
		return false
	}
	size, exact := it.it.Size()
	if !exact {
		for size = 0; it.it.Next(); size++ {
			for ; it.it.NextPath(); size++ {
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

func (it *Count) Result() graph.Value {
	if it.result == nil {
		return nil
	}
	return graph.PreFetched(it.result)
}

func (it *Count) Contains(val graph.Value) bool {
	if !it.done {
		it.Next()
	}
	if v, ok := val.(graph.PreFetchedValue); ok {
		return v.NameOf() == it.result
	}
	if it.qs != nil {
		return it.qs.NameOf(val) == it.result
	}
	return false
}

func (it *Count) NextPath() bool {
	return false
}

func (it *Count) Close() error {
	return it.it.Close()
}

func (it *Count) Type() graph.Type { return graph.Count }

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
