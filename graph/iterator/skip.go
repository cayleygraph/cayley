package iterator

import (
	"github.com/cayleygraph/cayley/graph"
)

// Skip iterator will skip certain number of values from primary iterator.
type Skip struct {
	uid       uint64
	skip      int64
	skipped   int64
	primaryIt graph.Iterator
}

func NewSkip(primaryIt graph.Iterator, skip int64) *Skip {
	return &Skip{
		uid:       NextUID(),
		skip:      skip,
		primaryIt: primaryIt,
	}
}

func (it *Skip) UID() uint64 {
	return it.uid
}

// Reset resets the internal iterators and the iterator itself.
func (it *Skip) Reset() {
	it.skipped = 0
	it.primaryIt.Reset()
}

func (it *Skip) Tagger() *graph.Tagger {
	return it.primaryIt.Tagger()
}

func (it *Skip) TagResults(dst map[string]graph.Value) {
	it.primaryIt.TagResults(dst)
}

func (it *Skip) Clone() graph.Iterator {
	return NewSkip(it.primaryIt.Clone(), it.skip)
}

// SubIterators returns a slice of the sub iterators.
func (it *Skip) SubIterators() []graph.Iterator {
	return []graph.Iterator{it.primaryIt}
}

// Next advances the Skip iterator. It will skip all initial values
// before returning actual result.
func (it *Skip) Next() bool {
	graph.NextLogIn(it)
	for ; it.skipped < it.skip; it.skipped++ {
		if !it.primaryIt.Next() {
			return graph.NextLogOut(it, false)
		}
	}
	if it.primaryIt.Next() {
		return graph.NextLogOut(it, true)
	}
	return graph.NextLogOut(it, false)
}

func (it *Skip) Err() error {
	return it.primaryIt.Err()
}

func (it *Skip) Result() graph.Value {
	return it.primaryIt.Result()
}

func (it *Skip) Contains(val graph.Value) bool {
	return it.primaryIt.Contains(val) // FIXME(dennwc): will not skip anything in this case
}

// NextPath checks whether there is another path. It will skip first paths
// according to iterator parameter.
func (it *Skip) NextPath() bool {
	for ; it.skipped < it.skip; it.skipped++ {
		if !it.primaryIt.NextPath() {
			return false
		}
	}
	return it.primaryIt.NextPath()
}

// Close closes the primary and all iterators.  It closes all subiterators
// it can, but returns the first error it encounters.
func (it *Skip) Close() error {
	return it.primaryIt.Close()
}

func (it *Skip) Type() graph.Type { return graph.Skip }

func (it *Skip) Optimize() (graph.Iterator, bool) {
	optimizedPrimaryIt, optimized := it.primaryIt.Optimize()
	if it.skip == 0 { // nothing to skip
		return optimizedPrimaryIt, true
	}
	it.primaryIt = optimizedPrimaryIt
	return it, optimized
}

func (it *Skip) Stats() graph.IteratorStats {
	primaryStats := it.primaryIt.Stats()
	primaryStats.Size -= it.skip
	if primaryStats.Size < 0 {
		primaryStats.Size = 0
	}
	return primaryStats
}

func (it *Skip) Size() (int64, bool) {
	primarySize, exact := it.primaryIt.Size()
	if exact {
		primarySize -= it.skip
		if primarySize < 0 {
			primarySize = 0
		}
	}
	return primarySize, exact
}

func (it *Skip) Describe() graph.Description {
	subIts := []graph.Description{
		it.primaryIt.Describe(),
	}

	return graph.Description{
		UID:       it.UID(),
		Type:      it.Type(),
		Tags:      it.Tagger().Tags(),
		Iterators: subIts,
	}
}

var _ graph.Iterator = &Skip{}
