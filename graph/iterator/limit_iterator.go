package iterator

import (
	"github.com/cayleygraph/cayley/graph"
)

// Limit iterator will stop iterating if certain a number of values were encountered.
// Zero and negative limit values means no limit.
type Limit struct {
	uid       uint64
	limit     int64
	count     int64
	primaryIt graph.Iterator
}

func NewLimit(primaryIt graph.Iterator, limit int64) *Limit {
	return &Limit{
		uid:       NextUID(),
		limit:     limit,
		primaryIt: primaryIt,
	}
}

func (it *Limit) UID() uint64 {
	return it.uid
}

// Reset resets the internal iterators and the iterator itself.
func (it *Limit) Reset() {
	it.count = 0
	it.primaryIt.Reset()
}

func (it *Limit) Tagger() *graph.Tagger {
	return it.primaryIt.Tagger()
}

func (it *Limit) TagResults(dst map[string]graph.Value) {
	it.primaryIt.TagResults(dst)
}

func (it *Limit) Clone() graph.Iterator {
	return NewLimit(it.primaryIt.Clone(), it.limit)
}

// SubIterators returns a slice of the sub iterators.
func (it *Limit) SubIterators() []graph.Iterator {
	return []graph.Iterator{it.primaryIt}
}

// Next advances the Limit iterator. It will stop iteration if limit was reached.
func (it *Limit) Next() bool {
	graph.NextLogIn(it)
	if it.limit > 0 && it.count >= it.limit {
		return graph.NextLogOut(it, false)
	}
	if it.primaryIt.Next() {
		it.count++
		return graph.NextLogOut(it, true)
	}
	return graph.NextLogOut(it, false)
}

func (it *Limit) Err() error {
	return it.primaryIt.Err()
}

func (it *Limit) Result() graph.Value {
	return it.primaryIt.Result()
}

func (it *Limit) Contains(val graph.Value) bool {
	return it.primaryIt.Contains(val) // FIXME(dennwc): limit is ignored in this case
}

// NextPath checks whether there is another path. Will call primary iterator
// if limit is not reached yet.
func (it *Limit) NextPath() bool {
	if it.limit > 0 && it.count >= it.limit {
		return false
	}
	if it.primaryIt.NextPath() {
		it.count++
		return true
	}
	return false
}

// Close closes the primary and all iterators.  It closes all subiterators
// it can, but returns the first error it encounters.
func (it *Limit) Close() error {
	return it.primaryIt.Close()
}

func (it *Limit) Type() graph.Type { return graph.Limit }

func (it *Limit) Optimize() (graph.Iterator, bool) {
	optimizedPrimaryIt, optimized := it.primaryIt.Optimize()
	if it.limit <= 0 { // no limit
		return optimizedPrimaryIt, true
	}
	it.primaryIt = optimizedPrimaryIt
	return it, optimized
}

func (it *Limit) Stats() graph.IteratorStats {
	primaryStats := it.primaryIt.Stats()
	if it.limit > 0 && primaryStats.Size > it.limit {
		primaryStats.Size = it.limit
	}
	return primaryStats
}

func (it *Limit) Size() (int64, bool) {
	primarySize, exact := it.primaryIt.Size()
	if it.limit > 0 && primarySize > it.limit {
		primarySize = it.limit
	}
	return primarySize, exact
}

func (it *Limit) Describe() graph.Description {
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

var _ graph.Iterator = &Limit{}
