package iterator

import (
	"context"
	"fmt"

	"github.com/cayleygraph/cayley/graph"
)

var _ graph.Iterator = &Limit{}

// Limit iterator will stop iterating if certain a number of values were encountered.
// Zero and negative limit values means no limit.
type Limit struct {
	limit     int64
	count     int64
	primaryIt graph.Iterator
}

func NewLimit(primaryIt graph.Iterator, limit int64) *Limit {
	return &Limit{
		limit:     limit,
		primaryIt: primaryIt,
	}
}

// Reset resets the internal iterators and the iterator itself.
func (it *Limit) Reset() {
	it.count = 0
	it.primaryIt.Reset()
}

func (it *Limit) TagResults(dst map[string]graph.Ref) {
	it.primaryIt.TagResults(dst)
}

// SubIterators returns a slice of the sub iterators.
func (it *Limit) SubIterators() []graph.Iterator {
	return []graph.Iterator{it.primaryIt}
}

// Next advances the Limit iterator. It will stop iteration if limit was reached.
func (it *Limit) Next(ctx context.Context) bool {
	if it.limit > 0 && it.count >= it.limit {
		return false
	}
	if it.primaryIt.Next(ctx) {
		it.count++
		return true
	}
	return false
}

func (it *Limit) Err() error {
	return it.primaryIt.Err()
}

func (it *Limit) Result() graph.Ref {
	return it.primaryIt.Result()
}

func (it *Limit) Contains(ctx context.Context, val graph.Ref) bool {
	return it.primaryIt.Contains(ctx, val) // FIXME(dennwc): limit is ignored in this case
}

// NextPath checks whether there is another path. Will call primary iterator
// if limit is not reached yet.
func (it *Limit) NextPath(ctx context.Context) bool {
	if it.limit > 0 && it.count >= it.limit {
		return false
	}
	if it.primaryIt.NextPath(ctx) {
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

func (it *Limit) String() string {
	return fmt.Sprintf("Limit(%d)", it.limit)
}
