package iterator

import (
	"context"
	"fmt"

	"github.com/cayleygraph/cayley/graph"
)

var _ graph.Iterator = &Skip{}

// Skip iterator will skip certain number of values from primary iterator.
type Skip struct {
	skip      int64
	skipped   int64
	primaryIt graph.Iterator
}

func NewSkip(primaryIt graph.Iterator, skip int64) *Skip {
	return &Skip{
		skip:      skip,
		primaryIt: primaryIt,
	}
}

// Reset resets the internal iterators and the iterator itself.
func (it *Skip) Reset() {
	it.skipped = 0
	it.primaryIt.Reset()
}

func (it *Skip) TagResults(dst map[string]graph.Ref) {
	it.primaryIt.TagResults(dst)
}

// SubIterators returns a slice of the sub iterators.
func (it *Skip) SubIterators() []graph.Iterator {
	return []graph.Iterator{it.primaryIt}
}

// Next advances the Skip iterator. It will skip all initial values
// before returning actual result.
func (it *Skip) Next(ctx context.Context) bool {
	for ; it.skipped < it.skip; it.skipped++ {
		if !it.primaryIt.Next(ctx) {
			return false
		}
	}
	if it.primaryIt.Next(ctx) {
		return true
	}
	return false
}

func (it *Skip) Err() error {
	return it.primaryIt.Err()
}

func (it *Skip) Result() graph.Ref {
	return it.primaryIt.Result()
}

func (it *Skip) Contains(ctx context.Context, val graph.Ref) bool {
	return it.primaryIt.Contains(ctx, val) // FIXME(dennwc): will not skip anything in this case
}

// NextPath checks whether there is another path. It will skip first paths
// according to iterator parameter.
func (it *Skip) NextPath(ctx context.Context) bool {
	for ; it.skipped < it.skip; it.skipped++ {
		if !it.primaryIt.NextPath(ctx) {
			return false
		}
	}
	return it.primaryIt.NextPath(ctx)
}

// Close closes the primary and all iterators.  It closes all subiterators
// it can, but returns the first error it encounters.
func (it *Skip) Close() error {
	return it.primaryIt.Close()
}

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

func (it *Skip) String() string {
	return fmt.Sprintf("Skip(%d)", it.skip)
}
