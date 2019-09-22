package iterator

import (
	"context"
	"fmt"

	"github.com/cayleygraph/cayley/graph"
)

var _ graph.IteratorFuture = &Skip{}

// Skip iterator will skip certain number of values from primary iterator.
type Skip struct {
	it *skip
	graph.Iterator
}

func NewSkip(primaryIt graph.Iterator, skip int64) *Skip {
	it := &Skip{
		it: newSkip(graph.AsShape(primaryIt), skip),
	}
	it.Iterator = graph.NewLegacy(it.it, it)
	return it
}

func (it *Skip) AsShape() graph.IteratorShape {
	it.Close()
	return it.it
}

var _ graph.IteratorShapeCompat = &skip{}

// Skip iterator will skip certain number of values from primary iterator.
type skip struct {
	skip      int64
	primaryIt graph.IteratorShape
}

func newSkip(primaryIt graph.IteratorShape, off int64) *skip {
	return &skip{
		skip:      off,
		primaryIt: primaryIt,
	}
}

func (it *skip) Iterate() graph.Scanner {
	return newSkipNext(it.primaryIt.Iterate(), it.skip)
}

func (it *skip) Lookup() graph.Index {
	return newSkipContains(it.primaryIt.Lookup(), it.skip)
}

func (it *skip) AsLegacy() graph.Iterator {
	it2 := &Skip{it: it}
	it2.Iterator = graph.NewLegacy(it, it2)
	return it2
}

// SubIterators returns a slice of the sub iterators.
func (it *skip) SubIterators() []graph.IteratorShape {
	return []graph.IteratorShape{it.primaryIt}
}

func (it *skip) Optimize(ctx context.Context) (graph.IteratorShape, bool) {
	optimizedPrimaryIt, optimized := it.primaryIt.Optimize(ctx)
	if it.skip == 0 { // nothing to skip
		return optimizedPrimaryIt, true
	}
	it.primaryIt = optimizedPrimaryIt
	return it, optimized
}

func (it *skip) Stats(ctx context.Context) (graph.IteratorCosts, error) {
	primaryStats, err := it.primaryIt.Stats(ctx)
	if primaryStats.Size.Exact {
		primaryStats.Size.Size -= it.skip
		if primaryStats.Size.Size < 0 {
			primaryStats.Size.Size = 0
		}
	}
	return primaryStats, err
}

func (it *skip) String() string {
	return fmt.Sprintf("Skip(%d)", it.skip)
}

// Skip iterator will skip certain number of values from primary iterator.
type skipNext struct {
	skip      int64
	skipped   int64
	primaryIt graph.Scanner
}

func newSkipNext(primaryIt graph.Scanner, skip int64) *skipNext {
	return &skipNext{
		skip:      skip,
		primaryIt: primaryIt,
	}
}

func (it *skipNext) TagResults(dst map[string]graph.Ref) {
	it.primaryIt.TagResults(dst)
}

// Next advances the Skip iterator. It will skip all initial values
// before returning actual result.
func (it *skipNext) Next(ctx context.Context) bool {
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

func (it *skipNext) Err() error {
	return it.primaryIt.Err()
}

func (it *skipNext) Result() graph.Ref {
	return it.primaryIt.Result()
}

// NextPath checks whether there is another path. It will skip first paths
// according to iterator parameter.
func (it *skipNext) NextPath(ctx context.Context) bool {
	for ; it.skipped < it.skip; it.skipped++ {
		if !it.primaryIt.NextPath(ctx) {
			return false
		}
	}
	return it.primaryIt.NextPath(ctx)
}

// Close closes the primary and all iterators.  It closes all subiterators
// it can, but returns the first error it encounters.
func (it *skipNext) Close() error {
	return it.primaryIt.Close()
}

func (it *skipNext) String() string {
	return fmt.Sprintf("SkipNext(%d)", it.skip)
}

// Skip iterator will skip certain number of values from primary iterator.
type skipContains struct {
	skip      int64
	skipped   int64
	primaryIt graph.Index
}

func newSkipContains(primaryIt graph.Index, skip int64) *skipContains {
	return &skipContains{
		skip:      skip,
		primaryIt: primaryIt,
	}
}

func (it *skipContains) TagResults(dst map[string]graph.Ref) {
	it.primaryIt.TagResults(dst)
}

func (it *skipContains) Err() error {
	return it.primaryIt.Err()
}

func (it *skipContains) Result() graph.Ref {
	return it.primaryIt.Result()
}

func (it *skipContains) Contains(ctx context.Context, val graph.Ref) bool {
	inNextPath := false
	for it.skipped <= it.skip {
		// skipping main iterator results
		inNextPath = false
		if !it.primaryIt.Contains(ctx, val) {
			return false
		}
		it.skipped++

		// TODO(dennwc): we don't really know if we should call NextPath or not,
		//               and there is no good way to know
		if it.skipped <= it.skip {
			// skipping NextPath results
			inNextPath = true
			if !it.primaryIt.NextPath(ctx) {
				// main path exists, but we skipped it
				// and we skipped all alternative paths now
				// so we definitely "don't have" this value
				return false
			}
			it.skipped++

			for it.skipped <= it.skip {
				if !it.primaryIt.NextPath(ctx) {
					return false
				}
				it.skipped++
			}
		}
	}
	if inNextPath && it.primaryIt.NextPath(ctx) {
		return true
	}
	return it.primaryIt.Contains(ctx, val)
}

// NextPath checks whether there is another path. It will skip first paths
// according to iterator parameter.
func (it *skipContains) NextPath(ctx context.Context) bool {
	for ; it.skipped < it.skip; it.skipped++ {
		if !it.primaryIt.NextPath(ctx) {
			return false
		}
	}
	return it.primaryIt.NextPath(ctx)
}

// Close closes the primary and all iterators.  It closes all subiterators
// it can, but returns the first error it encounters.
func (it *skipContains) Close() error {
	return it.primaryIt.Close()
}

func (it *skipContains) String() string {
	return fmt.Sprintf("SkipContains(%d)", it.skip)
}
