package iterator

import (
	"context"
	"fmt"

	"github.com/cayleygraph/cayley/graph"
)

var _ graph.IteratorFuture = &Limit{}

// Limit iterator will stop iterating if certain a number of values were encountered.
// Zero and negative limit values means no limit.
type Limit struct {
	it *limit
	graph.Iterator
}

func NewLimit(primaryIt graph.Iterator, limit int64) *Limit {
	it := &Limit{
		it: newLimit(graph.AsShape(primaryIt), limit),
	}
	it.Iterator = graph.NewLegacy(it.it, it)
	return it
}

func (it *Limit) AsShape() graph.IteratorShape {
	it.Close()
	return it.it
}

var _ graph.IteratorShapeCompat = &limit{}

// Limit iterator will stop iterating if certain a number of values were encountered.
// Zero and negative limit values means no limit.
type limit struct {
	limit     int64
	primaryIt graph.IteratorShape
}

func newLimit(primaryIt graph.IteratorShape, max int64) *limit {
	return &limit{
		limit:     max,
		primaryIt: primaryIt,
	}
}

func (it *limit) Iterate() graph.Scanner {
	return newLimitNext(it.primaryIt.Iterate(), it.limit)
}

func (it *limit) Lookup() graph.Index {
	return newLimitContains(it.primaryIt.Lookup(), it.limit)
}

func (it *limit) AsLegacy() graph.Iterator {
	it2 := &Limit{it: it}
	it2.Iterator = graph.NewLegacy(it, it2)
	return it2
}

// SubIterators returns a slice of the sub iterators.
func (it *limit) SubIterators() []graph.IteratorShape {
	return []graph.IteratorShape{it.primaryIt}
}

func (it *limit) Optimize(ctx context.Context) (graph.IteratorShape, bool) {
	optimizedPrimaryIt, optimized := it.primaryIt.Optimize(ctx)
	if it.limit <= 0 { // no limit
		return optimizedPrimaryIt, true
	}
	it.primaryIt = optimizedPrimaryIt
	return it, optimized
}

func (it *limit) Stats(ctx context.Context) (graph.IteratorCosts, error) {
	primaryStats, err := it.primaryIt.Stats(ctx)
	if it.limit > 0 && primaryStats.Size.Size > it.limit {
		primaryStats.Size.Size = it.limit
	}
	return primaryStats, err
}

func (it *limit) String() string {
	return fmt.Sprintf("Limit(%d)", it.limit)
}

// Limit iterator will stop iterating if certain a number of values were encountered.
// Zero and negative limit values means no limit.
type limitNext struct {
	limit     int64
	count     int64
	primaryIt graph.Scanner
}

func newLimitNext(primaryIt graph.Scanner, limit int64) *limitNext {
	return &limitNext{
		limit:     limit,
		primaryIt: primaryIt,
	}
}

func (it *limitNext) TagResults(dst map[string]graph.Ref) {
	it.primaryIt.TagResults(dst)
}

// Next advances the Limit iterator. It will stop iteration if limit was reached.
func (it *limitNext) Next(ctx context.Context) bool {
	if it.limit > 0 && it.count >= it.limit {
		return false
	}
	if it.primaryIt.Next(ctx) {
		it.count++
		return true
	}
	return false
}

func (it *limitNext) Err() error {
	return it.primaryIt.Err()
}

func (it *limitNext) Result() graph.Ref {
	return it.primaryIt.Result()
}

// NextPath checks whether there is another path. Will call primary iterator
// if limit is not reached yet.
func (it *limitNext) NextPath(ctx context.Context) bool {
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
func (it *limitNext) Close() error {
	return it.primaryIt.Close()
}

func (it *limitNext) String() string {
	return fmt.Sprintf("LimitNext(%d)", it.limit)
}

// Limit iterator will stop iterating if certain a number of values were encountered.
// Zero and negative limit values means no limit.
type limitContains struct {
	limit     int64
	count     int64
	primaryIt graph.Index
}

func newLimitContains(primaryIt graph.Index, limit int64) *limitContains {
	return &limitContains{
		limit:     limit,
		primaryIt: primaryIt,
	}
}

func (it *limitContains) TagResults(dst map[string]graph.Ref) {
	it.primaryIt.TagResults(dst)
}

func (it *limitContains) Err() error {
	return it.primaryIt.Err()
}

func (it *limitContains) Result() graph.Ref {
	return it.primaryIt.Result()
}

func (it *limitContains) Contains(ctx context.Context, val graph.Ref) bool {
	if it.limit > 0 && it.count >= it.limit {
		return false
	}
	if it.primaryIt.Contains(ctx, val) {
		it.count++
		return true
	}
	return false
}

// NextPath checks whether there is another path. Will call primary iterator
// if limit is not reached yet.
func (it *limitContains) NextPath(ctx context.Context) bool {
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
func (it *limitContains) Close() error {
	return it.primaryIt.Close()
}

func (it *limitContains) String() string {
	return fmt.Sprintf("LimitContains(%d)", it.limit)
}
