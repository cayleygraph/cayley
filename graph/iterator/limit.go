package iterator

import (
	"context"
	"fmt"

	"github.com/cayleygraph/cayley/graph"
)

// Limit iterator will stop iterating if certain a number of values were encountered.
// Zero and negative Limit values means no Limit.
type Limit struct {
	limit int64
	it    graph.IteratorShape
}

func NewLimit(it graph.IteratorShape, max int64) *Limit {
	return &Limit{
		limit: max,
		it:    it,
	}
}

func (it *Limit) Iterate() graph.Scanner {
	return NewLimitNext(it.it.Iterate(), it.limit)
}

func (it *Limit) Lookup() graph.Index {
	return newLimitContains(it.it.Lookup(), it.limit)
}

// SubIterators returns a slice of the sub iterators.
func (it *Limit) SubIterators() []graph.IteratorShape {
	return []graph.IteratorShape{it.it}
}

func (it *Limit) Optimize(ctx context.Context) (graph.IteratorShape, bool) {
	nit, optimized := it.it.Optimize(ctx)
	if it.limit <= 0 { // no Limit
		return nit, true
	}
	it.it = nit
	return it, optimized
}

func (it *Limit) Stats(ctx context.Context) (graph.IteratorCosts, error) {
	st, err := it.it.Stats(ctx)
	if it.limit > 0 && st.Size.Value > it.limit {
		st.Size.Value = it.limit
	}
	return st, err
}

func (it *Limit) String() string {
	return fmt.Sprintf("Limit(%d)", it.limit)
}

// Limit iterator will stop iterating if certain a number of values were encountered.
// Zero and negative Limit values means no Limit.
type limitNext struct {
	limit int64
	count int64
	it    graph.Scanner
}

func NewLimitNext(it graph.Scanner, limit int64) graph.Scanner {
	return &limitNext{
		limit: limit,
		it:    it,
	}
}

func (it *limitNext) TagResults(dst map[string]graph.Ref) {
	it.it.TagResults(dst)
}

// Next advances the Limit iterator. It will stop iteration if Limit was reached.
func (it *limitNext) Next(ctx context.Context) bool {
	if it.limit > 0 && it.count >= it.limit {
		return false
	}
	if it.it.Next(ctx) {
		it.count++
		return true
	}
	return false
}

func (it *limitNext) Err() error {
	return it.it.Err()
}

func (it *limitNext) Result() graph.Ref {
	return it.it.Result()
}

// NextPath checks whether there is another path. Will call primary iterator
// if Limit is not reached yet.
func (it *limitNext) NextPath(ctx context.Context) bool {
	if it.limit > 0 && it.count >= it.limit {
		return false
	}
	if it.it.NextPath(ctx) {
		it.count++
		return true
	}
	return false
}

// Close closes the primary and all iterators.  It closes all subiterators
// it can, but returns the first error it encounters.
func (it *limitNext) Close() error {
	return it.it.Close()
}

func (it *limitNext) String() string {
	return fmt.Sprintf("LimitNext(%d)", it.limit)
}

// Limit iterator will stop iterating if certain a number of values were encountered.
// Zero and negative Limit values means no Limit.
type limitContains struct {
	limit int64
	count int64
	it    graph.Index
}

func newLimitContains(it graph.Index, limit int64) *limitContains {
	return &limitContains{
		limit: limit,
		it:    it,
	}
}

func (it *limitContains) TagResults(dst map[string]graph.Ref) {
	it.it.TagResults(dst)
}

func (it *limitContains) Err() error {
	return it.it.Err()
}

func (it *limitContains) Result() graph.Ref {
	return it.it.Result()
}

func (it *limitContains) Contains(ctx context.Context, val graph.Ref) bool {
	if it.limit > 0 && it.count >= it.limit {
		return false
	}
	if it.it.Contains(ctx, val) {
		it.count++
		return true
	}
	return false
}

// NextPath checks whether there is another path. Will call primary iterator
// if Limit is not reached yet.
func (it *limitContains) NextPath(ctx context.Context) bool {
	if it.limit > 0 && it.count >= it.limit {
		return false
	}
	if it.it.NextPath(ctx) {
		it.count++
		return true
	}
	return false
}

// Close closes the primary and all iterators.  It closes all subiterators
// it can, but returns the first error it encounters.
func (it *limitContains) Close() error {
	return it.it.Close()
}

func (it *limitContains) String() string {
	return fmt.Sprintf("LimitContains(%d)", it.limit)
}
