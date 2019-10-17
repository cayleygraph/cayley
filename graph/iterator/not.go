package iterator

import (
	"context"

	"github.com/cayleygraph/cayley/graph/refs"
)

// Not iterator acts like a complement for the primary iterator.
// It will return all the vertices which are not part of the primary iterator.
type Not struct {
	primary Shape
	allIt   Shape
}

func NewNot(primaryIt, allIt Shape) *Not {
	return &Not{
		primary: primaryIt,
		allIt:   allIt,
	}
}

func (it *Not) Iterate() Scanner {
	return newNotNext(it.primary.Lookup(), it.allIt.Iterate())
}

func (it *Not) Lookup() Index {
	return newNotContains(it.primary.Lookup())
}

// SubIterators returns a slice of the sub iterators.
// The first iterator is the primary iterator, for which the complement
// is generated.
func (it *Not) SubIterators() []Shape {
	return []Shape{it.primary, it.allIt}
}

func (it *Not) Optimize(ctx context.Context) (Shape, bool) {
	// TODO - consider wrapping the primary with a MaterializeIt
	optimizedPrimaryIt, optimized := it.primary.Optimize(ctx)
	if optimized {
		it.primary = optimizedPrimaryIt
	}
	it.primary = NewMaterialize(it.primary)
	return it, false
}

func (it *Not) Stats(ctx context.Context) (Costs, error) {
	primaryStats, err := it.primary.Stats(ctx)
	allStats, err2 := it.allIt.Stats(ctx)
	if err == nil {
		err = err2
	}
	return Costs{
		NextCost:     allStats.NextCost + primaryStats.ContainsCost,
		ContainsCost: primaryStats.ContainsCost,
		Size: refs.Size{
			Value: allStats.Size.Value - primaryStats.Size.Value,
			Exact: false,
		},
	}, err
}

func (it *Not) String() string {
	return "Not"
}

// Not iterator acts like a complement for the primary iterator.
// It will return all the vertices which are not part of the primary iterator.
type notNext struct {
	primaryIt Index
	allIt     Scanner
	result    refs.Ref
}

func newNotNext(primaryIt Index, allIt Scanner) *notNext {
	return &notNext{
		primaryIt: primaryIt,
		allIt:     allIt,
	}
}

func (it *notNext) TagResults(dst map[string]refs.Ref) {
	if it.primaryIt != nil {
		it.primaryIt.TagResults(dst)
	}
}

// Next advances the Not iterator. It returns whether there is another valid
// new value. It fetches the next value of the all iterator which is not
// contained by the primary iterator.
func (it *notNext) Next(ctx context.Context) bool {
	for it.allIt.Next(ctx) {
		if curr := it.allIt.Result(); !it.primaryIt.Contains(ctx, curr) {
			it.result = curr
			return true
		}
	}
	return false
}

func (it *notNext) Err() error {
	if err := it.allIt.Err(); err != nil {
		return err
	}
	if err := it.primaryIt.Err(); err != nil {
		return err
	}
	return nil
}

func (it *notNext) Result() refs.Ref {
	return it.result
}

// NextPath checks whether there is another path. Not applicable, hence it will
// return false.
func (it *notNext) NextPath(ctx context.Context) bool {
	return false
}

// Close closes the primary and all iterators.  It closes all subiterators
// it can, but returns the first error it encounters.
func (it *notNext) Close() error {
	err := it.primaryIt.Close()
	if err2 := it.allIt.Close(); err2 != nil && err == nil {
		err = err2
	}
	return err
}

func (it *notNext) String() string {
	return "NotNext"
}

// Not iterator acts like a complement for the primary iterator.
// It will return all the vertices which are not part of the primary iterator.
type notContains struct {
	primaryIt Index
	result    refs.Ref
	err       error
}

func newNotContains(primaryIt Index) *notContains {
	return &notContains{
		primaryIt: primaryIt,
	}
}

func (it *notContains) TagResults(dst map[string]refs.Ref) {
	if it.primaryIt != nil {
		it.primaryIt.TagResults(dst)
	}
}

func (it *notContains) Err() error {
	return it.err
}

func (it *notContains) Result() refs.Ref {
	return it.result
}

// Contains checks whether the passed value is part of the primary iterator's
// complement. For a valid value, it updates the Result returned by the iterator
// to the value itself.
func (it *notContains) Contains(ctx context.Context, val refs.Ref) bool {
	if it.primaryIt.Contains(ctx, val) {
		return false
	}
	it.err = it.primaryIt.Err()
	if it.err != nil {
		// Explicitly return 'false', since an error occurred.
		return false
	}
	it.result = val
	return true
}

// NextPath checks whether there is another path. Not applicable, hence it will
// return false.
func (it *notContains) NextPath(ctx context.Context) bool {
	return false
}

// Close closes the primary and all iterators.  It closes all subiterators
// it can, but returns the first error it encounters.
func (it *notContains) Close() error {
	return it.primaryIt.Close()
}

func (it *notContains) String() string {
	return "NotContains"
}
