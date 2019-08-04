package iterator

import (
	"context"

	"github.com/cayleygraph/cayley/graph"
)

var _ graph.IteratorFuture = &Not{}

// Not iterator acts like a complement for the primary iterator.
// It will return all the vertices which are not part of the primary iterator.
type Not struct {
	it *not
	graph.Iterator
}

func NewNot(primaryIt, allIt graph.Iterator) *Not {
	it := &Not{
		it: newNot(graph.AsShape(primaryIt), graph.AsShape(allIt)),
	}
	it.Iterator = graph.NewLegacy(it.it)
	return it
}

func (it *Not) AsShape() graph.Shape {
	it.Close()
	return it.it
}

var _ graph.ShapeCompat = (*not)(nil)

// Not iterator acts like a complement for the primary iterator.
// It will return all the vertices which are not part of the primary iterator.
type not struct {
	primary graph.Shape
	allIt   graph.Shape
}

func newNot(primaryIt, allIt graph.Shape) *not {
	return &not{
		primary: primaryIt,
		allIt:   allIt,
	}
}

func (it *not) Iterate() graph.Scanner {
	return newNotNext(it.primary.Lookup(), it.allIt.Iterate())
}

func (it *not) Lookup() graph.Index {
	return newNotContains(it.primary.Lookup())
}

func (it *not) AsLegacy() graph.Iterator {
	it2 := &Not{it: it}
	it2.Iterator = graph.NewLegacy(it)
	return it2
}

// SubIterators returns a slice of the sub iterators.
// The first iterator is the primary iterator, for which the complement
// is generated.
func (it *not) SubIterators() []graph.Shape {
	return []graph.Shape{it.primary, it.allIt}
}

func (it *not) Optimize() (graph.Shape, bool) {
	// TODO - consider wrapping the primary with a MaterializeIt
	optimizedPrimaryIt, optimized := it.primary.Optimize()
	if optimized {
		it.primary = optimizedPrimaryIt
	}
	it.primary = newMaterialize(it.primary)
	return it, false
}

func (it *not) Stats() graph.IteratorStats {
	primaryStats := it.primary.Stats()
	allStats := it.allIt.Stats()
	return graph.IteratorStats{
		NextCost:     allStats.NextCost + primaryStats.ContainsCost,
		ContainsCost: primaryStats.ContainsCost,
		Size:         allStats.Size - primaryStats.Size,
		ExactSize:    false,
	}
}

func (it *not) Size() (int64, bool) {
	st := it.Stats()
	return st.Size, st.ExactSize
}

func (it *not) String() string {
	return "Not"
}

// Not iterator acts like a complement for the primary iterator.
// It will return all the vertices which are not part of the primary iterator.
type notNext struct {
	primaryIt graph.Index
	allIt     graph.Scanner
	result    graph.Ref
}

func newNotNext(primaryIt graph.Index, allIt graph.Scanner) *notNext {
	return &notNext{
		primaryIt: primaryIt,
		allIt:     allIt,
	}
}

func (it *notNext) TagResults(dst map[string]graph.Ref) {
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

func (it *notNext) Result() graph.Ref {
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
	primaryIt graph.Index
	result    graph.Ref
	err       error
}

func newNotContains(primaryIt graph.Index) *notContains {
	return &notContains{
		primaryIt: primaryIt,
	}
}

func (it *notContains) TagResults(dst map[string]graph.Ref) {
	if it.primaryIt != nil {
		it.primaryIt.TagResults(dst)
	}
}

func (it *notContains) Err() error {
	return it.err
}

func (it *notContains) Result() graph.Ref {
	return it.result
}

// Contains checks whether the passed value is part of the primary iterator's
// complement. For a valid value, it updates the Result returned by the iterator
// to the value itself.
func (it *notContains) Contains(ctx context.Context, val graph.Ref) bool {
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
