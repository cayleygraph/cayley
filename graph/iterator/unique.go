package iterator

import (
	"context"

	"github.com/cayleygraph/cayley/graph/refs"
)

// Unique iterator removes duplicate values from it's subiterator.
type Unique struct {
	subIt Shape
}

func NewUnique(subIt Shape) *Unique {
	return &Unique{
		subIt: subIt,
	}
}

func (it *Unique) Iterate() Scanner {
	return newUniqueNext(it.subIt.Iterate())
}

func (it *Unique) Lookup() Index {
	return newUniqueContains(it.subIt.Lookup())
}

// SubIterators returns a slice of the sub iterators. The first iterator is the
// primary iterator, for which the complement is generated.
func (it *Unique) SubIterators() []Shape {
	return []Shape{it.subIt}
}

func (it *Unique) Optimize(ctx context.Context) (Shape, bool) {
	newIt, optimized := it.subIt.Optimize(ctx)
	if optimized {
		it.subIt = newIt
	}
	return it, false
}

const uniquenessFactor = 2

func (it *Unique) Stats(ctx context.Context) (Costs, error) {
	subStats, err := it.subIt.Stats(ctx)
	return Costs{
		NextCost:     subStats.NextCost * uniquenessFactor,
		ContainsCost: subStats.ContainsCost,
		Size: refs.Size{
			Value: subStats.Size.Value / uniquenessFactor,
			Exact: false,
		},
	}, err
}

func (it *Unique) String() string {
	return "Unique"
}

// Unique iterator removes duplicate values from it's subiterator.
type uniqueNext struct {
	subIt  Scanner
	result refs.Ref
	err    error
	seen   map[interface{}]bool
}

func newUniqueNext(subIt Scanner) *uniqueNext {
	return &uniqueNext{
		subIt: subIt,
		seen:  make(map[interface{}]bool),
	}
}

func (it *uniqueNext) TagResults(dst map[string]refs.Ref) {
	if it.subIt != nil {
		it.subIt.TagResults(dst)
	}
}

// Next advances the subiterator, continuing until it returns a value which it
// has not previously seen.
func (it *uniqueNext) Next(ctx context.Context) bool {
	for it.subIt.Next(ctx) {
		curr := it.subIt.Result()
		key := refs.ToKey(curr)
		if ok := it.seen[key]; !ok {
			it.result = curr
			it.seen[key] = true
			return true
		}
	}
	it.err = it.subIt.Err()
	return false
}

func (it *uniqueNext) Err() error {
	return it.err
}

func (it *uniqueNext) Result() refs.Ref {
	return it.result
}

// NextPath for unique always returns false. If we were to return multiple
// paths, we'd no longer be a unique result, so we have to choose only the first
// path that got us here. Unique is serious on this point.
func (it *uniqueNext) NextPath(ctx context.Context) bool {
	return false
}

// Close closes the primary iterators.
func (it *uniqueNext) Close() error {
	it.seen = nil
	return it.subIt.Close()
}

func (it *uniqueNext) String() string {
	return "UniqueNext"
}

// Unique iterator removes duplicate values from it's subiterator.
type uniqueContains struct {
	subIt Index
}

func newUniqueContains(subIt Index) *uniqueContains {
	return &uniqueContains{
		subIt: subIt,
	}
}

func (it *uniqueContains) TagResults(dst map[string]refs.Ref) {
	if it.subIt != nil {
		it.subIt.TagResults(dst)
	}
}

func (it *uniqueContains) Err() error {
	return it.subIt.Err()
}

func (it *uniqueContains) Result() refs.Ref {
	return it.subIt.Result()
}

// Contains checks whether the passed value is part of the primary iterator,
// which is irrelevant for uniqueness.
func (it *uniqueContains) Contains(ctx context.Context, val refs.Ref) bool {
	return it.subIt.Contains(ctx, val)
}

// NextPath for unique always returns false. If we were to return multiple
// paths, we'd no longer be a unique result, so we have to choose only the first
// path that got us here. Unique is serious on this point.
func (it *uniqueContains) NextPath(ctx context.Context) bool {
	return false
}

// Close closes the primary iterators.
func (it *uniqueContains) Close() error {
	return it.subIt.Close()
}

func (it *uniqueContains) String() string {
	return "UniqueContains"
}
