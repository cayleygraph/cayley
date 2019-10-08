package iterator

import (
	"context"
	"sort"

	"github.com/cayleygraph/cayley/graph"
)

var _ graph.IteratorFuture = &Sort{}

// Sort iterator orders values from it's subiterator.
type Sort struct {
	it *sortIt
	graph.Iterator
}

// NewSort creates a new Sort iterator.
// TODO(dennwc): This iterator must not be used inside And: it may be moved to a Contains branch and won't do anything.
//               We should make And/Intersect account for this.
func NewSort(namer graph.Namer, it graph.Iterator) *Sort {
	return &Sort{
		it: newSort(namer, graph.AsShape(it)),
	}
}

// AsShape returns Sort's underlying iterator shape
func (it *Sort) AsShape() graph.IteratorShape {
	return it.it
}

type sortIt struct {
	namer graph.Namer
	subIt graph.IteratorShape
}

var _ graph.IteratorShapeCompat = (*sortIt)(nil)

func newSort(namer graph.Namer, subIt graph.IteratorShape) *sortIt {
	return &sortIt{namer, subIt}
}

func (it *sortIt) Iterate() graph.Scanner {
	return newSortNext(it.namer, it.subIt.Iterate())
}

func (it *sortIt) AsLegacy() graph.Iterator {
	it2 := &Sort{it: it}
	it2.Iterator = graph.NewLegacy(it, it2)
	return it2
}

func (it *sortIt) Lookup() graph.Index {
	// TODO(dennwc): Lookup doesn't need any sorting. Using it this way is a bug in the optimizer.
	//               But instead of failing here, let still allow the query to execute. It won't be sorted,
	//               but it will work at least. Later consider changing returning an error here.
	return it.subIt.Lookup()
}

func (it *sortIt) Optimize(ctx context.Context) (graph.IteratorShape, bool) {
	newIt, optimized := it.subIt.Optimize(ctx)
	if optimized {
		it.subIt = newIt
	}
	return it, false
}

func (it *sortIt) Stats(ctx context.Context) (graph.IteratorCosts, error) {
	subStats, err := it.subIt.Stats(ctx)
	return graph.IteratorCosts{
		// TODO(dennwc): better cost calculation; we probably need an InitCost defined in graph.IteratorCosts
		NextCost:     subStats.NextCost * 2,
		ContainsCost: subStats.ContainsCost,
		Size: graph.Size{
			Size:  subStats.Size.Size,
			Exact: true,
		},
	}, err
}

func (it *sortIt) String() string {
	return "Sort"
}

// SubIterators returns a slice of the sub iterators.
func (it *sortIt) SubIterators() []graph.IteratorShape {
	return []graph.IteratorShape{it.subIt}
}

type sortValue struct {
	result
	str   string
	paths []result
}
type sortByString []sortValue

func (v sortByString) Len() int { return len(v) }
func (v sortByString) Less(i, j int) bool {
	return v[i].str < v[j].str
}
func (v sortByString) Swap(i, j int) { v[i], v[j] = v[j], v[i] }

type sortNext struct {
	namer     graph.Namer
	subIt     graph.Scanner
	ordered   sortByString
	result    result
	err       error
	index     int
	pathIndex int
}

func newSortNext(namer graph.Namer, subIt graph.Scanner) *sortNext {
	return &sortNext{
		namer:     namer,
		subIt:     subIt,
		pathIndex: -1,
	}
}

func (it *sortNext) TagResults(dst map[string]graph.Value) {
	for tag, value := range it.result.tags {
		dst[tag] = value
	}
}

func (it *sortNext) Err() error {
	return it.err
}

func (it *sortNext) Result() graph.Value {
	return it.result.id
}

func (it *sortNext) Next(ctx context.Context) bool {
	if it.err != nil {
		return false
	}
	if it.ordered == nil {
		v, err := getSortedValues(ctx, it.namer, it.subIt)
		it.ordered = v
		it.err = err
		if it.err != nil {
			return false
		}
	}
	if it.index >= len(it.ordered) {
		return false
	}
	it.pathIndex = -1
	it.result = it.ordered[it.index].result
	it.index++
	return true
}

func (it *sortNext) NextPath(ctx context.Context) bool {
	if it.index >= len(it.ordered) {
		return false
	}
	r := it.ordered[it.index]
	if it.pathIndex+1 >= len(r.paths) {
		return false
	}
	it.pathIndex++
	it.result = r.paths[it.pathIndex]
	return true
}

func (it *sortNext) Close() error {
	it.ordered = nil
	return it.subIt.Close()
}

func (it *sortNext) String() string {
	return "SortNext"
}

func getSortedValues(ctx context.Context, namer graph.Namer, it graph.Scanner) (sortByString, error) {
	var v sortByString
	for it.Next(ctx) {
		id := it.Result()
		// TODO(dennwc): batch and use graph.ValuesOf
		name := namer.NameOf(id)
		str := name.String()
		tags := make(map[string]graph.Ref)
		it.TagResults(tags)
		val := sortValue{
			result: result{id, tags},
			str:    str,
		}
		for it.NextPath(ctx) {
			tags = make(map[string]graph.Ref)
			it.TagResults(tags)
			val.paths = append(val.paths, result{id, tags})
		}
		v = append(v, val)
	}
	if err := it.Err(); err != nil {
		return v, err
	}
	sort.Sort(v)
	return v, nil
}
