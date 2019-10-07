package iterator

import (
	"context"
	"sort"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/quad"
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

type value struct {
	result result
	name   quad.Value
	str    string
}
type values []value

func (v values) Len() int { return len(v) }
func (v values) Less(i, j int) bool {
	return v[i].str < v[j].str
}
func (v values) Swap(i, j int) { v[i], v[j] = v[j], v[i] }

type sortNext struct {
	namer   graph.Namer
	subIt   graph.Scanner
	ordered values
	result  result
	err     error
	index   int
}

func newSortNext(namer graph.Namer, subIt graph.Scanner) *sortNext {
	return &sortNext{
		namer: namer,
		subIt: subIt,
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
	}
	ordered := it.ordered
	if it.index < len(ordered) {
		it.result = ordered[it.index].result
		it.index++
		return true
	}
	return false
}

func (it *sortNext) NextPath(ctx context.Context) bool {
	return false
}

func (it *sortNext) Close() error {
	it.ordered = nil
	return it.subIt.Close()
}

func (it *sortNext) String() string {
	return "SortNext"
}

func getSortedValues(ctx context.Context, namer graph.Namer, it graph.Scanner) (values, error) {
	var v values

	for it.Next(ctx) {
		var id = it.Result()
		var name = namer.NameOf(id)
		var str = name.String()
		var tags = make(map[string]graph.Value)
		it.TagResults(tags)
		result := result{id, tags}
		value := value{result, name, str}
		v = append(v, value)
		err := it.Err()
		if err != nil {
			return v, err
		}
	}

	sort.Sort(v)

	return v, nil
}
