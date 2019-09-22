package iterator

import (
	"context"
	"math"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/quad"
)

var _ graph.IteratorFuture = &Recursive{}

const recursiveBaseTag = "__base_recursive"

// Recursive iterator takes a base iterator and a morphism to be applied recursively, for each result.
type Recursive struct {
	it *recursive
	graph.Iterator
}

type seenAt struct {
	depth int
	val   graph.Ref
}

var DefaultMaxRecursiveSteps = 50

func NewRecursive(sub graph.Iterator, morphism Morphism, maxDepth int) *Recursive {
	it := &Recursive{
		it: newRecursive(graph.AsShape(sub), func(it graph.IteratorShape) graph.IteratorShape {
			return graph.AsShape(morphism(graph.AsLegacy(it)))
		}, maxDepth),
	}
	it.Iterator = graph.NewLegacy(it.it, it)
	return it
}

func (it *Recursive) AddDepthTag(s string) {
	it.it.AddDepthTag(s)
}

func (it *Recursive) AsShape() graph.IteratorShape {
	it.Close()
	return it.it
}

var _ graph.IteratorShapeCompat = &recursive{}

// Recursive iterator takes a base iterator and a morphism to be applied recursively, for each result.
type recursive struct {
	subIt     graph.IteratorShape
	morphism  Morphism2
	maxDepth  int
	depthTags []string
}

func newRecursive(it graph.IteratorShape, morphism Morphism2, maxDepth int) *recursive {
	if maxDepth == 0 {
		maxDepth = DefaultMaxRecursiveSteps
	}
	return &recursive{
		subIt:    it,
		morphism: morphism,
		maxDepth: maxDepth,
	}
}

func (it *recursive) Iterate() graph.Scanner {
	return newRecursiveNext(it.subIt.Iterate(), it.morphism, it.maxDepth, it.depthTags)
}

func (it *recursive) Lookup() graph.Index {
	return newRecursiveContains(newRecursiveNext(it.subIt.Iterate(), it.morphism, it.maxDepth, it.depthTags))
}

func (it *recursive) AsLegacy() graph.Iterator {
	it2 := &Recursive{it: it}
	it2.Iterator = graph.NewLegacy(it, it2)
	return it2
}

func (it *recursive) AddDepthTag(s string) {
	it.depthTags = append(it.depthTags, s)
}

func (it *recursive) SubIterators() []graph.IteratorShape {
	return []graph.IteratorShape{it.subIt}
}

func (it *recursive) Optimize(ctx context.Context) (graph.IteratorShape, bool) {
	newIt, optimized := it.subIt.Optimize(ctx)
	if optimized {
		it.subIt = newIt
	}
	return it, false
}

func (it *recursive) Stats(ctx context.Context) (graph.IteratorCosts, error) {
	base := newFixed()
	base.Add(Int64Node(20))
	fanoutit := it.morphism(base)
	fanoutStats, err := fanoutit.Stats(ctx)
	subitStats, err2 := it.subIt.Stats(ctx)
	if err == nil {
		err = err2
	}
	size := int64(math.Pow(float64(subitStats.Size.Size*fanoutStats.Size.Size), 5))
	return graph.IteratorCosts{
		NextCost:     subitStats.NextCost + fanoutStats.NextCost,
		ContainsCost: (subitStats.NextCost+fanoutStats.NextCost)*(size/10) + subitStats.ContainsCost,
		Size: graph.Size{
			Size:  size,
			Exact: false,
		},
	}, err
}

func (it *recursive) String() string {
	return "Recursive"
}

// Recursive iterator takes a base iterator and a morphism to be applied recursively, for each result.
type recursiveNext struct {
	subIt  graph.Scanner
	result seenAt
	err    error

	morphism      Morphism2
	seen          map[interface{}]seenAt
	nextIt        graph.Scanner
	depth         int
	maxDepth      int
	pathMap       map[interface{}][]map[string]graph.Ref
	pathIndex     int
	containsValue graph.Ref
	depthTags     []string
	depthCache    []graph.Ref
	baseIt        *fixed
}

func newRecursiveNext(it graph.Scanner, morphism Morphism2, maxDepth int, depthTags []string) *recursiveNext {
	return &recursiveNext{
		subIt:     it,
		morphism:  morphism,
		maxDepth:  maxDepth,
		depthTags: depthTags,

		seen:    make(map[interface{}]seenAt),
		nextIt:  &Null{},
		baseIt:  newFixed(),
		pathMap: make(map[interface{}][]map[string]graph.Ref),
	}
}

func (it *recursiveNext) TagResults(dst map[string]graph.Ref) {
	for _, tag := range it.depthTags {
		dst[tag] = graph.PreFetched(quad.Int(it.result.depth))
	}

	if it.containsValue != nil {
		paths := it.pathMap[graph.ToKey(it.containsValue)]
		if len(paths) != 0 {
			for k, v := range paths[it.pathIndex] {
				dst[k] = v
			}
		}
	}
	if it.nextIt != nil {
		it.nextIt.TagResults(dst)
		delete(dst, recursiveBaseTag)
	}
}

func (it *recursiveNext) Next(ctx context.Context) bool {
	it.pathIndex = 0
	if it.depth == 0 {
		for it.subIt.Next(ctx) {
			res := it.subIt.Result()
			it.depthCache = append(it.depthCache, it.subIt.Result())
			tags := make(map[string]graph.Ref)
			it.subIt.TagResults(tags)
			key := graph.ToKey(res)
			it.pathMap[key] = append(it.pathMap[key], tags)
			for it.subIt.NextPath(ctx) {
				tags := make(map[string]graph.Ref)
				it.subIt.TagResults(tags)
				it.pathMap[key] = append(it.pathMap[key], tags)
			}
		}
	}

	for {
		if !it.nextIt.Next(ctx) {
			if it.maxDepth > 0 && it.depth >= it.maxDepth {
				return false
			} else if len(it.depthCache) == 0 {
				return false
			}
			it.depth++
			it.baseIt = newFixed(it.depthCache...)
			it.depthCache = nil
			if it.nextIt != nil {
				it.nextIt.Close()
			}
			it.nextIt = it.morphism(TagShape(it.baseIt, recursiveBaseTag)).Iterate()
			continue
		}
		val := it.nextIt.Result()
		results := make(map[string]graph.Ref)
		it.nextIt.TagResults(results)
		key := graph.ToKey(val)
		if _, seen := it.seen[key]; !seen {
			it.seen[key] = seenAt{
				val:   results[recursiveBaseTag],
				depth: it.depth,
			}
			it.result.depth = it.depth
			it.result.val = val
			it.containsValue = it.getBaseValue(val)
			it.depthCache = append(it.depthCache, val)
			return true
		}
	}
}

func (it *recursiveNext) Err() error {
	return it.err
}

func (it *recursiveNext) Result() graph.Ref {
	return it.result.val
}

func (it *recursiveNext) getBaseValue(val graph.Ref) graph.Ref {
	var at seenAt
	var ok bool
	if at, ok = it.seen[graph.ToKey(val)]; !ok {
		panic("trying to getBaseValue of something unseen")
	}
	for at.depth != 1 {
		if at.depth == 0 {
			panic("seen chain is broken")
		}
		at = it.seen[graph.ToKey(at.val)]
	}
	return at.val
}

func (it *recursiveNext) NextPath(ctx context.Context) bool {
	if it.pathIndex+1 >= len(it.pathMap[graph.ToKey(it.containsValue)]) {
		return false
	}
	it.pathIndex++
	return true
}

func (it *recursiveNext) Close() error {
	err := it.subIt.Close()
	if err != nil {
		return err
	}
	err = it.nextIt.Close()
	if err != nil {
		return err
	}
	it.seen = nil
	return it.err
}

func (it *recursiveNext) String() string {
	return "RecursiveNext"
}

// Recursive iterator takes a base iterator and a morphism to be applied recursively, for each result.
type recursiveContains struct {
	next *recursiveNext
}

func newRecursiveContains(next *recursiveNext) *recursiveContains {
	return &recursiveContains{
		next: next,
	}
}

func (it *recursiveContains) TagResults(dst map[string]graph.Ref) {
	it.next.TagResults(dst)
}

func (it *recursiveContains) Err() error {
	return it.next.Err()
}

func (it *recursiveContains) Result() graph.Ref {
	return it.next.Result()
}

func (it *recursiveContains) Contains(ctx context.Context, val graph.Ref) bool {
	it.next.pathIndex = 0
	key := graph.ToKey(val)
	if at, ok := it.next.seen[key]; ok {
		it.next.containsValue = it.next.getBaseValue(val)
		it.next.result.depth = at.depth
		it.next.result.val = val
		return true
	}
	for it.next.Next(ctx) {
		if graph.ToKey(it.next.Result()) == key {
			return true
		}
	}
	return false
}

func (it *recursiveContains) NextPath(ctx context.Context) bool {
	return it.next.NextPath(ctx)
}

func (it *recursiveContains) Close() error {
	return it.next.Close()
}

func (it *recursiveContains) String() string {
	return "RecursiveContains(" + it.next.String() + ")"
}
