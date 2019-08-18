package iterator

import (
	"context"
	"math"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/quad"
)

// Recursive iterator takes a base iterator and a morphism to be applied recursively, for each result.
type Recursive struct {
	subIt    graph.Iterator
	result   seenAt
	runstats graph.IteratorStats
	err      error

	morphism      Morphism
	seen          map[interface{}]seenAt
	nextIt        graph.Iterator
	depth         int
	maxDepth      int
	pathMap       map[interface{}][]map[string]graph.Ref
	pathIndex     int
	containsValue graph.Ref
	depthTags     []string
	depthCache    []graph.Ref
	baseIt        graph.FixedIterator
}

type seenAt struct {
	depth int
	val   graph.Ref
}

var _ graph.Iterator = &Recursive{}

var DefaultMaxRecursiveSteps = 50

func NewRecursive(it graph.Iterator, morphism Morphism, maxDepth int) *Recursive {
	if maxDepth == 0 {
		maxDepth = DefaultMaxRecursiveSteps
	}

	return &Recursive{
		subIt: it,

		morphism:      morphism,
		seen:          make(map[interface{}]seenAt),
		nextIt:        &Null{},
		baseIt:        NewFixed(),
		pathMap:       make(map[interface{}][]map[string]graph.Ref),
		containsValue: nil,
		maxDepth:      maxDepth,
	}
}

func (it *Recursive) Reset() {
	it.result.val = nil
	it.result.depth = 0
	it.err = nil
	it.subIt.Reset()
	it.seen = make(map[interface{}]seenAt)
	it.pathMap = make(map[interface{}][]map[string]graph.Ref)
	it.containsValue = nil
	it.pathIndex = 0
	it.nextIt = &Null{}
	it.baseIt = NewFixed()
	it.depth = 0
}

func (it *Recursive) AddDepthTag(s string) {
	it.depthTags = append(it.depthTags, s)
}

func (it *Recursive) TagResults(dst map[string]graph.Ref) {
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
		delete(dst, "__base_recursive")
	}
}

func (it *Recursive) SubIterators() []graph.Iterator {
	return []graph.Iterator{it.subIt}
}

func (it *Recursive) Next(ctx context.Context) bool {
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
			it.baseIt = NewFixed(it.depthCache...)
			it.depthCache = nil
			if it.nextIt != nil {
				it.nextIt.Close()
			}
			it.nextIt = it.morphism(Tag(it.baseIt, "__base_recursive"))
			continue
		}
		val := it.nextIt.Result()
		results := make(map[string]graph.Ref)
		it.nextIt.TagResults(results)
		key := graph.ToKey(val)
		if _, seen := it.seen[key]; !seen {
			it.seen[key] = seenAt{
				val:   results["__base_recursive"],
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

func (it *Recursive) Err() error {
	return it.err
}

func (it *Recursive) Result() graph.Ref {
	return it.result.val
}

func (it *Recursive) getBaseValue(val graph.Ref) graph.Ref {
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

func (it *Recursive) Contains(ctx context.Context, val graph.Ref) bool {
	it.pathIndex = 0
	key := graph.ToKey(val)
	if at, ok := it.seen[key]; ok {
		it.containsValue = it.getBaseValue(val)
		it.result.depth = at.depth
		it.result.val = val
		return true
	}
	for it.Next(ctx) {
		if graph.ToKey(it.Result()) == key {
			return true
		}
	}
	return false
}

func (it *Recursive) NextPath(ctx context.Context) bool {
	if it.pathIndex+1 >= len(it.pathMap[graph.ToKey(it.containsValue)]) {
		return false
	}
	it.pathIndex++
	return true
}

func (it *Recursive) Close() error {
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

func (it *Recursive) Optimize() (graph.Iterator, bool) {
	newIt, optimized := it.subIt.Optimize()
	if optimized {
		it.subIt = newIt
	}
	return it, false
}

func (it *Recursive) Size() (int64, bool) {
	return it.Stats().Size, false
}

func (it *Recursive) Stats() graph.IteratorStats {
	base := NewFixed()
	base.Add(Int64Node(20))
	fanoutit := it.morphism(base)
	fanoutStats := fanoutit.Stats()
	subitStats := it.subIt.Stats()

	size := int64(math.Pow(float64(subitStats.Size*fanoutStats.Size), 5))
	return graph.IteratorStats{
		NextCost:     subitStats.NextCost + fanoutStats.NextCost,
		ContainsCost: (subitStats.NextCost+fanoutStats.NextCost)*(size/10) + subitStats.ContainsCost,
		Size:         size,
		Next:         it.runstats.Next,
		Contains:     it.runstats.Contains,
		ContainsNext: it.runstats.ContainsNext,
	}
}

func (it *Recursive) String() string {
	return "Recursive"
}
