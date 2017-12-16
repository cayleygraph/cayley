package iterator

import (
	"math"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/quad"
)

// Recursive iterator takes a base iterator and a morphism to be applied recursively, for each result.
type Recursive struct {
	uid      uint64
	tags     graph.Tagger
	subIt    graph.Iterator
	result   seenAt
	runstats graph.IteratorStats
	err      error

	qs            graph.QuadStore
	morphism      graph.ApplyMorphism
	seen          map[interface{}]seenAt
	nextIt        graph.Iterator
	depth         int
	maxDepth      int
	pathMap       map[interface{}][]map[string]graph.Value
	pathIndex     int
	containsValue graph.Value
	depthTags     graph.Tagger
	depthCache    []graph.Value
	baseIt        graph.FixedIterator
}

type seenAt struct {
	depth int
	val   graph.Value
}

var _ graph.Iterator = &Recursive{}

var DefaultMaxRecursiveSteps = 50

func NewRecursive(qs graph.QuadStore, it graph.Iterator, morphism graph.ApplyMorphism, maxDepth int) *Recursive {
	if maxDepth == 0 {
		maxDepth = DefaultMaxRecursiveSteps
	}

	return &Recursive{
		uid:   NextUID(),
		subIt: it,

		qs:            qs,
		morphism:      morphism,
		seen:          make(map[interface{}]seenAt),
		nextIt:        &Null{},
		baseIt:        qs.FixedIterator(),
		pathMap:       make(map[interface{}][]map[string]graph.Value),
		containsValue: nil,
		maxDepth:      maxDepth,
	}
}

func (it *Recursive) UID() uint64 {
	return it.uid
}

func (it *Recursive) Reset() {
	it.result.val = nil
	it.result.depth = 0
	it.err = nil
	it.subIt.Reset()
	it.seen = make(map[interface{}]seenAt)
	it.pathMap = make(map[interface{}][]map[string]graph.Value)
	it.containsValue = nil
	it.pathIndex = 0
	it.nextIt = &Null{}
	it.baseIt = it.qs.FixedIterator()
	it.depth = 0
}

func (it *Recursive) Tagger() *graph.Tagger {
	return &it.tags
}

func (it *Recursive) AddDepthTag(s string) {
	it.depthTags.Add(s)
}

func (it *Recursive) TagResults(dst map[string]graph.Value) {
	it.tags.TagResult(dst, it.Result())
	it.depthTags.TagResult(dst, graph.PreFetched(quad.Int(it.result.depth)))

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

func (it *Recursive) Clone() graph.Iterator {
	n := NewRecursive(it.qs, it.subIt.Clone(), it.morphism, it.maxDepth)
	n.tags.CopyFrom(it)
	n.depthTags.CopyFromTagger(&it.depthTags)
	return n
}

func (it *Recursive) SubIterators() []graph.Iterator {
	return []graph.Iterator{it.subIt}
}

func (it *Recursive) Next() bool {
	it.pathIndex = 0
	if it.depth == 0 {
		for it.subIt.Next() {
			res := it.subIt.Result()
			it.depthCache = append(it.depthCache, it.subIt.Result())
			tags := make(map[string]graph.Value)
			it.subIt.TagResults(tags)
			key := graph.ToKey(res)
			it.pathMap[key] = append(it.pathMap[key], tags)
			for it.subIt.NextPath() {
				tags := make(map[string]graph.Value)
				it.subIt.TagResults(tags)
				it.pathMap[key] = append(it.pathMap[key], tags)
			}
		}
	}
	if it.depth >= it.maxDepth {
		return graph.NextLogOut(it, false)
	}
	for {
		ok := it.nextIt.Next()
		if !ok {
			if len(it.depthCache) == 0 {
				return graph.NextLogOut(it, false)
			}
			it.depth++
			it.baseIt = it.qs.FixedIterator()
			for _, x := range it.depthCache {
				it.baseIt.Add(x)
			}
			it.baseIt.Tagger().Add("__base_recursive")
			it.depthCache = nil
			it.nextIt = it.morphism(it.qs, it.baseIt)
			continue
		}
		val := it.nextIt.Result()
		results := make(map[string]graph.Value)
		it.nextIt.TagResults(results)
		key := graph.ToKey(val)
		if _, ok := it.seen[key]; ok {
			continue
		}
		it.seen[key] = seenAt{
			val:   results["__base_recursive"],
			depth: it.depth,
		}
		it.result.depth = it.depth
		it.result.val = val
		it.containsValue = it.getBaseValue(val)
		it.depthCache = append(it.depthCache, val)
		break
	}
	return graph.NextLogOut(it, true)
}

func (it *Recursive) Err() error {
	return it.err
}

func (it *Recursive) Result() graph.Value {
	return it.result.val
}

func (it *Recursive) getBaseValue(val graph.Value) graph.Value {
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

func (it *Recursive) Contains(val graph.Value) bool {
	graph.ContainsLogIn(it, val)
	it.pathIndex = 0
	key := graph.ToKey(val)
	if at, ok := it.seen[key]; ok {
		it.containsValue = it.getBaseValue(val)
		it.result.depth = at.depth
		it.result.val = val
		return graph.ContainsLogOut(it, val, true)
	}
	for it.Next() {
		if graph.ToKey(it.Result()) == key {
			return graph.ContainsLogOut(it, val, true)
		}
	}
	return graph.ContainsLogOut(it, val, false)
}

func (it *Recursive) NextPath() bool {
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

func (it *Recursive) Type() graph.Type { return graph.Recursive }

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
	base := it.qs.FixedIterator()
	base.Add(Int64Node(20))
	fanoutit := it.morphism(it.qs, base)
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
