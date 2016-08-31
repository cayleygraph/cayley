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
	seen          map[graph.Value]seenAt
	nextIt        graph.Iterator
	depth         int
	pathMap       map[graph.Value][]map[string]graph.Value
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

var MaxRecursiveSteps = 50

func NewRecursive(qs graph.QuadStore, it graph.Iterator, morphism graph.ApplyMorphism) *Recursive {
	return &Recursive{
		uid:   NextUID(),
		subIt: it,

		qs:            qs,
		morphism:      morphism,
		seen:          make(map[graph.Value]seenAt),
		nextIt:        &Null{},
		baseIt:        qs.FixedIterator(),
		pathMap:       make(map[graph.Value][]map[string]graph.Value),
		containsValue: nil,
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
	it.seen = make(map[graph.Value]seenAt)
	it.pathMap = make(map[graph.Value][]map[string]graph.Value)
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
	for _, tag := range it.tags.Tags() {
		dst[tag] = it.Result()
	}

	for _, tag := range it.depthTags.Tags() {
		dst[tag] = it.qs.ValueOf(quad.Int(it.result.depth))
	}

	for tag, value := range it.tags.Fixed() {
		dst[tag] = value
	}

	for tag, value := range it.depthTags.Fixed() {
		dst[tag] = value
	}
	if it.containsValue != nil {
		m := it.pathMap[it.containsValue][it.pathIndex]
		for k, v := range m {
			dst[k] = v
		}
	}

}

func (it *Recursive) Clone() graph.Iterator {
	n := NewRecursive(it.qs, it.subIt.Clone(), it.morphism)
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
			it.pathMap[res] = append(it.pathMap[res], tags)
			for it.subIt.NextPath() {
				tags := make(map[string]graph.Value)
				it.subIt.TagResults(tags)
				it.pathMap[res] = append(it.pathMap[res], tags)
			}
		}
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
		if _, ok := it.seen[val]; ok {
			continue
		}
		it.seen[val] = seenAt{
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
	if at, ok = it.seen[val]; !ok {
		panic("trying to getBaseValue of something unseen")
	}
	for at.depth != 1 {
		if at.depth == 0 {
			panic("seen chain is broken")
		}
		at = it.seen[at.val]
	}
	return at.val
}

func (it *Recursive) Contains(val graph.Value) bool {
	graph.ContainsLogIn(it, val)
	it.pathIndex = 0
	if at, ok := it.seen[val]; ok {
		it.containsValue = it.getBaseValue(val)
		it.result.depth = at.depth
		it.result.val = val
		return graph.ContainsLogOut(it, val, true)
	}
	for it.Next() {
		if it.Result() == val {
			return graph.ContainsLogOut(it, val, true)
		}
	}
	return graph.ContainsLogOut(it, val, false)
}

func (it *Recursive) NextPath() bool {
	if len(it.pathMap[it.containsValue]) <= it.pathIndex+1 {
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

func (it *Recursive) Describe() graph.Description {
	base := it.qs.FixedIterator()
	base.Add(Int64Node(20))
	fanoutdesc := it.morphism(it.qs, base).Describe()
	subIts := []graph.Description{
		it.subIt.Describe(),
		fanoutdesc,
	}

	return graph.Description{
		UID:       it.UID(),
		Type:      it.Type(),
		Tags:      it.tags.Tags(),
		Iterators: subIts,
	}
}
