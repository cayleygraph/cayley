package iterator

import "github.com/google/cayley/graph"

// Not iterator acts like a set difference between the primary iterator
// and the forbidden iterator.
type Not struct {
	uid       uint64
	tags      graph.Tagger
	ts        graph.TripleStore
	primaryIt graph.Iterator
	allIt     graph.Iterator
	result    graph.Value
	runstats  graph.IteratorStats
}

func NewNot(ts graph.TripleStore, primaryIt graph.Iterator) *Not {
	return &Not{
		uid:       NextUID(),
		ts:        ts,
		allIt:     ts.NodesAllIterator(),
		primaryIt: primaryIt,
	}
}

func (it *Not) UID() uint64 {
	return it.uid
}

func (it *Not) Reset() {
	it.result = nil
	it.primaryIt.Reset()
	it.allIt = it.ts.NodesAllIterator()
}

func (it *Not) Tagger() *graph.Tagger {
	return &it.tags
}

func (it *Not) TagResults(dst map[string]graph.Value) {
	for _, tag := range it.tags.Tags() {
		dst[tag] = it.Result()
	}

	for tag, value := range it.tags.Fixed() {
		dst[tag] = value
	}

	if it.primaryIt != nil {
		it.primaryIt.TagResults(dst)
	}
}

func (it *Not) Clone() graph.Iterator {
	not := NewNot(it.ts, it.primaryIt.Clone())
	not.tags.CopyFrom(it)
	return not
}

func (it *Not) SubIterators() []graph.Iterator {
	return []graph.Iterator{it.primaryIt, it.allIt}
}

func (it *Not) ResultTree() *graph.ResultTree {
	tree := graph.NewResultTree(it.Result())
	tree.AddSubtree(it.primaryIt.ResultTree())
	tree.AddSubtree(it.allIt.ResultTree())
	return tree
}

func (it *Not) DebugString(indent int) string {
	return "todo"
}

func (it *Not) Next() bool {
	graph.NextLogIn(it)
	it.runstats.Next += 1

	for graph.Next(it.primaryIt) {
		if curr := it.allIt.Result(); !it.primaryIt.Contains(curr) {
			it.result = curr
			it.runstats.ContainsNext += 1
			return graph.NextLogOut(it, curr, true)
		}
	}
	return graph.NextLogOut(it, nil, false)
}

func (it *Not) Result() graph.Value {
	return it.result
}

func (it *Not) Contains(val graph.Value) bool {
	graph.ContainsLogIn(it, val)
	it.runstats.Contains += 1

	if it.primaryIt.Contains(val) {
		return graph.ContainsLogOut(it, val, false)
	}

	// TODO - figure out if this really needs to be checked or it's safe to return true directly
	return graph.ContainsLogOut(it, val, it.allIt.Contains(val))
}

// TODO
func (it *Not) NextPath() bool {
	if it.primaryIt.NextPath() {
		return true
	}
	return false
}

func (it *Not) Close() {
	it.primaryIt.Close()
	it.allIt.Close()
}

func (it *Not) Type() graph.Type { return graph.Not }

// TODO - call optimize for the primaryIt and allIt?
func (it *Not) Optimize() (graph.Iterator, bool) {
	//it.forbiddenIt = NewMaterialize(it.forbiddenIt)
	return it, false
}

// TODO
func (it *Not) Stats() graph.IteratorStats {
	subitStats := it.primaryIt.Stats()
	// TODO(barakmich): These should really come from the triplestore itself
	fanoutFactor := int64(20)
	checkConstant := int64(1)
	nextConstant := int64(2)
	return graph.IteratorStats{
		NextCost:     nextConstant + subitStats.NextCost,
		ContainsCost: checkConstant + subitStats.ContainsCost,
		Size:         fanoutFactor * subitStats.Size,
		Next:         it.runstats.Next,
		Contains:     it.runstats.Contains,
		ContainsNext: it.runstats.ContainsNext,
	}
}

func (it *Not) Size() (int64, bool) {
	return it.Stats().Size, false
}
