package iterator

import "github.com/google/cayley/graph"

// Not iterator acts like a set difference between the primary iterator
// and the forbidden iterator.
type Not struct {
	uid         uint64
	tags        graph.Tagger
	primaryIt   graph.Iterator
	forbiddenIt graph.Iterator
	result      graph.Value
	runstats    graph.IteratorStats
}

func NewNot(primaryIt, forbiddenIt graph.Iterator) *Not {
	return &Not{
		uid:         NextUID(),
		primaryIt:   primaryIt,
		forbiddenIt: forbiddenIt,
	}
}

func (it *Not) UID() uint64 {
	return it.uid
}

func (it *Not) Reset() {
	it.result = nil
	it.primaryIt.Reset()
	it.forbiddenIt.Reset()
}

func (it *Not) Tagger() *graph.Tagger {
	return &it.tags
}

// TODO
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
	not := NewNot(it.primaryIt.Clone(), it.forbiddenIt.Clone())
	not.tags.CopyFrom(it)
	return not
}

func (it *Not) SubIterators() []graph.Iterator {
	return []graph.Iterator{it.primaryIt, it.forbiddenIt}
}

func (it *Not) ResultTree() *graph.ResultTree {
	tree := graph.NewResultTree(it.Result())
	tree.AddSubtree(it.primaryIt.ResultTree())
	tree.AddSubtree(it.forbiddenIt.ResultTree())
	return tree
}

func (it *Not) DebugString(indent int) string {
	return "todo"
}

func (it *Not) Next() bool {
	graph.NextLogIn(it)
	it.runstats.Next += 1

	for graph.Next(it.primaryIt) {
		// Consider only the elements from the primary set which are not
		// contained in the forbidden set.
		if curr := it.primaryIt.Result(); !it.forbiddenIt.Contains(curr) {
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

	mainGood := it.primaryIt.Contains(val)
	if mainGood {
		mainGood = !it.forbiddenIt.Contains(val)
	}
	return graph.ContainsLogOut(it, val, mainGood)
}

func (it *Not) NextPath() bool {
	if it.primaryIt.NextPath() {
		return true
	}
	return it.forbiddenIt.NextPath()
}

func (it *Not) Close() {
	it.primaryIt.Close()
	it.forbiddenIt.Close()
}

func (it *Not) Type() graph.Type { return graph.Not }

// TODO
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
