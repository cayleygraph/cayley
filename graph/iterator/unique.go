package iterator

import (
	"github.com/cayleygraph/cayley/graph"
)

// Unique iterator removes duplicate values from it's subiterator.
type Unique struct {
	uid      uint64
	tags     graph.Tagger
	subIt    graph.Iterator
	result   graph.Value
	runstats graph.IteratorStats
	err      error
	seen     map[interface{}]bool
}

func NewUnique(subIt graph.Iterator) *Unique {
	return &Unique{
		uid:   NextUID(),
		subIt: subIt,
		seen:  make(map[interface{}]bool),
	}
}

func (it *Unique) UID() uint64 {
	return it.uid
}

// Reset resets the internal iterators and the iterator itself.
func (it *Unique) Reset() {
	it.result = nil
	it.subIt.Reset()
	it.seen = make(map[interface{}]bool)
}

func (it *Unique) Tagger() *graph.Tagger {
	return &it.tags
}

func (it *Unique) TagResults(dst map[string]graph.Value) {
	for _, tag := range it.tags.Tags() {
		dst[tag] = it.Result()
	}

	for tag, value := range it.tags.Fixed() {
		dst[tag] = value
	}

	if it.subIt != nil {
		it.subIt.TagResults(dst)
	}
}

func (it *Unique) Clone() graph.Iterator {
	uniq := NewUnique(it.subIt.Clone())
	uniq.tags.CopyFrom(it)
	return uniq
}

// SubIterators returns a slice of the sub iterators. The first iterator is the
// primary iterator, for which the complement is generated.
func (it *Unique) SubIterators() []graph.Iterator {
	return []graph.Iterator{it.subIt}
}

// Next advances the subiterator, continuing until it returns a value which it
// has not previously seen.
func (it *Unique) Next() bool {
	graph.NextLogIn(it)
	it.runstats.Next += 1

	for it.subIt.Next() {
		curr := it.subIt.Result()
		var key interface{} = curr
		if v, ok := curr.(graph.Keyer); ok {
			key = v.Key()
		}
		if ok := it.seen[key]; !ok {
			it.result = curr
			it.seen[key] = true
			return graph.NextLogOut(it, true)
		}
	}
	it.err = it.subIt.Err()
	return graph.NextLogOut(it, false)
}

func (it *Unique) Err() error {
	return it.err
}

func (it *Unique) Result() graph.Value {
	return it.result
}

// Contains checks whether the passed value is part of the primary iterator,
// which is irrelevant for uniqueness.
func (it *Unique) Contains(val graph.Value) bool {
	graph.ContainsLogIn(it, val)
	it.runstats.Contains += 1
	return graph.ContainsLogOut(it, val, it.subIt.Contains(val))
}

// NextPath for unique always returns false. If we were to return multiple
// paths, we'd no longer be a unique result, so we have to choose only the first
// path that got us here. Unique is serious on this point.
func (it *Unique) NextPath() bool {
	return false
}

// Close closes the primary iterators.
func (it *Unique) Close() error {
	it.seen = nil
	return it.subIt.Close()
}

func (it *Unique) Type() graph.Type { return graph.Unique }

func (it *Unique) Optimize() (graph.Iterator, bool) {
	newIt, optimized := it.subIt.Optimize()
	if optimized {
		it.subIt = newIt
	}
	return it, false
}

const uniquenessFactor = 2

func (it *Unique) Stats() graph.IteratorStats {
	subStats := it.subIt.Stats()
	return graph.IteratorStats{
		NextCost:     subStats.NextCost * uniquenessFactor,
		ContainsCost: subStats.ContainsCost,
		Size:         subStats.Size / uniquenessFactor,
		ExactSize:    false,
		Next:         it.runstats.Next,
		Contains:     it.runstats.Contains,
		ContainsNext: it.runstats.ContainsNext,
	}
}

func (it *Unique) Size() (int64, bool) {
	st := it.Stats()
	return st.Size, st.ExactSize
}

func (it *Unique) Describe() graph.Description {
	subIts := []graph.Description{
		it.subIt.Describe(),
	}

	return graph.Description{
		UID:       it.UID(),
		Type:      it.Type(),
		Tags:      it.tags.Tags(),
		Iterators: subIts,
	}
}

var _ graph.Iterator = &Unique{}
