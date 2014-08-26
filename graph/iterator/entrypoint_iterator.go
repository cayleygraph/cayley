package iterator

import (
	"fmt"

	"github.com/google/cayley/graph"
)

type EntryPoint struct {
	uid       uint64
	primaryIt graph.Iterator
}

func NewEntryPoint(it graph.Iterator) *EntryPoint {
	return &EntryPoint{
		uid:       NextUID(),
		primaryIt: it,
	}
}

func (it *EntryPoint) UID() uint64 {
	return it.uid
}

func (it *EntryPoint) Tagger() *graph.Tagger {
	return nil
}

func (it *EntryPoint) TagResults(dst map[string]graph.Value) {
	it.primaryIt.TagResults(dst)
}

func (it *EntryPoint) Contains(val graph.Value) bool {
	return it.primaryIt.Contains(val)
}

func (it *EntryPoint) Clone() graph.Iterator {
	return NewEntryPoint(it.primaryIt)
}

func (it *EntryPoint) Type() graph.Type { return it.primaryIt.Type() }

func (it *EntryPoint) Reset() {
	it.primaryIt.Reset()
}

func (it *EntryPoint) SetIterator(iterator graph.Iterator) {
	it.primaryIt = iterator
	it.primaryIt.Reset()
}

func (it *EntryPoint) Next() bool {
	return graph.Next(it.primaryIt)
}

func (it *EntryPoint) Result() graph.Value {
	return it.primaryIt.Result()
}

func (it *EntryPoint) NextPath() bool {
	return it.primaryIt.NextPath()
}

func (it *EntryPoint) Stats() graph.IteratorStats {
	return it.primaryIt.Stats()
}

func (it *EntryPoint) Size() (int64, bool) {
	return it.Stats().Size, false
}

func (it *EntryPoint) Optimize() (graph.Iterator, bool) {
	return it, false
}

func (it *EntryPoint) SubIterators() []graph.Iterator {
	return []graph.Iterator{it.primaryIt}
}

func (it *EntryPoint) DebugString(indent int) string {
	return fmt.Sprintf("todo")
}

func (it *EntryPoint) Close() {
	it.primaryIt.Close()
}

// DEPRECATED
func (it *EntryPoint) ResultTree() *graph.ResultTree {
	return graph.NewResultTree(it.Result())
}
