package rethinkdb

import (
	"gopkg.in/dancannon/gorethink.v2"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/quad"
)

type Iterator struct {
	uid        uint64
	tags       graph.Tagger
	qs         *QuadStore
	dir        quad.Direction
	iter       *gorethink.Cursor
	hash       NodeHash
	size       int64
	isAll      bool
	constraint *gorethink.Term
	table      string
	result     graph.Value
	err        error
}

func NewIterator(qs *QuadStore, table string, d quad.Direction, val graph.Value) *Iterator {
	h := val.(NodeHash)

	constraint := gorethink.Row.Field(d.String()).Eq(string(h))

	return &Iterator{
		uid:        iterator.NextUID(),
		constraint: &constraint,
		table:      table,
		qs:         qs,
		dir:        d,
		iter:       nil,
		size:       -1,
		hash:       h,
		isAll:      false,
	}
}

func (it *Iterator) makeRDBIterator() (c *gorethink.Cursor) {
	var query gorethink.Term
	if it.isAll {
		query = gorethink.Table(it.table)
	} else {
		query = gorethink.Table(it.table).Filter(*it.constraint)
	}

	if clog.V(5) {
		// Debug
		clog.Infof("Running RDB query: %+v", query)
	}

	var err error
	if c, err = query.Run(it.qs.session); err != nil {
		clog.Errorf("Error: Couldn't make rdb cursor/iterator: %v", err)
		return
	}
	return
}

func NewAllIterator(qs *QuadStore, table string) *Iterator {
	return &Iterator{
		uid:        iterator.NextUID(),
		qs:         qs,
		dir:        quad.Any,
		constraint: nil,
		table:      table,
		iter:       nil,
		size:       -1,
		hash:       "",
		isAll:      true,
	}
}

func NewIteratorWithConstraints(qs *QuadStore, table string, constraint gorethink.Term) *Iterator {
	return &Iterator{
		uid:        iterator.NextUID(),
		qs:         qs,
		dir:        quad.Any,
		constraint: &constraint,
		table:      table,
		iter:       nil,
		size:       -1,
		hash:       "",
		isAll:      false,
	}
}

func (it *Iterator) UID() uint64 {
	return it.uid
}

func (it *Iterator) Reset() {
	it.Close()
	it.iter = it.makeRDBIterator()
	it.err = nil
}

func (it *Iterator) Close() error {
	if it.iter != nil {
		return it.iter.Close()
	}
	return nil
}

func (it *Iterator) Tagger() *graph.Tagger {
	return &it.tags
}

func (it *Iterator) TagResults(dst map[string]graph.Value) {
	for _, tag := range it.tags.Tags() {
		dst[tag] = it.Result()
	}

	for tag, value := range it.tags.Fixed() {
		dst[tag] = value
	}
}

func (it *Iterator) Clone() graph.Iterator {
	var m *Iterator
	if it.isAll {
		m = NewAllIterator(it.qs, it.table)
	} else {
		m = NewIterator(it.qs, it.table, it.dir, NodeHash(it.hash))
	}
	m.tags.CopyFrom(it)
	return m
}

func (it *Iterator) Next() bool {
	var result Quad
	if it.iter == nil {
		it.iter = it.makeRDBIterator()
	}
	found := it.iter.Next(&result)
	if !found {
		err := it.iter.Err()
		if err != nil {
			it.err = err
			clog.Errorf("Error Nexting Iterator: %v", err)
		}
		return false
	}
	if it.table == quadTableName && len(result.Added) <= len(result.Deleted) {
		return it.Next()
	}
	if it.table == quadTableName {
		it.result = QuadHash{
			NodeHash(result.Subject),
			NodeHash(result.Predicate),
			NodeHash(result.Object),
			NodeHash(result.Label),
		}
	} else {
		it.result = NodeHash(result.ID)
	}
	return true
}

func (it *Iterator) Err() error {
	return it.err
}

func (it *Iterator) Result() graph.Value {
	return it.result
}

func (it *Iterator) NextPath() bool {
	return false
}

func (it *Iterator) SubIterators() []graph.Iterator {
	return nil
}

func (it *Iterator) Contains(v graph.Value) bool {
	graph.ContainsLogIn(it, v)
	if it.isAll {
		it.result = v
		return graph.ContainsLogOut(it, v, true)
	}
	val := NodeHash(v.(QuadHash).Get(it.dir))
	if val == it.hash {
		it.result = v
		return graph.ContainsLogOut(it, v, true)
	}
	return graph.ContainsLogOut(it, v, false)
}

func (it *Iterator) Size() (int64, bool) {
	if it.size == -1 {
		var err error
		it.size, err = it.qs.getSize(it.table, it.constraint)
		if err != nil {
			it.err = err
		}
	}
	return it.size, true
}

var rethinkDBType graph.Type

func init() {
	rethinkDBType = graph.RegisterIterator("rethinkdb")
}

func Type() graph.Type { return rethinkDBType }

func (it *Iterator) Type() graph.Type {
	if it.isAll {
		return graph.All
	}
	return rethinkDBType
}

func (it *Iterator) Sorted() bool                     { return true }
func (it *Iterator) Optimize() (graph.Iterator, bool) { return it, false }

func (it *Iterator) Describe() graph.Description {
	size, _ := it.Size()
	return graph.Description{
		UID:  it.UID(),
		Name: string(it.hash),
		Type: it.Type(),
		Size: size,
	}
}

func (it *Iterator) Stats() graph.IteratorStats {
	size, _ := it.Size()
	return graph.IteratorStats{
		ContainsCost: 1,
		NextCost:     5,
		Size:         size,
	}
}

var _ graph.Iterator = &Iterator{}
