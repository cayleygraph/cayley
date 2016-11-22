package rethinkdb

import (
	"gopkg.in/dancannon/gorethink.v2"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/quad"
)

type itType int

const (
	regular itType = iota
	all
	comparison
)

type Iterator struct {
	uid    uint64
	tagger graph.Tagger
	qs     *QuadStore
	dir    quad.Direction
	iter   *gorethink.Cursor
	hash   NodeHash
	size   int64
	query  gorethink.Term
	table  string
	result graph.Value
	err    error
	typ    itType
}

func (it *Iterator) makeRDBIterator() (c *gorethink.Cursor) {
	if clog.V(5) {
		clog.Infof("Making RDB iterator: %v", it.query)
	}

	var err error
	if c, err = it.query.Run(it.qs.session, gorethink.RunOpts{
		ReadMode: it.qs.readMode,
	}); err != nil {
		clog.Errorf("Error: Couldn't make rdb cursor/iterator: %v", err)
		return
	}
	return
}

func NewIterator(qs *QuadStore, table string, d quad.Direction, val graph.Value) *Iterator {
	h := val.(NodeHash)

	return &Iterator{
		uid:   iterator.NextUID(),
		query: gorethink.Table(quadTableName).GetAllByIndex(d.String(), string(h)),
		table: table,
		qs:    qs,
		dir:   d,
		size:  -1,
		hash:  h,
		typ:   regular,
	}
}

func NewAllIterator(qs *QuadStore, table string) *Iterator {
	return &Iterator{
		uid:   iterator.NextUID(),
		qs:    qs,
		query: gorethink.Table(table),
		table: table,
		size:  -1,
		typ:   all,
	}
}

func NewComparisonIterator(qs *QuadStore, table string, query gorethink.Term) *Iterator {
	return &Iterator{
		uid:   iterator.NextUID(),
		qs:    qs,
		query: query,
		table: table,
		size:  -1,
		typ:   comparison,
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
	return &it.tagger
}

func (it *Iterator) TagResults(dst map[string]graph.Value) {
	for _, tag := range it.tagger.Tags() {
		dst[tag] = it.Result()
	}

	for tag, value := range it.tagger.Fixed() {
		dst[tag] = value
	}
}

func (it *Iterator) Clone() graph.Iterator {
	var m *Iterator
	switch it.typ {
	case regular:
		m = NewIterator(it.qs, it.table, it.dir, it.hash)
	case all:
		m = NewAllIterator(it.qs, it.table)
	case comparison:
		m = NewComparisonIterator(it.qs, it.table, it.query)
	}
	m.tagger.CopyFrom(it)
	return m
}

func (it *Iterator) Next() bool {
	var result Quad
	if it.iter == nil {
		it.iter = it.makeRDBIterator()
	}
	found := it.iter.Next(&result)
	if !found {
		if err := it.iter.Err(); err != nil {
			it.err = err
			clog.Errorf("Error Nexting Iterator: %v", err)
		}
		return false
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
	if it.typ == all {
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
		it.size, err = it.qs.getSize(it.query)
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
	if it.typ == all {
		return graph.All
	}
	return rethinkDBType
}

func (it *Iterator) Sorted() bool                     { return true }
func (it *Iterator) Optimize() (graph.Iterator, bool) { return it, false }

func (it *Iterator) Describe() graph.Description {
	size, _ := it.Size()
	return graph.Description{
		UID:       it.UID(),
		Name:      string(it.hash),
		Type:      it.Type(),
		Size:      size,
		Tags:      it.tagger.Tags(),
		Direction: it.dir,
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
