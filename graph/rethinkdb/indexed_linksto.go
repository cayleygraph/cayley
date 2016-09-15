package rethinkdb

import (
	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/quad"
	gorethink "gopkg.in/dancannon/gorethink.v2"
)

var _ graph.Iterator = &LinksTo{}

var linksToType graph.Type

func init() {
	linksToType = graph.RegisterIterator("rethinkdb-linksto")
}

type LinksTo struct {
	uid       uint64
	table     string
	tagger    graph.Tagger
	qs        *QuadStore
	primaryIt graph.Iterator
	dir       quad.Direction
	query     *gorethink.Term
	nextIt    *gorethink.Cursor
	result    graph.Value
	runstats  graph.IteratorStats
	linkage   graph.Linkage
	err       error
}

func NewLinksTo(qs *QuadStore, it graph.Iterator, table string, d quad.Direction, linkage graph.Linkage) *LinksTo {
	return &LinksTo{
		uid:       iterator.NextUID(),
		qs:        qs,
		primaryIt: it,
		dir:       d,
		nextIt:    nil,
		linkage:   linkage,
		table:     table,
	}
}

func mapToDirLinkIndex(lset [2]graph.Linkage) (string, interface{}) {
	dir1 := lset[0].Dir
	dir2 := lset[1].Dir

	getHash := func(i int, dir quad.Direction) NodeHash {
		switch v := lset[i].Value.(type) {
		case QuadHash:
			return v.Get(dir)
		case NodeHash:
			return v
		default:
			return ""
		}
	}

	val1 := string(getHash(0, dir1))
	val2 := string(getHash(1, dir2))

	if index, ok := dirLinkIndexMap[[2]quad.Direction{dir1, dir2}]; ok {
		return index, []interface{}{val1, val2}
	}

	// reversed
	if index, ok := dirLinkIndexMap[[2]quad.Direction{dir2, dir2}]; ok {
		return index, []interface{}{val2, val1}
	}

	clog.Errorf("Unable to map dir links")
	return "", nil
}

func (it *LinksTo) buildIteratorFor(d quad.Direction, val graph.Value) *gorethink.Cursor {
	var hash NodeHash
	switch v := val.(type) {
	case NodeHash:
		hash = v
	case QuadHash:
		hash = NodeHash(v.Get(d))
	}

	index, value := mapToDirLinkIndex([2]graph.Linkage{
		graph.Linkage{Dir: d, Value: hash},
		it.linkage,
	})

	query := gorethink.Table(it.table).GetAllByIndex(index, value)

	if clog.V(5) {
		clog.Infof("Running RDB query: %s", query)
	}

	it.query = &query

	c, err := it.query.Run(it.qs.session)
	if err != nil {
		clog.Errorf("Error: Couldn't build iterator for %v: %v", val, err)
		return nil
	}
	return c
}

func (it *LinksTo) UID() uint64 {
	return it.uid
}

func (it *LinksTo) Tagger() *graph.Tagger {
	return &it.tagger
}

// Return the direction under consideration.
func (it *LinksTo) Direction() quad.Direction { return it.dir }

// Tag these results, and our subiterator's results.
func (it *LinksTo) TagResults(dst map[string]graph.Value) {
	for _, tag := range it.tagger.Tags() {
		dst[tag] = it.Result()
	}

	for tag, value := range it.tagger.Fixed() {
		dst[tag] = value
	}

	it.primaryIt.TagResults(dst)
}

// Optimize the LinksTo, by replacing it if it can be.
func (it *LinksTo) Optimize() (graph.Iterator, bool) {
	return it, false
}

func (it *LinksTo) Next() bool {
	var result Quad
	graph.NextLogIn(it)

	for {
		it.runstats.Next++
		if it.nextIt != nil && it.nextIt.Next(&result) {
			it.runstats.ContainsNext++
			it.result = QuadHash{
				NodeHash(result.Subject),
				NodeHash(result.Predicate),
				NodeHash(result.Object),
				NodeHash(result.Label),
			}
			return graph.NextLogOut(it, true)
		}

		if it.nextIt != nil {
			it.err = it.nextIt.Err()
			if it.err != nil {
				return false
			}

		}
		if !it.primaryIt.Next() {
			it.err = it.primaryIt.Err()

			return graph.NextLogOut(it, false)
		}
		if it.nextIt != nil {
			it.nextIt.Close()
		}

		it.nextIt = it.buildIteratorFor(it.dir, it.primaryIt.Result())
	}
}

func (it *LinksTo) Err() error {
	return it.err
}

func (it *LinksTo) Result() graph.Value {
	return it.result
}

func (it *LinksTo) Close() error {
	var err error
	if it.nextIt != nil {
		err = it.nextIt.Close()
	}

	_err := it.primaryIt.Close()
	if _err != nil && err == nil {
		err = _err
	}

	return err
}

func (it *LinksTo) NextPath() bool {
	ok := it.primaryIt.NextPath()
	if !ok {
		it.err = it.primaryIt.Err()
	}
	return ok
}

func (it *LinksTo) Type() graph.Type {
	return linksToType
}

func (it *LinksTo) Clone() graph.Iterator {
	m := NewLinksTo(it.qs, it.primaryIt.Clone(), it.table, it.dir, it.linkage)
	m.tagger.CopyFrom(it)
	return m
}

func (it *LinksTo) Contains(val graph.Value) bool {
	graph.ContainsLogIn(it, val)
	it.runstats.Contains++

	dval := it.qs.QuadDirection(val, it.linkage.Dir)
	if dval != it.linkage.Value {
		return graph.ContainsLogOut(it, val, false)
	}

	node := it.qs.QuadDirection(val, it.dir)
	if it.primaryIt.Contains(node) {
		it.result = val
		return graph.ContainsLogOut(it, val, true)
	}
	it.err = it.primaryIt.Err()
	return graph.ContainsLogOut(it, val, false)
}

func (it *LinksTo) Describe() graph.Description {
	primary := it.primaryIt.Describe()
	return graph.Description{
		UID:       it.UID(),
		Type:      it.Type(),
		Direction: it.dir,
		Iterator:  &primary,
	}
}

func (it *LinksTo) Reset() {
	it.primaryIt.Reset()
	if it.nextIt != nil {
		it.nextIt.Close()
	}
	it.err = nil
	it.nextIt = nil
}

// Return a guess as to how big or costly it is to next the iterator.
func (it *LinksTo) Stats() graph.IteratorStats {
	subitStats := it.primaryIt.Stats()

	fanoutFactor := int64(20)
	checkConstant := int64(1)
	nextConstant := int64(2)

	size := fanoutFactor * subitStats.Size
	if it.query != nil {
		csize, _ := it.qs.getSize(*it.query)
		if size > csize {
			size = csize
		}
	}

	return graph.IteratorStats{
		NextCost:     nextConstant + subitStats.NextCost,
		ContainsCost: checkConstant + subitStats.ContainsCost,
		Size:         size,
		Next:         it.runstats.Next,
		Contains:     it.runstats.Contains,
		ContainsNext: it.runstats.ContainsNext,
	}
}

func (it *LinksTo) Size() (int64, bool) {
	return it.Stats().Size, false
}

// Return a list containing only our subiterator.
func (it *LinksTo) SubIterators() []graph.Iterator {
	return []graph.Iterator{it.primaryIt}
}
