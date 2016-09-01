package rethinkdb

import (
	"time"

	"gopkg.in/dancannon/gorethink.v2"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/quad"
)

func (qs *QuadStore) OptimizeIterator(it graph.Iterator) (graph.Iterator, bool) {
	switch it.Type() {
	case graph.LinksTo:
		return qs.optimizeLinksTo(it.(*iterator.LinksTo))
	case graph.And:
		return qs.optimizeAndIterator(it.(*iterator.And))
	case graph.Comparison:
		return qs.optimizeComparison(it.(*iterator.Comparison))
	}
	return it, false
}

func (qs *QuadStore) optimizeAndIterator(it *iterator.And) (graph.Iterator, bool) {
	// Fail fast if nothing can happen
	if clog.V(4) {
		clog.Infof("Entering optimizeAndIterator %v", it.UID())
	}
	found := false
	for _, it := range it.SubIterators() {
		if clog.V(4) {
			clog.Infof("%v", it.Type())
		}
		if it.Type() == rethinkDBType {
			found = true
		}
	}
	if !found {
		if clog.V(4) {
			clog.Infof("Aborting optimizeAndIterator")
		}
		return it, false
	}

	newAnd := iterator.NewAnd(qs)
	var rethinkDBIt *Iterator
	for _, it := range it.SubIterators() {
		switch it.Type() {
		case rethinkDBType:
			if rethinkDBIt == nil {
				rethinkDBIt = it.(*Iterator)
			} else {
				newAnd.AddSubIterator(it)
			}
		case graph.LinksTo:
			continue
		default:
			newAnd.AddSubIterator(it)
		}
	}
	stats := rethinkDBIt.Stats()

	lset := []graph.Linkage{
		{
			Dir:   rethinkDBIt.dir,
			Value: rethinkDBIt.hash,
		},
	}

	n := 0
	for _, it := range it.SubIterators() {
		if it.Type() == graph.LinksTo {
			lto := it.(*iterator.LinksTo)
			ltostats := lto.Stats()
			if (ltostats.ContainsCost+stats.NextCost)*stats.Size > (ltostats.NextCost+stats.ContainsCost)*ltostats.Size {
				continue
			}
			newLto := NewLinksTo(qs, lto.SubIterators()[0], quadTableName, lto.Direction(), lset)
			newAnd.AddSubIterator(newLto)
			n++
		}
	}
	if n == 0 {
		return it, false
	}

	return newAnd.Optimize()
}

func (qs *QuadStore) optimizeLinksTo(it *iterator.LinksTo) (graph.Iterator, bool) {
	subs := it.SubIterators()
	if len(subs) != 1 {
		return it, false
	}
	primary := subs[0]
	if primary.Type() == graph.Fixed {
		size, _ := primary.Size()
		if size == 1 {
			if !primary.Next() {
				panic("unexpected size during optimize")
			}
			val := primary.Result()
			newIt := qs.QuadIterator(it.Direction(), val)
			nt := newIt.Tagger()
			nt.CopyFrom(it)
			for _, tag := range primary.Tagger().Tags() {
				nt.AddFixed(tag, val)
			}
			it.Close()
			return newIt, true
		}
	}
	return it, false
}

func (qs *QuadStore) optimizeComparison(it *iterator.Comparison) (graph.Iterator, bool) {
	subs := it.SubIterators()
	if len(subs) != 1 {
		return it, false
	}
	mit, ok := subs[0].(*Iterator)
	if !ok || !mit.isAll {
		return it, false
	}

	comparer := func(t gorethink.Term) func(args ...interface{}) gorethink.Term {
		switch it.Operator() {
		case iterator.CompareGT:
			return t.Gt
		case iterator.CompareGTE:
			return t.Ge
		case iterator.CompareLT:
			return t.Lt
		case iterator.CompareLTE:
			return t.Le
		default:
			clog.Errorf("Unknown operator: %v", it.Operator())
			return t.Eq
		}
	}

	var constraint gorethink.Term

	switch v := it.Value().(type) {
	case quad.String:
		constraint = comparer(gorethink.Row.Field("val_string"))(string(v)).
			And(gorethink.Row.Field("type").Eq(dbString))
	case quad.IRI:
		constraint = comparer(gorethink.Row.Field("val_string"))(string(v)).
			And(gorethink.Row.Field("type").Eq(dbIRI))
	case quad.BNode:
		constraint = comparer(gorethink.Row.Field("val_string"))(string(v)).
			And(gorethink.Row.Field("type").Eq(dbBNode))
	case quad.Int:
		constraint = comparer(gorethink.Row.Field("val_int"))(int64(v)).
			And(gorethink.Row.Field("type").Eq(dbInt))
	case quad.Float:
		constraint = comparer(gorethink.Row.Field("val_float"))(float64(v)).
			And(gorethink.Row.Field("type").Eq(dbFloat))
	case quad.Time:
		constraint = comparer(gorethink.Row.Field("val_time"))(time.Time(v).UTC()).
			And(gorethink.Row.Field("type").Eq(dbTime))
	default:
		return it, false
	}
	return NewIteratorWithConstraints(qs, mit.table, constraint), true
}
