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
	if clog.V(4) {
		clog.Infof("Entering OptimizeIterator %v", it.UID())
	}
	switch it.Type() {
	case graph.LinksTo:
		return qs.optimizeLinksToIterator(it.(*iterator.LinksTo))
	case graph.And:
		return qs.optimizeAndIterator(it.(*iterator.And))
	case graph.Comparison:
		return qs.optimizeComparisonIterator(it.(*iterator.Comparison))
	case graph.Limit:
		return qs.optimizeLimitIterator(it.(*iterator.Limit))
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

	linkage := graph.Linkage{
		Dir:   rethinkDBIt.dir,
		Value: rethinkDBIt.hash,
	}

	n := 0
	for _, it := range it.SubIterators() {
		if it.Type() == graph.LinksTo {
			lto := it.(*iterator.LinksTo)
			ltostats := lto.Stats()
			if (ltostats.ContainsCost+stats.NextCost)*stats.Size > (ltostats.NextCost+stats.ContainsCost)*ltostats.Size {
				continue
			}
			newLto := NewLinksTo(qs, lto.SubIterators()[0], quadTableName, lto.Direction(), linkage)
			newAnd.AddSubIterator(newLto)
			n++
		}
	}
	if n == 0 {
		return it, false
	}

	return newAnd.Optimize()
}

func (qs *QuadStore) optimizeLinksToIterator(it *iterator.LinksTo) (graph.Iterator, bool) {
	subs := it.SubIterators()
	if len(subs) != 1 {
		return it, false
	}
	primary := subs[0]

	switch primary.Type() {
	case graph.Fixed:
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

func (qs *QuadStore) optimizeComparisonIterator(it *iterator.Comparison) (graph.Iterator, bool) {
	subs := it.SubIterators()
	if len(subs) != 1 {
		return it, false
	}
	mit, ok := subs[0].(*Iterator)
	if !ok || mit.typ != all {
		return it, false
	}

	comparer := func(q gorethink.Term, index string, typ dbType, val interface{}) gorethink.Term {
		v := []interface{}{typ, val}
		vt := [2]interface{}{
			typ,
		}
		switch it.Operator() {
		case iterator.CompareGT:
			vt[1] = gorethink.MaxVal
			q = q.Between(v, vt, gorethink.BetweenOpts{
				Index:      index,
				LeftBound:  "open",
				RightBound: "closed",
			})
		case iterator.CompareGTE:
			vt[1] = gorethink.MaxVal
			q = q.Between(v, vt, gorethink.BetweenOpts{
				Index:      index,
				LeftBound:  "closed",
				RightBound: "closed",
			})
		case iterator.CompareLT:
			vt[1] = gorethink.MinVal
			q = q.Between(vt, v, gorethink.BetweenOpts{
				Index:      index,
				RightBound: "open",
				LeftBound:  "closed",
			})
		case iterator.CompareLTE:
			vt[1] = gorethink.MinVal
			q = q.Between(vt, v, gorethink.BetweenOpts{
				Index:      index,
				RightBound: "closed",
				LeftBound:  "closed",
			})
		default:
			clog.Errorf("Unknown operator: %v", it.Operator())
		}
		return q
	}

	q := gorethink.Table(mit.table)

	switch v := it.Value().(type) {
	case quad.String:
		q = comparer(q, "val_string", dbString, string(v))
	case quad.IRI:
		q = comparer(q, "val_string", dbIRI, string(v))
	case quad.BNode:
		q = comparer(q, "val_string", dbBNode, string(v))
	case quad.Int:
		q = comparer(q, "val_int", dbInt, int64(v))
	case quad.Float:
		q = comparer(q, "val_float", dbFloat, float64(v))
	case quad.Time:
		q = comparer(q, "val_time", dbTime, time.Time(v))
	case quad.Raw:
		q = comparer(q, "val_bytes", dbRaw, []byte(v))
	default:
		clog.Errorf("Unknown type: %+v", v)
		return it, false
	}
	return NewComparisonIterator(qs, mit.table, q), true
}

func (qs *QuadStore) optimizeLimitIterator(it *iterator.Limit) (graph.Iterator, bool) {
	subs := it.SubIterators()
	if len(subs) != 1 {
		return it, false
	}
	primaryIt, ok := subs[0].(*Iterator)
	if !ok {
		return it, false
	}
	limit, _ := it.Size()
	primaryIt.query = primaryIt.query.Limit(limit)
	return primaryIt, true
}
