// Copyright 2014 The Cayley Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nosql

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/quad"
)

func (qs *QuadStore) OptimizeIterator(it graph.Iterator) (graph.Iterator, bool) {
	switch it.Type() {
	case graph.LinksTo:
		return qs.optimizeLinksTo(it.(*iterator.LinksTo))
	//case graph.And:
	//	return qs.optimizeAndIterator(it.(*iterator.And))
	case graph.Comparison:
		return qs.optimizeComparison(it.(*iterator.Comparison))
	}
	return it, false
}

//func (qs *QuadStore) optimizeAndIterator(it *iterator.And) (graph.Iterator, bool) {
//	// Fail fast if nothing can happen
//	if clog.V(4) {
//		clog.Infof("Entering optimizeAndIterator %v", it.UID())
//	}
//	found := false
//	for _, it := range it.SubIterators() {
//		if clog.V(4) {
//			clog.Infof("%v", it.Type())
//		}
//		if _, ok := it.(*Iterator); ok {
//			found = true
//		}
//	}
//	if !found {
//		if clog.V(4) {
//			clog.Infof("Aborting optimizeAndIterator")
//		}
//		return it, false
//	}
//
//	newAnd := iterator.NewAnd(qs)
//	var mongoIt *Iterator
//	for _, it := range it.SubIterators() {
//		if sit, ok := it.(*Iterator); ok {
//			if mongoIt == nil {
//				mongoIt = sit
//			} else {
//				newAnd.AddSubIterator(it)
//			}
//			continue
//		}
//		switch it.Type() {
//		case graph.LinksTo:
//			continue
//		default:
//			newAnd.AddSubIterator(it)
//		}
//	}
//	stats := mongoIt.Stats()
//
//	lset := []graph.Linkage{
//		{
//			Dir:   mongoIt.dir,
//			Value: mongoIt.hash,
//		},
//	}
//
//	n := 0
//	for _, it := range it.SubIterators() {
//		if it.Type() == graph.LinksTo {
//			lto := it.(*iterator.LinksTo)
//			// Is it more effective to do the replacement, or let the mongo check the linksto?
//			ltostats := lto.Stats()
//			if (ltostats.ContainsCost+stats.NextCost)*stats.Size > (ltostats.NextCost+stats.ContainsCost)*ltostats.Size {
//				continue
//			}
//			newLto := NewLinksTo(qs, lto.SubIterators()[0], "quads", lto.Direction(), lset)
//			newAnd.AddSubIterator(newLto)
//			n++
//		}
//	}
//	if n == 0 {
//		return it, false
//	}
//
//	return newAnd.Optimize()
//}

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
	var filter Filter
	switch it.Operator() {
	case iterator.CompareGT:
		filter = GT
	case iterator.CompareGTE:
		filter = GTE
	case iterator.CompareLT:
		filter = LT
	case iterator.CompareLTE:
		filter = LTE
	default:
		return it, false
	}
	fieldPath := func(s string) []string {
		return []string{fldValue, s}
	}

	var constraints []FieldFilter
	switch v := it.Value().(type) {
	case quad.String:
		constraints = []FieldFilter{
			{Path: fieldPath(fldValData), Filter: filter, Value: String(v)},
			{Path: fieldPath(fldIRI), Filter: NotEqual, Value: Bool(true)},
			{Path: fieldPath(fldBNode), Filter: NotEqual, Value: Bool(true)},
			{Path: fieldPath(fldRaw), Filter: NotEqual, Value: Bool(true)},
		}
	case quad.IRI:
		constraints = []FieldFilter{
			{Path: fieldPath(fldValData), Filter: filter, Value: String(v)},
			{Path: fieldPath(fldIRI), Filter: Equal, Value: Bool(true)},
		}
	case quad.BNode:
		constraints = []FieldFilter{
			{Path: fieldPath(fldValData), Filter: filter, Value: String(v)},
			{Path: fieldPath(fldBNode), Filter: Equal, Value: Bool(true)},
		}
	case quad.Int:
		constraints = []FieldFilter{
			{Path: fieldPath(fldValInt), Filter: filter, Value: Int(v)},
		}
	case quad.Float:
		constraints = []FieldFilter{
			{Path: fieldPath(fldValFloat), Filter: filter, Value: Float(v)},
		}
	case quad.Time:
		constraints = []FieldFilter{
			{Path: fieldPath(fldValTime), Filter: filter, Value: Time(v)},
		}
	default:
		return it, false
	}
	return NewIteratorWithConstraints(qs, mit.collection, constraints), true
}
