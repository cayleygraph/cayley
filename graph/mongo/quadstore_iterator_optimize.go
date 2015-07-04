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

package mongo

import (
	"github.com/barakmich/glog"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
)

func (qs *QuadStore) OptimizeIterator(it graph.Iterator) (graph.Iterator, bool) {
	switch it.Type() {
	case graph.LinksTo:
		return qs.optimizeLinksTo(it.(*iterator.LinksTo))
	case graph.And:
		return qs.optimizeAndIterator(it.(*iterator.And))

	}
	return it, false
}

func (qs *QuadStore) optimizeAndIterator(it *iterator.And) (graph.Iterator, bool) {
	// Fail fast if nothing can happen
	glog.V(4).Infoln("Entering optimizeAndIterator", it.UID())
	found := false
	for _, it := range it.SubIterators() {
		glog.V(4).Infoln(it.Type())
		if it.Type() == mongoType {
			found = true
		}
	}
	if !found {
		glog.V(4).Infoln("Aborting optimizeAndIterator")
		return it, false
	}

	newAnd := iterator.NewAnd(qs)
	var mongoIt *Iterator
	for _, it := range it.SubIterators() {
		switch it.Type() {
		case mongoType:
			if mongoIt == nil {
				mongoIt = it.(*Iterator)
			} else {
				newAnd.AddSubIterator(it)
			}
		case graph.LinksTo:
			continue
		default:
			newAnd.AddSubIterator(it)
		}
	}
	stats := mongoIt.Stats()

	lset := []graph.Linkage{
		{
			Dir:    mongoIt.dir,
			Value: qs.ValueOf(mongoIt.name),
		},
	}

	n := 0
	for _, it := range it.SubIterators() {
		if it.Type() == graph.LinksTo {
			lto := it.(*iterator.LinksTo)
			// Is it more effective to do the replacement, or let the mongo check the linksto?
			ltostats := lto.Stats()
			if (ltostats.ContainsCost+stats.NextCost)*stats.Size > (ltostats.NextCost+stats.ContainsCost)*ltostats.Size {
				continue
			}
			newLto := NewLinksTo(qs, lto.SubIterators()[0], "quads", lto.Direction(), lset)
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
			if !graph.Next(primary) {
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
