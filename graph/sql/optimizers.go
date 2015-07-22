// Copyright 2015 The Cayley Authors. All rights reserved.
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

package sql

import (
	"errors"

	"github.com/barakmich/glog"
	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/quad"
)

func intersect(a *StatementIterator, b *StatementIterator) (*StatementIterator, error) {
	if a.stType != b.stType {
		return nil, errors.New("Cannot combine SQL iterators of two different types")
	}
	min := a.size
	if b.size < a.size {
		min = b.size
	}
	var where clause
	if a.where == nil {
		if b.where == nil {
			where = nil
		}
		where = b.where
	} else {
		if b.where == nil {
			where = a.where
		}
		where = joinClause{a.where, b.where, andClause}
	}
	out := &StatementIterator{
		uid:        iterator.NextUID(),
		qs:         a.qs,
		buildWhere: append(a.buildWhere, b.buildWhere...),
		tags:       append(a.tags, b.tags...),
		where:      where,
		stType:     a.stType,
		size:       min,
		dir:        a.dir,
	}
	out.tagger.CopyFrom(a)
	out.tagger.CopyFrom(b)
	if out.stType == node {
		out.buildWhere = append(out.buildWhere, baseClause{
			pair:   tableDir{"", a.dir},
			target: tableDir{b.tableName(), b.dir},
		})
	}
	return out, nil
}

func hasa(a *StatementIterator, d quad.Direction) (*StatementIterator, error) {
	if a.stType != link {
		return nil, errors.New("Can't take the HASA of a link SQL iterator")
	}

	out := &StatementIterator{
		uid:    iterator.NextUID(),
		qs:     a.qs,
		stType: node,
		dir:    d,
	}
	where := a.where
	for _, w := range a.buildWhere {
		w.pair.table = out.tableName()
		wherenew := joinClause{where, w, andClause}
		where = wherenew
	}
	out.where = where
	//out := &StatementIterator{
	//uid:        iterator.NextUID(),
	//qs:         a.qs,
	//stType:     node,
	//dir:        d,
	//buildWhere: a.buildWhere,
	//where:      a.where,
	//size:       -1,
	//}
	for k, v := range a.tagger.Fixed() {
		out.tagger.AddFixed(k, v)
	}
	var tags []tag
	for _, t := range a.tagger.Tags() {
		tags = append(tags, tag{
			pair: tableDir{
				table: out.tableName(),
				dir:   quad.Any,
			},
			t: t,
		})
	}
	out.tags = append(tags, a.tags...)
	return out, nil
}

func linksto(a *StatementIterator, d quad.Direction) (*StatementIterator, error) {
	if a.stType != node {
		return nil, errors.New("Can't take the LINKSTO of a node SQL iterator")
	}
	out := &StatementIterator{
		uid:    iterator.NextUID(),
		qs:     a.qs,
		stType: link,
		dir:    d,
		size:   -1,
	}
	where := a.where
	for _, w := range a.buildWhere {
		w.pair.table = a.tableName()
		wherenew := joinClause{where, w, andClause}
		where = wherenew
	}

	out.where = where
	out.buildWhere = []baseClause{
		baseClause{
			pair: tableDir{
				dir: d,
			},
			target: tableDir{
				table: a.tableName(),
				dir:   a.dir,
			},
		},
	}
	var tags []tag
	for _, t := range a.tagger.Tags() {
		tags = append(tags, tag{
			pair: tableDir{
				table: a.tableName(),
				dir:   a.dir,
			},
			t: t,
		})
	}
	for k, v := range a.tagger.Fixed() {
		out.tagger.AddFixed(k, v)
	}
	for _, t := range a.tags {
		if t.pair.table == "" {
			t.pair.table = a.tableName()
		}
		tags = append(tags, t)
	}
	out.tags = tags
	return out, nil
}

func (qs *QuadStore) OptimizeIterator(it graph.Iterator) (graph.Iterator, bool) {
	switch it.Type() {
	case graph.LinksTo:
		return qs.optimizeLinksTo(it.(*iterator.LinksTo))
	case graph.HasA:
		return qs.optimizeHasA(it.(*iterator.HasA))
	case graph.And:
		return qs.optimizeAnd(it.(*iterator.And))
	}
	return it, false
}

func (qs *QuadStore) optimizeLinksTo(it *iterator.LinksTo) (graph.Iterator, bool) {
	subs := it.SubIterators()
	if len(subs) != 1 {
		return it, false
	}
	primary := subs[0]
	switch primary.Type() {
	case graph.Fixed:
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
	case sqlBuilderType:
		newit, err := linksto(primary.(*StatementIterator), it.Direction())
		if err != nil {
			glog.Errorln(err)
			return it, false
		}
		newit.Tagger().CopyFrom(it)
		return newit, true
	case graph.All:
		newit := &StatementIterator{
			uid:    iterator.NextUID(),
			qs:     qs,
			stType: link,
			size:   qs.Size(),
		}
		for _, t := range primary.Tagger().Tags() {
			newit.tags = append(newit.tags, tag{
				pair: tableDir{"", it.Direction()},
				t:    t,
			})
		}
		for k, v := range primary.Tagger().Fixed() {
			newit.tagger.AddFixed(k, v)
		}
		newit.tagger.CopyFrom(it)

		return newit, true
	}
	return it, false
}

func (qs *QuadStore) optimizeAnd(it *iterator.And) (graph.Iterator, bool) {
	subs := it.SubIterators()
	var unusedIts []graph.Iterator
	var newit *StatementIterator
	newit = nil
	changed := false
	var err error

	for _, it := range subs {
		if it.Type() == sqlBuilderType {
			if newit == nil {
				newit = it.(*StatementIterator)
			} else {
				changed = true
				newit, err = intersect(newit, it.(*StatementIterator))
				if err != nil {
					glog.Error(err)
					return it, false
				}
			}
		} else {
			unusedIts = append(unusedIts, it)
		}
	}

	if !changed {
		return it, false
	}
	if len(unusedIts) == 0 {
		newit.tagger.CopyFrom(it)
		return newit, true
	}
	newAnd := iterator.NewAnd(qs)
	newAnd.Tagger().CopyFrom(it)
	newAnd.AddSubIterator(newit)
	for _, i := range unusedIts {
		newAnd.AddSubIterator(i)
	}
	return newAnd.Optimize()
}

func (qs *QuadStore) optimizeHasA(it *iterator.HasA) (graph.Iterator, bool) {
	subs := it.SubIterators()
	if len(subs) != 1 {
		return it, false
	}
	primary := subs[0]
	if primary.Type() == sqlBuilderType {
		newit, err := hasa(primary.(*StatementIterator), it.Direction())
		if err != nil {
			glog.Errorln(err)
			return it, false
		}
		newit.Tagger().CopyFrom(it)
		return newit, true
	}
	return it, false
}
