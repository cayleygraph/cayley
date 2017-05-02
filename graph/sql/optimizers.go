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

	"github.com/codelingo/cayley/clog"
	"github.com/codelingo/cayley/graph"
	"github.com/codelingo/cayley/graph/iterator"
	"github.com/codelingo/cayley/quad"
)

func intersect(a sqlIterator, b sqlIterator, qs *QuadStore) (*SQLIterator, error) {
	if anew, ok := a.(*SQLNodeIterator); ok {
		if bnew, ok := b.(*SQLNodeIterator); ok {
			return intersectNode(anew, bnew, qs)
		}
		if bnew, ok := b.(*SQLNodeIntersection); ok {
			return appendNodeIntersection(bnew, anew, qs)
		}
	} else if anew, ok := a.(*SQLNodeIntersection); ok {
		if bnew, ok := b.(*SQLNodeIterator); ok {
			return appendNodeIntersection(anew, bnew, qs)
		}
		if bnew, ok := b.(*SQLNodeIntersection); ok {
			return combineNodeIntersection(anew, bnew, qs)
		}
	} else if anew, ok := a.(*SQLLinkIterator); ok {
		if bnew, ok := b.(*SQLLinkIterator); ok {
			return intersectLink(anew, bnew, qs)
		}

	} else {
		return nil, errors.New("Unknown iterator types")
	}
	return nil, errors.New("Cannot combine SQL iterators of two different types")
}

func intersectNode(a *SQLNodeIterator, b *SQLNodeIterator, qs *QuadStore) (*SQLIterator, error) {
	m := &SQLNodeIntersection{
		tableName: newTableName(),
		nodeIts:   []sqlIterator{a, b},
	}
	m.Tagger().CopyFromTagger(a.Tagger())
	m.Tagger().CopyFromTagger(b.Tagger())
	it := NewSQLIterator(qs, m)
	return it, nil
}

func appendNodeIntersection(a *SQLNodeIntersection, b *SQLNodeIterator, qs *QuadStore) (*SQLIterator, error) {
	m := &SQLNodeIntersection{
		tableName: newTableName(),
		nodeIts:   append(a.nodeIts, b),
	}
	m.Tagger().CopyFromTagger(a.Tagger())
	m.Tagger().CopyFromTagger(b.Tagger())
	it := NewSQLIterator(qs, m)
	return it, nil
}

func combineNodeIntersection(a *SQLNodeIntersection, b *SQLNodeIntersection, qs *QuadStore) (*SQLIterator, error) {
	m := &SQLNodeIntersection{
		tableName: newTableName(),
		nodeIts:   append(a.nodeIts, b.nodeIts...),
	}
	m.Tagger().CopyFromTagger(a.Tagger())
	m.Tagger().CopyFromTagger(b.Tagger())
	it := NewSQLIterator(qs, m)
	return it, nil
}

func intersectLink(a *SQLLinkIterator, b *SQLLinkIterator, qs *QuadStore) (*SQLIterator, error) {
	m := &SQLLinkIterator{
		tableName:   newTableName(),
		nodeIts:     append(a.nodeIts, b.nodeIts...),
		constraints: append(a.constraints, b.constraints...),
		tagdirs:     append(a.tagdirs, b.tagdirs...),
	}
	m.Tagger().CopyFromTagger(a.Tagger())
	m.Tagger().CopyFromTagger(b.Tagger())
	it := NewSQLIterator(qs, m)
	return it, nil
}

func hasa(aIn sqlIterator, d quad.Direction, qs *QuadStore) (*SQLIterator, error) {
	a, ok := aIn.(*SQLLinkIterator)
	if !ok {
		return nil, errors.New("Can't take the HASA of a link SQL iterator")
	}

	out := &SQLNodeIterator{
		tableName: newTableName(),
		linkIt: sqlItDir{
			it:  a,
			dir: d,
		},
	}
	it := NewSQLIterator(qs, out)
	return it, nil
}

func linksto(aIn sqlIterator, d quad.Direction, qs *QuadStore) (*SQLIterator, error) {
	var a sqlIterator
	a, ok := aIn.(*SQLNodeIterator)
	if !ok {
		a, ok = aIn.(*SQLNodeIntersection)
		if !ok {
			return nil, errors.New("Can't take the LINKSTO of a node SQL iterator")
		}
	}

	out := &SQLLinkIterator{
		tableName: newTableName(),
		nodeIts: []sqlItDir{
			sqlItDir{
				it:  a,
				dir: d,
			},
		},
	}
	it := NewSQLIterator(qs, out)
	return it, nil
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
		if size == 0 {
			return iterator.NewNull(), true
		}
		if size == 1 {
			if !primary.Next() {
				panic("sql: unexpected size during optimize")
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
		} else if size > 1 {
			var vals []NodeHash
			for primary.Next() {
				vals = append(vals, primary.Result().(NodeHash))
			}
			lsql := &SQLLinkIterator{
				constraints: []constraint{
					constraint{
						dir:    it.Direction(),
						hashes: vals,
					},
				},
				tableName: newTableName(),
				size:      0,
			}
			l := &SQLIterator{
				uid: iterator.NextUID(),
				qs:  qs,
				sql: lsql,
			}
			nt := l.Tagger()
			nt.CopyFrom(it)
			for _, t := range primary.Tagger().Tags() {
				lsql.tagdirs = append(lsql.tagdirs, tagDir{
					dir: it.Direction(),
					tag: t,
				})
			}
			it.Close()
			return l, true
		}
	case sqlType:
		p := primary.(*SQLIterator)
		newit, err := linksto(p.sql, it.Direction(), qs)
		if err != nil {
			clog.Errorf("%v", err)
			return it, false
		}
		newit.Tagger().CopyFrom(it)
		return newit, true
	case graph.All:
		linkit := &SQLLinkIterator{
			tableName: newTableName(),
			size:      qs.Size(),
		}
		for _, t := range primary.Tagger().Tags() {
			linkit.tagdirs = append(linkit.tagdirs, tagDir{
				dir: it.Direction(),
				tag: t,
			})
		}
		for k, v := range primary.Tagger().Fixed() {
			linkit.tagger.AddFixed(k, v)
		}
		linkit.tagger.CopyFrom(it)
		newit := NewSQLIterator(qs, linkit)
		return newit, true
	}
	return it, false
}

func (qs *QuadStore) optimizeAnd(it *iterator.And) (graph.Iterator, bool) {
	subs := it.SubIterators()
	var unusedIts []graph.Iterator
	var newit *SQLIterator
	newit = nil
	changed := false
	var err error

	// Combine SQL iterators
	if clog.V(4) {
		clog.Infof("Combining SQL %#v", subs)
	}
	for _, subit := range subs {
		if subit.Type() == sqlType {
			if newit == nil {
				newit = subit.(*SQLIterator)
			} else {
				changed = true
				newit, err = intersect(newit.sql, subit.(*SQLIterator).sql, qs)
				if err != nil {
					clog.Errorf("%v", err)
					return it, false
				}
			}
		} else {
			unusedIts = append(unusedIts, subit)
		}
	}

	if newit == nil {
		return it, false
	}

	// Combine fixed iterators into the SQL iterators.
	if clog.V(4) {
		clog.Infof("Combining fixed %#v", unusedIts)
	}
	var nodeit *SQLNodeIterator
	if n, ok := newit.sql.(*SQLNodeIterator); ok {
		nodeit = n
	} else if n, ok := newit.sql.(*SQLNodeIntersection); ok {
		nodeit = n.nodeIts[0].(*SQLNodeIterator)
	}
	if nodeit != nil {
		passOneIts := unusedIts
		unusedIts = nil
		for _, subit := range passOneIts {
			if subit.Type() != graph.Fixed {
				unusedIts = append(unusedIts, subit)
				continue
			}
			changed = true
			for subit.Next() {
				nodeit.fixedSet = append(nodeit.fixedSet, qs.NameOf(subit.Result()))
			}
		}
	}

	if !changed {
		return it, false
	}

	// Clean up if we're done.
	if len(unusedIts) == 0 {
		newit.Tagger().CopyFrom(it)
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
	if primary.Type() == sqlType {
		p := primary.(*SQLIterator)
		newit, err := hasa(p.sql, it.Direction(), qs)
		if err != nil {
			clog.Errorf("%v", err)
			return it, false
		}
		newit.Tagger().CopyFrom(it)
		return newit, true
	}
	return it, false
}
