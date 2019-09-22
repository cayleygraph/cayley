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

package memstore

import (
	"context"

	"github.com/cayleygraph/cayley/graph"
)

var _ graph.IteratorFuture = (*AllIterator)(nil)

type AllIterator struct {
	it *allIterator
	graph.Iterator
}

func newAllIterator(qs *QuadStore, nodes bool, maxid int64) *AllIterator {
	it := &AllIterator{
		it: newAllIterator2(qs, nodes, maxid),
	}
	it.Iterator = graph.NewLegacy(it.it, it)
	return it
}

func (it *AllIterator) AsShape() graph.IteratorShape {
	it.Close()
	return it.it
}

var _ graph.IteratorShape = (*allIterator)(nil)

type allIterator struct {
	qs    *QuadStore
	all   []*primitive
	maxid int64 // id of last observed insert (prim id)
	nodes bool
}

func newAllIterator2(qs *QuadStore, nodes bool, maxid int64) *allIterator {
	return &allIterator{
		qs: qs, all: qs.cloneAll(), nodes: nodes,
		maxid: maxid,
	}
}

func (it *allIterator) Iterate() graph.Scanner {
	return newAllIteratorNext(it.qs, it.nodes, it.maxid, it.all)
}

func (it *allIterator) Lookup() graph.Index {
	return newAllIteratorContains(it.qs, it.nodes, it.maxid)
}

func (it *allIterator) AsLegacy() graph.Iterator {
	it2 := &AllIterator{it: it}
	it2.Iterator = graph.NewLegacy(it, it2)
	return it2
}

func (it *allIterator) SubIterators() []graph.IteratorShape { return nil }
func (it *allIterator) Optimize(ctx context.Context) (graph.IteratorShape, bool) {
	return it, false
}

func (it *allIterator) String() string {
	return "MemStoreAll"
}

func (it *allIterator) Stats(ctx context.Context) (graph.IteratorCosts, error) {
	return graph.IteratorCosts{
		NextCost:     1,
		ContainsCost: 1,
		Size: graph.Size{
			// TODO(dennwc): use maxid?
			Size:  int64(len(it.all)),
			Exact: true,
		},
	}, nil
}

func (p *primitive) filter(isNode bool, maxid int64) bool {
	if p.ID > maxid {
		return false
	} else if isNode && p.Value != nil {
		return true
	} else if !isNode && !p.Quad.Zero() {
		return true
	}
	return false
}

type allIteratorNext struct {
	qs    *QuadStore
	all   []*primitive
	maxid int64 // id of last observed insert (prim id)
	nodes bool

	i    int // index into qs.all
	cur  *primitive
	done bool
}

func newAllIteratorNext(qs *QuadStore, nodes bool, maxid int64, all []*primitive) *allIteratorNext {
	return &allIteratorNext{
		qs: qs, all: all, nodes: nodes,
		i: -1, maxid: maxid,
	}
}

func (it *allIteratorNext) ok(p *primitive) bool {
	return p.filter(it.nodes, it.maxid)
}

func (it *allIteratorNext) Next(ctx context.Context) bool {
	it.cur = nil
	if it.done {
		return false
	}
	all := it.all
	if it.i >= len(all) {
		it.done = true
		return false
	}
	it.i++
	for ; it.i < len(all); it.i++ {
		p := all[it.i]
		if p.ID > it.maxid {
			break
		}
		if it.ok(p) {
			it.cur = p
			return true
		}
	}
	it.done = true
	return false
}

func (it *allIteratorNext) Result() graph.Ref {
	if it.cur == nil {
		return nil
	}
	if !it.cur.Quad.Zero() {
		return qprim{p: it.cur}
	}
	return bnode(it.cur.ID)
}

func (it *allIteratorNext) Err() error { return nil }
func (it *allIteratorNext) Close() error {
	it.done = true
	it.all = nil
	return nil
}

func (it *allIteratorNext) TagResults(dst map[string]graph.Ref) {}

func (it *allIteratorNext) String() string {
	return "MemStoreAllNext"
}
func (it *allIteratorNext) NextPath(ctx context.Context) bool { return false }

type allIteratorContains struct {
	qs    *QuadStore
	maxid int64 // id of last observed insert (prim id)
	nodes bool

	cur  *primitive
	done bool
}

func newAllIteratorContains(qs *QuadStore, nodes bool, maxid int64) *allIteratorContains {
	return &allIteratorContains{
		qs: qs, nodes: nodes,
		maxid: maxid,
	}
}

func (it *allIteratorContains) ok(p *primitive) bool {
	return p.filter(it.nodes, it.maxid)
}

func (it *allIteratorContains) Contains(ctx context.Context, v graph.Ref) bool {
	it.cur = nil
	if it.done {
		return false
	}
	id, ok := asID(v)
	if !ok {
		return false
	}
	p := it.qs.prim[id]
	if p.ID > it.maxid {
		return false
	}
	if !it.ok(p) {
		return false
	}
	it.cur = p
	return true
}
func (it *allIteratorContains) Result() graph.Ref {
	if it.cur == nil {
		return nil
	}
	if !it.cur.Quad.Zero() {
		return qprim{p: it.cur}
	}
	return bnode(it.cur.ID)
}

func (it *allIteratorContains) Err() error { return nil }
func (it *allIteratorContains) Close() error {
	it.done = true
	return nil
}

func (it *allIteratorContains) TagResults(dst map[string]graph.Ref) {}

func (it *allIteratorContains) String() string {
	return "MemStoreAllContains"
}
func (it *allIteratorContains) NextPath(ctx context.Context) bool { return false }
