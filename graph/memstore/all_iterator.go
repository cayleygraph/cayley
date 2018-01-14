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
	"github.com/cayleygraph/cayley/graph/iterator"
)

var _ graph.Iterator = (*AllIterator)(nil)

type AllIterator struct {
	uid uint64

	qs    *QuadStore
	all   []*primitive
	maxid int64 // id of last observed insert (prim id)
	nodes bool

	i    int // index into qs.all
	cur  *primitive
	done bool
}

func newAllIterator(qs *QuadStore, nodes bool, maxid int64) *AllIterator {
	return &AllIterator{
		uid: iterator.NextUID(),
		qs:  qs, all: qs.cloneAll(), nodes: nodes,
		i: -1, maxid: maxid,
	}
}

func (it *AllIterator) Clone() graph.Iterator {
	return newAllIterator(it.qs, it.nodes, it.maxid)
}

func (it *AllIterator) Reset() {
	it.i = -1
	it.cur = nil
	it.done = false
}

func (it *AllIterator) ok(p *primitive) bool {
	if p.ID > it.maxid {
		return false
	} else if it.nodes && p.Value != nil {
		return true
	} else if !it.nodes && !p.Quad.Zero() {
		return true
	}
	return false
}

func (it *AllIterator) Next(ctx context.Context) bool {
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

func (it *AllIterator) Contains(ctx context.Context, v graph.Value) bool {
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
func (it *AllIterator) Result() graph.Value {
	if it.cur == nil {
		return nil
	}
	if !it.cur.Quad.Zero() {
		return qprim{p: it.cur}
	}
	return bnode(it.cur.ID)
}

func (it *AllIterator) Err() error { return nil }
func (it *AllIterator) Close() error {
	it.done = true
	it.all = nil
	return nil
}

func (it *AllIterator) TagResults(dst map[string]graph.Value) {}

func (it *AllIterator) SubIterators() []graph.Iterator   { return nil }
func (it *AllIterator) Optimize() (graph.Iterator, bool) { return it, false }

func (it *AllIterator) UID() uint64 {
	return it.uid
}
func (it *AllIterator) Type() graph.Type { return graph.All }
func (it *AllIterator) String() string {
	return "MemStoreAll"
}
func (it *AllIterator) NextPath(ctx context.Context) bool { return false }

func (it *AllIterator) Size() (int64, bool) {
	// TODO: use maxid?
	return int64(len(it.all)), true
}
func (it *AllIterator) Stats() graph.IteratorStats {
	st := graph.IteratorStats{NextCost: 1, ContainsCost: 1}
	st.Size, st.ExactSize = it.Size()
	return st
}
