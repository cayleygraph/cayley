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

package leveldb

import (
	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/graph/path2"
	"github.com/google/cayley/quad"
)

func (qs *QuadStore) OptimizeIterator(it graph.Iterator) (graph.Iterator, bool) {
	switch it.Type() {
	case graph.LinksTo:
		return qs.optimizeLinksTo(it.(*iterator.LinksTo))

	}
	return it, false
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

var _ path.PathOptimizer = (*QuadStore)(nil)

func (qs *QuadStore) OptimizeLinksPath(p path.Links) (path.Links, bool) {
	switch tp := p.(type) {
	case path.LinksTo:
		return qs.optimizePathLinksTo(tp)
	}
	return p, false
}
func (qs *QuadStore) OptimizeNodesPath(p path.Nodes) (path.Nodes, bool) {
	return p, false
}

type pathLinksTo struct {
	qs     *QuadStore
	prefix string
	d      quad.Direction
	val    Token
}

func (p pathLinksTo) Optimize() (path.Links, bool)                                { return p, false }
func (p pathLinksTo) Replace(_ path.NodesWrapper, _ path.LinksWrapper) path.Links { return p }
func (p pathLinksTo) BuildIterator() graph.Iterator                               { return NewIterator(p.prefix, p.d, p.val, p.qs) }
func (qs *QuadStore) optimizePathLinksTo(p path.LinksTo) (path.Links, bool) {
	switch t := p.Nodes.(type) {
	case path.Fixed:
		if len(t) == 1 {
			return qs.LinksToValuePath(p.Dir, t[0]), true
		}
	}
	return p, false
}
