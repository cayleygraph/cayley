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

package api

import (
	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/quad"
)

type Morphism struct {
	Name     string
	Reversal func() Morphism
	Apply    graph.MorphismFunc
}

type iteratorMorphism interface {
	Apply(graph.Iterator) graph.Iterator
}

type Path struct {
	stack []Morphism
	it    graph.Iterator
	qs    graph.QuadStore
}

func V(qs graph.QuadStore, nodes ...string) *Path {
	fixed := qs.FixedIterator()
	for _, n := range nodes {
		fixed.Add(qs.ValueOf(n))
	}
	return &Path{
		it: fixed,
		qs: qs,
	}
}

func PathFromIterator(qs graph.QuadStore, it graph.Iterator) *Path {
	return &Path{
		it: it,
		qs: qs,
	}
}

func M(qs graph.QuadStore) *Path {
	return &Path{
		it: nil,
		qs: qs,
	}
}

func (p *Path) Reverse() *Path {
	newPath := M(p.qs)
	for i := len(p.stack) - 1; i >= 0; i-- {
		newPath.stack = append(newPath.stack, p.stack[i].Reversal())
	}
	return newPath
}

func (p *Path) IsConcrete() bool { return p.it != nil }

func (p *Path) Tag(tags ...string) *Path {
	p.stack = append(p.stack, TagMorphism(tags...))
	return p
}

func (p *Path) Out(via ...interface{}) *Path {
	p.stack = append(p.stack, OutMorphism(p.qs, via...))
	return p
}
func (p *Path) In(via ...interface{}) *Path {
	p.stack = append(p.stack, InMorphism(p.qs, via...))
	return p
}

func (p *Path) BuildIterator() graph.Iterator {
	f := p.MorphismFunc()
	return f(p.it)
}

func (p *Path) MorphismFunc() graph.MorphismFunc {
	return func(it graph.Iterator) graph.Iterator {
		i := it.Clone()
		for _, m := range p.stack {
			i = m.Apply(i)
		}
		return i
	}
}

func TagMorphism(tags ...string) Morphism {
	return Morphism{
		"tag",
		func() Morphism { return TagMorphism(tags...) },
		func(it graph.Iterator) graph.Iterator {
			for _, t := range tags {
				it.Tagger().Add(t)
			}
			return it
		}}
}

func OutMorphism(qs graph.QuadStore, via ...interface{}) Morphism {
	path := buildViaPath(qs, via...)
	return Morphism{
		"out",
		func() Morphism { return InMorphism(qs, via...) },
		inOutIterator(path, false),
	}
}

func InMorphism(qs graph.QuadStore, via ...interface{}) Morphism {
	path := buildViaPath(qs, via...)
	return Morphism{
		"in",
		func() Morphism { return OutMorphism(qs, via...) },
		inOutIterator(path, true),
	}
}

func inOutIterator(viaPath *Path, reverse bool) graph.MorphismFunc {
	return func(base graph.Iterator) graph.Iterator {
		in, out := quad.Subject, quad.Object
		if reverse {
			in, out = out, in
		}
		lto := iterator.NewLinksTo(viaPath.qs, base, in)
		and := iterator.NewAnd()
		and.AddSubIterator(iterator.NewLinksTo(viaPath.qs, viaPath.BuildIterator(), quad.Predicate))
		and.AddSubIterator(lto)
		return iterator.NewHasA(viaPath.qs, and, out)
	}
}

func buildViaPath(qs graph.QuadStore, via ...interface{}) *Path {
	if len(via) == 0 {
		return PathFromIterator(qs, qs.NodesAllIterator())
	} else if len(via) == 1 {
		v := via[0]
		if path, ok := v.(*Path); ok {
			return path
		} else if str, ok := v.(string); ok {
			return V(qs, str)
		} else {
			panic("Invalid type passed to buildViaPath.")
		}
	}
	var strings []string
	for _, s := range via {
		if str, ok := s.(string); ok {
			strings = append(strings, str)
		} else {
			panic("Non-string type passed to long Via path")
		}
	}
	return V(qs, strings...)
}
