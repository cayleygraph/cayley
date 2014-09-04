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

type morphism struct {
	Name     string
	Reversal func() morphism
	Apply    graph.MorphismFunc
}

type Path struct {
	stack []morphism
	qs    graph.QuadStore
}

func StartPath(qs graph.QuadStore, nodes ...string) *Path {
	return &Path{
		stack: []morphism{
			isMorphism(qs, nodes...),
		},
		qs: qs,
	}
}

func PathFromIterator(qs graph.QuadStore, it graph.Iterator) *Path {
	return &Path{
		stack: []morphism{
			intersectIteratorMorphism(it),
		},
		qs: qs,
	}
}

func NewPath(qs graph.QuadStore) *Path {
	return &Path{
		qs: qs,
	}
}

func (p *Path) Reverse() *Path {
	newPath := NewPath(p.qs)
	for i := len(p.stack) - 1; i >= 0; i-- {
		newPath.stack = append(newPath.stack, p.stack[i].Reversal())
	}
	return newPath
}

func (p *Path) Tag(tags ...string) *Path {
	p.stack = append(p.stack, tagMorphism(tags...))
	return p
}

func (p *Path) Out(via ...interface{}) *Path {
	p.stack = append(p.stack, outMorphism(p.qs, via...))
	return p
}
func (p *Path) In(via ...interface{}) *Path {
	p.stack = append(p.stack, inMorphism(p.qs, via...))
	return p
}

func (p *Path) And(path *Path) *Path {
	p.stack = append(p.stack, andMorphism(path))
	return p
}

func (p *Path) Or(path *Path) *Path {
	p.stack = append(p.stack, orMorphism(path))
	return p
}

func (p *Path) BuildIterator() graph.Iterator {
	f := p.MorphismFunc()
	return f(p.qs.NodesAllIterator())
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

func isMorphism(qs graph.QuadStore, nodes ...string) morphism {
	return morphism{
		"is",
		func() morphism { return isMorphism(qs, nodes...) },
		func(it graph.Iterator) graph.Iterator {
			fixed := qs.FixedIterator()
			for _, n := range nodes {
				fixed.Add(qs.ValueOf(n))
			}
			and := iterator.NewAnd()
			and.AddSubIterator(fixed)
			and.AddSubIterator(it)
			return and
		},
	}
}

func tagMorphism(tags ...string) morphism {
	return morphism{
		"tag",
		func() morphism { return tagMorphism(tags...) },
		func(it graph.Iterator) graph.Iterator {
			for _, t := range tags {
				it.Tagger().Add(t)
			}
			return it
		}}
}

func outMorphism(qs graph.QuadStore, via ...interface{}) morphism {
	path := buildViaPath(qs, via...)
	return morphism{
		"out",
		func() morphism { return inMorphism(qs, via...) },
		inOutIterator(path, false),
	}
}

func inMorphism(qs graph.QuadStore, via ...interface{}) morphism {
	path := buildViaPath(qs, via...)
	return morphism{
		"in",
		func() morphism { return outMorphism(qs, via...) },
		inOutIterator(path, true),
	}
}

func intersectIteratorMorphism(it graph.Iterator) morphism {
	return morphism{
		"iterator",
		func() morphism { return intersectIteratorMorphism(it) },
		func(subIt graph.Iterator) graph.Iterator {
			and := iterator.NewAnd()
			and.AddSubIterator(it)
			and.AddSubIterator(subIt)
			return and
		},
	}
}

func andMorphism(path *Path) morphism {
	return morphism{
		"and",
		func() morphism { return andMorphism(path) },
		func(it graph.Iterator) graph.Iterator {
			subIt := path.BuildIterator()
			and := iterator.NewAnd()
			and.AddSubIterator(it)
			and.AddSubIterator(subIt)
			return and
		},
	}
}

func orMorphism(path *Path) morphism {
	return morphism{
		"or",
		func() morphism { return orMorphism(path) },
		func(it graph.Iterator) graph.Iterator {
			subIt := path.BuildIterator()
			and := iterator.NewOr()
			and.AddSubIterator(it)
			and.AddSubIterator(subIt)
			return and
		},
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
			return StartPath(qs, str)
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
	return StartPath(qs, strings...)
}
