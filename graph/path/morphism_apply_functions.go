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

package path

import (
	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/quad"
)

func isMorphism(nodes ...string) morphism {
	return morphism{
		Name:     "is",
		Reversal: func() morphism { return isMorphism(nodes...) },
		Apply: func(qs graph.QuadStore, it graph.Iterator) graph.Iterator {
			var sub graph.Iterator
			if len(nodes) == 0 {
				sub = qs.NodesAllIterator()
			} else {
				fixed := qs.FixedIterator()
				for _, n := range nodes {
					fixed.Add(qs.ValueOf(n))
				}
				sub = fixed
			}
			and := iterator.NewAnd(qs)
			and.AddSubIterator(sub)
			and.AddSubIterator(it)
			return and
		},
	}
}

func hasMorphism(via interface{}, nodes ...string) morphism {
	return morphism{
		Name:     "has",
		Reversal: func() morphism { return hasMorphism(via, nodes...) },
		Apply: func(qs graph.QuadStore, it graph.Iterator) graph.Iterator {
			var sub graph.Iterator
			if len(nodes) == 0 {
				sub = qs.NodesAllIterator()
			} else {
				fixed := qs.FixedIterator()
				for _, n := range nodes {
					fixed.Add(qs.ValueOf(n))
				}
				sub = fixed
			}
			var viaPath *Path
			if via != nil {
				viaPath = buildViaPath(qs, via)
			} else {
				viaPath = buildViaPath(qs)
			}
			subAnd := iterator.NewAnd(qs)
			subAnd.AddSubIterator(iterator.NewLinksTo(qs, sub, quad.Object))
			subAnd.AddSubIterator(iterator.NewLinksTo(qs, viaPath.BuildIterator(), quad.Predicate))
			hasa := iterator.NewHasA(qs, subAnd, quad.Subject)
			and := iterator.NewAnd(qs)
			and.AddSubIterator(it)
			and.AddSubIterator(hasa)
			return and
		},
	}
}

func tagMorphism(tags ...string) morphism {
	return morphism{
		Name:     "tag",
		Reversal: func() morphism { return tagMorphism(tags...) },
		Apply: func(qs graph.QuadStore, it graph.Iterator) graph.Iterator {
			for _, t := range tags {
				it.Tagger().Add(t)
			}
			return it
		},
		tags: tags,
	}
}

func outMorphism(via ...interface{}) morphism {
	return morphism{
		Name:     "out",
		Reversal: func() morphism { return inMorphism(via...) },
		Apply: func(qs graph.QuadStore, it graph.Iterator) graph.Iterator {
			path := buildViaPath(qs, via...)
			return inOutIterator(path, it, false)
		},
	}
}

func inMorphism(via ...interface{}) morphism {
	return morphism{
		Name:     "in",
		Reversal: func() morphism { return outMorphism(via...) },
		Apply: func(qs graph.QuadStore, it graph.Iterator) graph.Iterator {
			path := buildViaPath(qs, via...)
			return inOutIterator(path, it, true)
		},
	}
}

func iteratorMorphism(it graph.Iterator) morphism {
	return morphism{
		Name:     "iterator",
		Reversal: func() morphism { return iteratorMorphism(it) },
		Apply: func(qs graph.QuadStore, subIt graph.Iterator) graph.Iterator {
			and := iterator.NewAnd(qs)
			and.AddSubIterator(it)
			and.AddSubIterator(subIt)
			return and
		},
	}
}

func andMorphism(p *Path) morphism {
	return morphism{
		Name:     "and",
		Reversal: func() morphism { return andMorphism(p) },
		Apply: func(qs graph.QuadStore, it graph.Iterator) graph.Iterator {
			subIt := p.BuildIteratorOn(qs)
			and := iterator.NewAnd(qs)
			and.AddSubIterator(it)
			and.AddSubIterator(subIt)
			return and
		},
	}
}

func orMorphism(p *Path) morphism {
	return morphism{
		Name:     "or",
		Reversal: func() morphism { return orMorphism(p) },
		Apply: func(qs graph.QuadStore, it graph.Iterator) graph.Iterator {
			subIt := p.BuildIteratorOn(qs)
			and := iterator.NewOr()
			and.AddSubIterator(it)
			and.AddSubIterator(subIt)
			return and
		},
	}
}

func followMorphism(p *Path) morphism {
	return morphism{
		Name:     "follow",
		Reversal: func() morphism { return followMorphism(p.Reverse()) },
		Apply: func(qs graph.QuadStore, base graph.Iterator) graph.Iterator {
			return p.Morphism()(qs, base)
		},
	}
}

func exceptMorphism(p *Path) morphism {
	return morphism{
		Name:     "except",
		Reversal: func() morphism { return exceptMorphism(p) },
		Apply: func(qs graph.QuadStore, base graph.Iterator) graph.Iterator {
			subIt := p.BuildIteratorOn(qs)
			notIt := iterator.NewNot(subIt, qs.NodesAllIterator())
			and := iterator.NewAnd(qs)
			and.AddSubIterator(base)
			and.AddSubIterator(notIt)
			return and
		},
	}
}

func saveMorphism(via interface{}, tag string) morphism {
	return morphism{
		Name:     "save",
		Reversal: func() morphism { return saveMorphism(via, tag) },
		Apply: func(qs graph.QuadStore, it graph.Iterator) graph.Iterator {
			return buildSave(qs, via, tag, it, false)
		},
		tags: []string{tag},
	}
}

func saveReverseMorphism(via interface{}, tag string) morphism {
	return morphism{
		Name:     "saver",
		Reversal: func() morphism { return saveReverseMorphism(via, tag) },
		Apply: func(qs graph.QuadStore, it graph.Iterator) graph.Iterator {
			return buildSave(qs, via, tag, it, true)
		},
		tags: []string{tag},
	}
}

func buildSave(qs graph.QuadStore, via interface{}, tag string, it graph.Iterator, reverse bool) graph.Iterator {
	all := qs.NodesAllIterator()
	all.Tagger().Add(tag)
	node, allDir := quad.Subject, quad.Object
	var viaPath *Path
	if via != nil {
		viaPath = buildViaPath(qs, via)
	} else {
		viaPath = buildViaPath(qs)
	}
	if reverse {
		node, allDir = allDir, node
	}
	lto := iterator.NewLinksTo(qs, all, allDir)
	subAnd := iterator.NewAnd(qs)
	subAnd.AddSubIterator(iterator.NewLinksTo(qs, viaPath.BuildIterator(), quad.Predicate))
	subAnd.AddSubIterator(lto)
	hasa := iterator.NewHasA(qs, subAnd, node)
	and := iterator.NewAnd(qs)
	and.AddSubIterator(hasa)
	and.AddSubIterator(it)
	return and
}

func inOutIterator(viaPath *Path, it graph.Iterator, reverse bool) graph.Iterator {
	in, out := quad.Subject, quad.Object
	if reverse {
		in, out = out, in
	}
	lto := iterator.NewLinksTo(viaPath.qs, it, in)
	and := iterator.NewAnd(viaPath.qs)
	and.AddSubIterator(iterator.NewLinksTo(viaPath.qs, viaPath.BuildIterator(), quad.Predicate))
	and.AddSubIterator(lto)
	return iterator.NewHasA(viaPath.qs, and, out)
}

func buildViaPath(qs graph.QuadStore, via ...interface{}) *Path {
	if len(via) == 0 {
		return PathFromIterator(qs, qs.NodesAllIterator())
	} else if len(via) == 1 {
		v := via[0]
		switch p := v.(type) {
		case *Path:
			return p
		case string:
			return StartPath(qs, p)
		default:
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
