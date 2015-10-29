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
	"fmt"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/quad"
)

// join puts two iterators together by intersecting their result sets with an AND
// Since we're using an and iterator, it's a good idea to put the smallest result
// set first so that Next() produces fewer values to check Contains().
func join(qs graph.QuadStore, itL, itR graph.Iterator) graph.Iterator {
	and := iterator.NewAnd(qs)
	and.AddSubIterator(itL)
	and.AddSubIterator(itR)

	return and
}

// isMorphism represents all nodes passed in-- if there are none, this function
// acts as a passthrough for the previous iterator.
func isMorphism(nodes ...string) morphism {
	return morphism{
		Name:     "is",
		Reversal: func(ctx *context) (morphism, *context) { return isMorphism(nodes...), ctx },
		Apply: func(qs graph.QuadStore, in graph.Iterator, ctx *context) (graph.Iterator, *context) {
			if len(nodes) == 0 {
				// Acting as a passthrough here is equivalent to
				// building a NodesAllIterator to Next() or Contains()
				// from here as in previous versions.
				return in, ctx
			}

			isNodes := qs.FixedIterator()
			for _, n := range nodes {
				isNodes.Add(qs.ValueOf(n))
			}

			// Anything with fixedIterators will usually have a much
			// smaller result set, so join isNodes first here.
			return join(qs, isNodes, in), ctx
		},
	}
}

// hasMorphism is the set of nodes that is reachable via either a *Path, a
// single node.(string) or a list of nodes.([]string).
func hasMorphism(via interface{}, nodes ...string) morphism {
	return morphism{
		Name:     "has",
		Reversal: func(ctx *context) (morphism, *context) { return hasMorphism(via, nodes...), ctx },
		Apply: func(qs graph.QuadStore, in graph.Iterator, ctx *context) (graph.Iterator, *context) {
			viaIter := buildViaPath(qs, via).
				BuildIterator()
			ends := func() graph.Iterator {
				if len(nodes) == 0 {
					return qs.NodesAllIterator()
				}

				fixed := qs.FixedIterator()
				for _, n := range nodes {
					fixed.Add(qs.ValueOf(n))
				}
				return fixed
			}()

			trail := iterator.NewLinksTo(qs, viaIter, quad.Predicate)
			dest := iterator.NewLinksTo(qs, ends, quad.Object)

			// If we were given nodes, intersecting with them first will
			// be extremely cheap-- otherwise, it will be the most expensive
			// (requiring iteration over all nodes). We have enough info to
			// make this optimization now since intersections are commutative
			if len(nodes) == 0 { // Where dest involves an All iterator.
				route := join(qs, trail, dest)
				has := iterator.NewHasA(qs, route, quad.Subject)
				return join(qs, in, has), ctx
			}

			// This looks backwards. That's OK-- see the note above.
			route := join(qs, dest, trail)
			has := iterator.NewHasA(qs, route, quad.Subject)
			return join(qs, has, in), ctx
		},
	}
}

func tagMorphism(tags ...string) morphism {
	return morphism{
		Name:     "tag",
		Reversal: func(ctx *context) (morphism, *context) { return tagMorphism(tags...), ctx },
		Apply: func(qs graph.QuadStore, in graph.Iterator, ctx *context) (graph.Iterator, *context) {
			for _, t := range tags {
				in.Tagger().Add(t)
			}
			return in, ctx
		},
		tags: tags,
	}
}

// outMorphism iterates forward one RDF triple or via an entire path.
func outMorphism(tags []string, via ...interface{}) morphism {
	return morphism{
		Name:     "out",
		Reversal: func(ctx *context) (morphism, *context) { return inMorphism(tags, via...), ctx },
		Apply: func(qs graph.QuadStore, in graph.Iterator, ctx *context) (graph.Iterator, *context) {
			path := buildViaPath(qs, via...)
			return inOutIterator(path, in, false, tags), ctx
		},
	}
}

// inMorphism iterates backwards one RDF triple or via an entire path.
func inMorphism(tags []string, via ...interface{}) morphism {
	return morphism{
		Name:     "in",
		Reversal: func(ctx *context) (morphism, *context) { return outMorphism(tags, via...), ctx },
		Apply: func(qs graph.QuadStore, in graph.Iterator, ctx *context) (graph.Iterator, *context) {
			path := buildViaPath(qs, via...)
			return inOutIterator(path, in, true, tags), ctx
		},
	}
}

func bothMorphism(tags []string, via ...interface{}) morphism {
	return morphism{
		Name:     "in",
		Reversal: func(ctx *context) (morphism, *context) { return bothMorphism(tags, via...), ctx },
		Apply: func(qs graph.QuadStore, in graph.Iterator, ctx *context) (graph.Iterator, *context) {
			path := buildViaPath(qs, via...)
			inSide := inOutIterator(path, in, true, tags)
			outSide := inOutIterator(path, in.Clone(), false, tags)
			or := iterator.NewOr()
			or.AddSubIterator(inSide)
			or.AddSubIterator(outSide)
			return or, ctx
		},
	}
}

// predicatesMorphism iterates to the uniqified set of predicates from
// the given set of nodes in the path.
func predicatesMorphism(isIn bool) morphism {
	m := morphism{
		Name: "out_predicates",
		Reversal: func(ctx *context) (morphism, *context) {
			panic("not implemented: need a function from predicates to their associated edges")
		},
		Apply: func(qs graph.QuadStore, in graph.Iterator, ctx *context) (graph.Iterator, *context) {
			dir := quad.Subject
			if isIn {
				dir = quad.Object
			}
			lto := iterator.NewLinksTo(qs, in, dir)
			hasa := iterator.NewHasA(qs, lto, quad.Predicate)
			return iterator.NewUnique(hasa), ctx
		},
	}
	if isIn {
		m.Name = "in_predicates"
	}
	return m
}

// iteratorMorphism simply tacks the input iterator onto the chain.
func iteratorMorphism(it graph.Iterator) morphism {
	return morphism{
		Name:     "iterator",
		Reversal: func(ctx *context) (morphism, *context) { return iteratorMorphism(it), ctx },
		Apply: func(qs graph.QuadStore, in graph.Iterator, ctx *context) (graph.Iterator, *context) {
			return join(qs, it, in), ctx
		},
	}
}

// andMorphism sticks a path onto the current iterator chain.
func andMorphism(p *Path) morphism {
	return morphism{
		Name:     "and",
		Reversal: func(ctx *context) (morphism, *context) { return andMorphism(p), ctx },
		Apply: func(qs graph.QuadStore, in graph.Iterator, ctx *context) (graph.Iterator, *context) {
			itR := p.BuildIteratorOn(qs)

			return join(qs, in, itR), ctx
		},
	}
}

// orMorphism is the union, vice intersection, of a path and the current iterator.
func orMorphism(p *Path) morphism {
	return morphism{
		Name:     "or",
		Reversal: func(ctx *context) (morphism, *context) { return orMorphism(p), ctx },
		Apply: func(qs graph.QuadStore, in graph.Iterator, ctx *context) (graph.Iterator, *context) {
			itR := p.BuildIteratorOn(qs)

			or := iterator.NewOr()
			or.AddSubIterator(in)
			or.AddSubIterator(itR)
			return or, ctx
		},
	}
}

func followMorphism(p *Path) morphism {
	return morphism{
		Name:     "follow",
		Reversal: func(ctx *context) (morphism, *context) { return followMorphism(p.Reverse()), ctx },
		Apply: func(qs graph.QuadStore, in graph.Iterator, ctx *context) (graph.Iterator, *context) {
			return p.Morphism()(qs, in), ctx
		},
	}
}

// exceptMorphism removes all results on p.(*Path) from the current iterators.
func exceptMorphism(p *Path) morphism {
	return morphism{
		Name:     "except",
		Reversal: func(ctx *context) (morphism, *context) { return exceptMorphism(p), ctx },
		Apply: func(qs graph.QuadStore, in graph.Iterator, ctx *context) (graph.Iterator, *context) {
			subIt := p.BuildIteratorOn(qs)
			allNodes := qs.NodesAllIterator()
			notIn := iterator.NewNot(subIt, allNodes)

			return join(qs, in, notIn), ctx
		},
	}
}

func saveMorphism(via interface{}, tag string) morphism {
	return morphism{
		Name:     "save",
		Reversal: func(ctx *context) (morphism, *context) { return saveMorphism(via, tag), ctx },
		Apply: func(qs graph.QuadStore, in graph.Iterator, ctx *context) (graph.Iterator, *context) {
			return buildSave(qs, via, tag, in, false), ctx
		},
		tags: []string{tag},
	}
}

func saveReverseMorphism(via interface{}, tag string) morphism {
	return morphism{
		Name:     "saver",
		Reversal: func(ctx *context) (morphism, *context) { return saveReverseMorphism(via, tag), ctx },
		Apply: func(qs graph.QuadStore, in graph.Iterator, ctx *context) (graph.Iterator, *context) {
			return buildSave(qs, via, tag, in, true), ctx
		},
		tags: []string{tag},
	}
}

func buildSave(
	qs graph.QuadStore, via interface{},
	tag string, from graph.Iterator, reverse bool,
) graph.Iterator {

	allNodes := qs.NodesAllIterator()
	allNodes.Tagger().Add(tag)

	start, goal := quad.Subject, quad.Object
	if reverse {
		start, goal = goal, start
	}
	viaIter := buildViaPath(qs, via).
		BuildIterator()

	dest := iterator.NewLinksTo(qs, allNodes, goal)
	trail := iterator.NewLinksTo(qs, viaIter, quad.Predicate)

	route := join(qs, trail, dest)
	save := iterator.NewHasA(qs, route, start)

	return join(qs, from, save)
}

func inOutIterator(viaPath *Path, from graph.Iterator, inIterator bool, tags []string) graph.Iterator {
	start, goal := quad.Subject, quad.Object
	if inIterator {
		start, goal = goal, start
	}

	viaIter := viaPath.BuildIterator()
	for _, tag := range tags {
		viaIter.Tagger().Add(tag)
	}

	source := iterator.NewLinksTo(viaPath.qs, from, start)
	trail := iterator.NewLinksTo(viaPath.qs, viaIter, quad.Predicate)

	route := join(viaPath.qs, source, trail)

	return iterator.NewHasA(viaPath.qs, route, goal)
}

func buildViaPath(qs graph.QuadStore, via ...interface{}) *Path {
	if len(via) == 0 {
		return PathFromIterator(qs, qs.NodesAllIterator())
	} else if len(via) == 1 {
		v := via[0]
		switch p := v.(type) {
		case nil:
			return PathFromIterator(qs, qs.NodesAllIterator())
		case *Path:
			if p.qs != qs {
				newp := &Path{
					qs:          qs,
					baseContext: p.baseContext,
					stack:       p.stack[:],
				}
				return newp
			}
			return p
		case string:
			return StartPath(qs, p)
		default:
			panic(fmt.Sprint("Invalid type passed to buildViaPath. ", p))
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
