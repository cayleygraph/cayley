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
	"regexp"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/quad"
)

// join puts two iterators together by intersecting their result sets with an AND
// Since we're using an and iterator, it's a good idea to put the smallest result
// set first so that Next() produces fewer values to check Contains().
func join(qs graph.QuadStore, its ...graph.Iterator) graph.Iterator {
	and := iterator.NewAnd(qs)
	for _, it := range its {
		if it == nil {
			continue
		}
		and.AddSubIterator(it)
	}
	return and
}

// isMorphism represents all nodes passed in-- if there are none, this function
// acts as a passthrough for the previous iterator.
func isMorphism(nodes ...quad.Value) morphism {
	return morphism{
		Name:     "is",
		Reversal: func(ctx *pathContext) (morphism, *pathContext) { return isMorphism(nodes...), ctx },
		Apply: func(qs graph.QuadStore, in graph.Iterator, ctx *pathContext) (graph.Iterator, *pathContext) {
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

func regexMorphism(pattern *regexp.Regexp, refs bool) morphism {
	return morphism{
		Name:     "regex",
		Reversal: func(ctx *pathContext) (morphism, *pathContext) { return regexMorphism(pattern, refs), ctx },
		Apply: func(qs graph.QuadStore, in graph.Iterator, ctx *pathContext) (graph.Iterator, *pathContext) {
			it := iterator.NewRegex(in, pattern, qs)
			it.AllowRefs(refs)
			return it, ctx
		},
	}
}

// cmpMorphism is the set of nodes that passes comparison iterator with the same parameters.
func cmpMorphism(op iterator.Operator, node quad.Value) morphism {
	return morphism{
		Name:     "cmp",
		Reversal: func(ctx *pathContext) (morphism, *pathContext) { return cmpMorphism(op, node), ctx },
		Apply: func(qs graph.QuadStore, in graph.Iterator, ctx *pathContext) (graph.Iterator, *pathContext) {
			return iterator.NewComparison(in, op, node, qs), ctx
		},
	}
}

// hasMorphism is the set of nodes that is reachable via either a *Path, a
// single node.(string) or a list of nodes.([]string).
func hasMorphism(via interface{}, nodes ...quad.Value) morphism {
	return morphism{
		Name:     "has",
		Reversal: func(ctx *pathContext) (morphism, *pathContext) { return hasMorphism(via, nodes...), ctx },
		Apply: func(qs graph.QuadStore, in graph.Iterator, ctx *pathContext) (graph.Iterator, *pathContext) {
			return buildHas(qs, via, in, false, nodes), ctx
		},
	}
}

func hasReverseMorphism(via interface{}, nodes ...quad.Value) morphism {
	return morphism{
		Name:     "hasr",
		Reversal: func(ctx *pathContext) (morphism, *pathContext) { return hasMorphism(via, nodes...), ctx },
		Apply: func(qs graph.QuadStore, in graph.Iterator, ctx *pathContext) (graph.Iterator, *pathContext) {
			return buildHas(qs, via, in, true, nodes), ctx
		},
	}
}

func tagMorphism(tags ...string) morphism {
	return morphism{
		Name:     "tag",
		Reversal: func(ctx *pathContext) (morphism, *pathContext) { return tagMorphism(tags...), ctx },
		Apply: func(qs graph.QuadStore, in graph.Iterator, ctx *pathContext) (graph.Iterator, *pathContext) {
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
		Reversal: func(ctx *pathContext) (morphism, *pathContext) { return inMorphism(tags, via...), ctx },
		Apply: func(qs graph.QuadStore, in graph.Iterator, ctx *pathContext) (graph.Iterator, *pathContext) {
			path := buildViaPath(qs, via...)
			return inOutIterator(path, in, false, tags, ctx), ctx
		},
		tags: tags,
	}
}

// inMorphism iterates backwards one RDF triple or via an entire path.
func inMorphism(tags []string, via ...interface{}) morphism {
	return morphism{
		Name:     "in",
		Reversal: func(ctx *pathContext) (morphism, *pathContext) { return outMorphism(tags, via...), ctx },
		Apply: func(qs graph.QuadStore, in graph.Iterator, ctx *pathContext) (graph.Iterator, *pathContext) {
			path := buildViaPath(qs, via...)
			return inOutIterator(path, in, true, tags, ctx), ctx
		},
		tags: tags,
	}
}

func bothMorphism(tags []string, via ...interface{}) morphism {
	return morphism{
		Name:     "in",
		Reversal: func(ctx *pathContext) (morphism, *pathContext) { return bothMorphism(tags, via...), ctx },
		Apply: func(qs graph.QuadStore, in graph.Iterator, ctx *pathContext) (graph.Iterator, *pathContext) {
			path := buildViaPath(qs, via...)
			inSide := inOutIterator(path, in, true, tags, ctx)
			outSide := inOutIterator(path, in.Clone(), false, tags, ctx)
			or := iterator.NewOr(inSide, outSide)
			return or, ctx
		},
		tags: tags,
	}
}

func labelContextMorphism(tags []string, via ...interface{}) morphism {
	var path *Path
	if len(via) == 0 {
		path = nil
	} else {
		path = buildViaPath(nil, via...)
		path = path.Tag(tags...)
	}
	return morphism{
		Name: "label_context",
		Reversal: func(ctx *pathContext) (morphism, *pathContext) {
			out := ctx.copy()
			ctx.labelSet = path
			return labelContextMorphism(tags, via...), &out
		},
		Apply: func(qs graph.QuadStore, in graph.Iterator, ctx *pathContext) (graph.Iterator, *pathContext) {
			out := ctx.copy()
			out.labelSet = path
			return in, &out
		},
		tags: tags,
	}
}

// predicatesMorphism iterates to the uniqified set of predicates from
// the given set of nodes in the path.
func predicatesMorphism(isIn bool) morphism {
	m := morphism{
		Name: "out_predicates",
		Reversal: func(ctx *pathContext) (morphism, *pathContext) {
			panic("not implemented: need a function from predicates to their associated edges")
		},
		Apply: func(qs graph.QuadStore, in graph.Iterator, ctx *pathContext) (graph.Iterator, *pathContext) {
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
		Reversal: func(ctx *pathContext) (morphism, *pathContext) { return iteratorMorphism(it), ctx },
		Apply: func(qs graph.QuadStore, in graph.Iterator, ctx *pathContext) (graph.Iterator, *pathContext) {
			return join(qs, it, in), ctx
		},
	}
}

// andMorphism sticks a path onto the current iterator chain.
func andMorphism(p *Path) morphism {
	return morphism{
		Name:     "and",
		Reversal: func(ctx *pathContext) (morphism, *pathContext) { return andMorphism(p), ctx },
		Apply: func(qs graph.QuadStore, in graph.Iterator, ctx *pathContext) (graph.Iterator, *pathContext) {
			itR := p.BuildIteratorOn(qs)

			return join(qs, in, itR), ctx
		},
	}
}

// orMorphism is the union, vice intersection, of a path and the current iterator.
func orMorphism(p *Path) morphism {
	return morphism{
		Name:     "or",
		Reversal: func(ctx *pathContext) (morphism, *pathContext) { return orMorphism(p), ctx },
		Apply: func(qs graph.QuadStore, in graph.Iterator, ctx *pathContext) (graph.Iterator, *pathContext) {
			itR := p.BuildIteratorOn(qs)
			or := iterator.NewOr(in, itR)
			return or, ctx
		},
	}
}

func followMorphism(p *Path) morphism {
	return morphism{
		Name:     "follow",
		Reversal: func(ctx *pathContext) (morphism, *pathContext) { return followMorphism(p.Reverse()), ctx },
		Apply: func(qs graph.QuadStore, in graph.Iterator, ctx *pathContext) (graph.Iterator, *pathContext) {
			return p.Morphism()(qs, in), ctx
		},
	}
}

// exceptMorphism removes all results on p.(*Path) from the current iterators.
func exceptMorphism(p *Path) morphism {
	return morphism{
		Name:     "except",
		Reversal: func(ctx *pathContext) (morphism, *pathContext) { return exceptMorphism(p), ctx },
		Apply: func(qs graph.QuadStore, in graph.Iterator, ctx *pathContext) (graph.Iterator, *pathContext) {
			subIt := p.BuildIteratorOn(qs)
			allNodes := qs.NodesAllIterator()
			notIn := iterator.NewNot(subIt, allNodes)

			return join(qs, in, notIn), ctx
		},
	}
}

// uniqueMorphism removes duplicate values from current path.
func uniqueMorphism() morphism {
	return morphism{
		Name:     "unique",
		Reversal: func(ctx *pathContext) (morphism, *pathContext) { return uniqueMorphism(), ctx },
		Apply: func(qs graph.QuadStore, in graph.Iterator, ctx *pathContext) (graph.Iterator, *pathContext) {
			return iterator.NewUnique(in), ctx
		},
	}
}

func saveMorphism(via interface{}, tag string) morphism {
	return morphism{
		Name:     "save",
		Reversal: func(ctx *pathContext) (morphism, *pathContext) { return saveMorphism(via, tag), ctx },
		Apply: func(qs graph.QuadStore, in graph.Iterator, ctx *pathContext) (graph.Iterator, *pathContext) {
			return buildSave(qs, via, tag, in, false, false), ctx
		},
		tags: []string{tag},
	}
}

func saveReverseMorphism(via interface{}, tag string) morphism {
	return morphism{
		Name:     "saver",
		Reversal: func(ctx *pathContext) (morphism, *pathContext) { return saveReverseMorphism(via, tag), ctx },
		Apply: func(qs graph.QuadStore, in graph.Iterator, ctx *pathContext) (graph.Iterator, *pathContext) {
			return buildSave(qs, via, tag, in, true, false), ctx
		},
		tags: []string{tag},
	}
}

func saveOptionalMorphism(via interface{}, tag string) morphism {
	return morphism{
		Name:     "saveo",
		Reversal: func(ctx *pathContext) (morphism, *pathContext) { return saveOptionalMorphism(via, tag), ctx },
		Apply: func(qs graph.QuadStore, in graph.Iterator, ctx *pathContext) (graph.Iterator, *pathContext) {
			return buildSave(qs, via, tag, in, false, true), ctx
		},
		tags: []string{tag},
	}
}

func saveOptionalReverseMorphism(via interface{}, tag string) morphism {
	return morphism{
		Name:     "saveor",
		Reversal: func(ctx *pathContext) (morphism, *pathContext) { return saveOptionalReverseMorphism(via, tag), ctx },
		Apply: func(qs graph.QuadStore, in graph.Iterator, ctx *pathContext) (graph.Iterator, *pathContext) {
			return buildSave(qs, via, tag, in, true, true), ctx
		},
		tags: []string{tag},
	}
}

func buildHas(qs graph.QuadStore, via interface{}, in graph.Iterator, reverse bool, nodes []quad.Value) graph.Iterator {
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

	start, goal := quad.Subject, quad.Object
	if reverse {
		start, goal = goal, start
	}

	trail := iterator.NewLinksTo(qs, viaIter, quad.Predicate)
	dest := iterator.NewLinksTo(qs, ends, goal)

	// If we were given nodes, intersecting with them first will
	// be extremely cheap-- otherwise, it will be the most expensive
	// (requiring iteration over all nodes). We have enough info to
	// make this optimization now since intersections are commutative
	if len(nodes) == 0 { // Where dest involves an All iterator.
		route := join(qs, trail, dest)
		has := iterator.NewHasA(qs, route, start)
		return join(qs, in, has)
	}

	// This looks backwards. That's OK-- see the note above.
	route := join(qs, dest, trail)
	has := iterator.NewHasA(qs, route, start)
	return join(qs, has, in)
}

func buildSave(
	qs graph.QuadStore, via interface{},
	tag string, from graph.Iterator, reverse bool, optional bool,
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
	save := graph.Iterator(iterator.NewHasA(qs, route, start))

	if optional {
		save = iterator.NewOptional(save)
	}
	return join(qs, from, save)
}

func inOutIterator(viaPath *Path, from graph.Iterator, inIterator bool, tags []string, ctx *pathContext) graph.Iterator {
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
	var label graph.Iterator
	if ctx != nil {
		if ctx.labelSet != nil {
			labeliter := ctx.labelSet.BuildIteratorOn(viaPath.qs)
			label = iterator.NewLinksTo(viaPath.qs, labeliter, quad.Label)
		}
	}
	route := join(viaPath.qs, source, trail, label)

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
		case quad.Value:
			return StartPath(qs, p)
		}
	}
	nodes := make([]quad.Value, 0, len(via))
	for _, v := range via {
		qv, ok := quad.AsValue(v)
		if !ok {
			panic(fmt.Errorf("Invalid type passed to buildViaPath: %v (%T)", v, v))
		}
		nodes = append(nodes, qv)
	}
	return StartPath(qs, nodes...)
}

// skipMorphism will skip a number of values-- if there are none, this function
// acts as a passthrough for the previous iterator.
func skipMorphism(v int64) morphism {
	return morphism{
		Name:     "skip",
		Reversal: func(ctx *pathContext) (morphism, *pathContext) { return skipMorphism(v), ctx },
		Apply: func(qs graph.QuadStore, in graph.Iterator, ctx *pathContext) (graph.Iterator, *pathContext) {
			if v == 0 {
				// Acting as a passthrough
				return in, ctx
			}
			return iterator.NewSkip(in, v), ctx
		},
	}
}

// limitMorphism will limit a number of values-- if number is negative or zero, this function
// acts as a passthrough for the previous iterator.
func limitMorphism(v int64) morphism {
	return morphism{
		Name:     "limit",
		Reversal: func(ctx *pathContext) (morphism, *pathContext) { return limitMorphism(v), ctx },
		Apply: func(qs graph.QuadStore, in graph.Iterator, ctx *pathContext) (graph.Iterator, *pathContext) {
			if v <= 0 {
				// Acting as a passthrough
				return in, ctx
			}
			return iterator.NewLimit(in, v), ctx
		},
	}
}

// countMorphism will return count of values.
func countMorphism() morphism {
	return morphism{
		Name:     "count",
		Reversal: func(ctx *pathContext) (morphism, *pathContext) { return countMorphism(), ctx },
		Apply: func(qs graph.QuadStore, in graph.Iterator, ctx *pathContext) (graph.Iterator, *pathContext) {
			return iterator.NewCount(in, qs), ctx
		},
	}
}
