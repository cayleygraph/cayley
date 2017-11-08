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
	"github.com/cayleygraph/cayley/graph/shape"
	"github.com/cayleygraph/cayley/quad"
)

// join puts two iterators together by intersecting their result sets with an AND
// Since we're using an and iterator, it's a good idea to put the smallest result
// set first so that Next() produces fewer values to check Contains().
func join(its ...shape.Shape) shape.Shape {
	if len(its) == 0 {
		return shape.Null{}
	} else if _, ok := its[0].(shape.AllNodes); ok {
		return join(its[1:]...)
	}
	return shape.Intersect(its)
}

// isMorphism represents all nodes passed in-- if there are none, this function
// acts as a passthrough for the previous iterator.
func isMorphism(nodes ...quad.Value) morphism {
	return morphism{
		Name:     "is",
		Reversal: func(ctx *pathContext) (morphism, *pathContext) { return isMorphism(nodes...), ctx },
		Apply: func(in shape.Shape, ctx *pathContext) (shape.Shape, *pathContext) {
			if len(nodes) == 0 {
				// Acting as a passthrough here is equivalent to
				// building a NodesAllIterator to Next() or Contains()
				// from here as in previous versions.
				return in, ctx
			}
			s := shape.Lookup(nodes)
			if _, ok := in.(shape.AllNodes); ok {
				return s, ctx
			}
			// Anything with fixedIterators will usually have a much
			// smaller result set, so join isNodes first here.
			return join(s, in), ctx
		},
	}
}

func regexMorphism(pattern *regexp.Regexp, refs bool) morphism {
	return morphism{
		Name:     "regex",
		Reversal: func(ctx *pathContext) (morphism, *pathContext) { return regexMorphism(pattern, refs), ctx },
		Apply: func(in shape.Shape, ctx *pathContext) (shape.Shape, *pathContext) {
			return shape.Filter{From: in,
				Filters: []shape.ValueFilter{
					shape.Regexp{Re: pattern, Refs: refs},
				},
			}, ctx
		},
	}
}

// isNodeMorphism represents all nodes passed in-- if there are none, this function
// acts as a passthrough for the previous iterator.
func isNodeMorphism(nodes ...graph.Value) morphism {
	return morphism{
		Name:     "is",
		Reversal: func(ctx *pathContext) (morphism, *pathContext) { return isNodeMorphism(nodes...), ctx },
		Apply: func(in shape.Shape, ctx *pathContext) (shape.Shape, *pathContext) {
			if len(nodes) == 0 {
				// Acting as a passthrough here is equivalent to
				// building a NodesAllIterator to Next() or Contains()
				// from here as in previous versions.
				return in, ctx
			}
			// Anything with fixedIterators will usually have a much
			// smaller result set, so join isNodes first here.
			return join(shape.Fixed(nodes), in), ctx
		},
	}
}

// cmpMorphism is the set of nodes that passes comparison iterator with the same parameters.
func cmpMorphism(op iterator.Operator, node quad.Value) morphism {
	return morphism{
		Name:     "cmp",
		Reversal: func(ctx *pathContext) (morphism, *pathContext) { return cmpMorphism(op, node), ctx },
		Apply: func(in shape.Shape, ctx *pathContext) (shape.Shape, *pathContext) {
			return shape.Filter{
				From: in,
				Filters: []shape.ValueFilter{
					shape.Comparison{Op: op, Val: node},
				},
			}, ctx
		},
	}
}

// hasMorphism is the set of nodes that is reachable via either a *Path, a
// single node.(string) or a list of nodes.([]string).
func hasMorphism(via interface{}, nodes ...quad.Value) morphism {
	return morphism{
		Name:     "has",
		Reversal: func(ctx *pathContext) (morphism, *pathContext) { return hasMorphism(via, nodes...), ctx },
		Apply: func(in shape.Shape, ctx *pathContext) (shape.Shape, *pathContext) {
			var node shape.Shape
			if len(nodes) == 0 {
				node = shape.AllNodes{}
			} else {
				node = shape.Lookup(nodes)
			}
			return shape.Has(in, buildVia(via), node, false), ctx
		},
	}
}

func hasReverseMorphism(via interface{}, nodes ...quad.Value) morphism {
	return morphism{
		Name:     "hasr",
		Reversal: func(ctx *pathContext) (morphism, *pathContext) { return hasMorphism(via, nodes...), ctx },
		Apply: func(in shape.Shape, ctx *pathContext) (shape.Shape, *pathContext) {
			return shape.Has(in, buildVia(via), shape.Lookup(nodes), true), ctx
		},
	}
}

func tagMorphism(tags ...string) morphism {
	return morphism{
		Name:     "tag",
		Reversal: func(ctx *pathContext) (morphism, *pathContext) { return tagMorphism(tags...), ctx },
		Apply: func(in shape.Shape, ctx *pathContext) (shape.Shape, *pathContext) {
			return shape.Save{From: in, Tags: tags}, ctx
		},
		tags: tags,
	}
}

// outMorphism iterates forward one RDF triple or via an entire path.
func outMorphism(tags []string, via ...interface{}) morphism {
	return morphism{
		Name:     "out",
		Reversal: func(ctx *pathContext) (morphism, *pathContext) { return inMorphism(tags, via...), ctx },
		Apply: func(in shape.Shape, ctx *pathContext) (shape.Shape, *pathContext) {
			return shape.Out(in, buildVia(via...), ctx.labelSet, tags...), ctx
		},
		tags: tags,
	}
}

// inMorphism iterates backwards one RDF triple or via an entire path.
func inMorphism(tags []string, via ...interface{}) morphism {
	return morphism{
		Name:     "in",
		Reversal: func(ctx *pathContext) (morphism, *pathContext) { return outMorphism(tags, via...), ctx },
		Apply: func(in shape.Shape, ctx *pathContext) (shape.Shape, *pathContext) {
			return shape.In(in, buildVia(via...), ctx.labelSet, tags...), ctx
		},
		tags: tags,
	}
}

func bothMorphism(tags []string, via ...interface{}) morphism {
	return morphism{
		Name:     "in",
		Reversal: func(ctx *pathContext) (morphism, *pathContext) { return bothMorphism(tags, via...), ctx },
		Apply: func(in shape.Shape, ctx *pathContext) (shape.Shape, *pathContext) {
			via := buildVia(via...)
			return shape.Union{
				shape.In(in, via, ctx.labelSet, tags...),
				shape.Out(in, via, ctx.labelSet, tags...),
			}, ctx
		},
		tags: tags,
	}
}

func labelContextMorphism(tags []string, via ...interface{}) morphism {
	var path shape.Shape
	if len(via) == 0 {
		path = nil
	} else {
		path = shape.Save{From: buildVia(via...), Tags: tags}
	}
	return morphism{
		Name: "label_context",
		Reversal: func(ctx *pathContext) (morphism, *pathContext) {
			out := ctx.copy()
			ctx.labelSet = path
			return labelContextMorphism(tags, via...), &out
		},
		Apply: func(in shape.Shape, ctx *pathContext) (shape.Shape, *pathContext) {
			out := ctx.copy()
			out.labelSet = path
			return in, &out
		},
		tags: tags,
	}
}

// labelsMorphism iterates to the uniqified set of labels from
// the given set of nodes in the path.
func labelsMorphism() morphism {
	return morphism{
		Name: "labels",
		Reversal: func(ctx *pathContext) (morphism, *pathContext) {
			panic("not implemented")
		},
		Apply: func(in shape.Shape, ctx *pathContext) (shape.Shape, *pathContext) {
			return shape.Labels(in), ctx
		},
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
		Apply: func(in shape.Shape, ctx *pathContext) (shape.Shape, *pathContext) {
			return shape.Predicates(in, isIn), ctx
		},
	}
	if isIn {
		m.Name = "in_predicates"
	}
	return m
}

type iteratorShape struct {
	it graph.Iterator
}

func (s iteratorShape) BuildIterator(qs graph.QuadStore) graph.Iterator {
	return s.it.Clone()
}
func (s iteratorShape) Optimize(r shape.Optimizer) (shape.Shape, bool) {
	return s, false
}

// iteratorMorphism simply tacks the input iterator onto the chain.
func iteratorMorphism(it graph.Iterator) morphism {
	return morphism{
		Name:     "iterator",
		Reversal: func(ctx *pathContext) (morphism, *pathContext) { return iteratorMorphism(it), ctx },
		Apply: func(in shape.Shape, ctx *pathContext) (shape.Shape, *pathContext) {
			return join(iteratorShape{it}, in), ctx
		},
	}
}

// andMorphism sticks a path onto the current iterator chain.
func andMorphism(p *Path) morphism {
	return morphism{
		Name:     "and",
		Reversal: func(ctx *pathContext) (morphism, *pathContext) { return andMorphism(p), ctx },
		Apply: func(in shape.Shape, ctx *pathContext) (shape.Shape, *pathContext) {
			return join(in, p.Shape()), ctx
		},
	}
}

// orMorphism is the union, vice intersection, of a path and the current iterator.
func orMorphism(p *Path) morphism {
	return morphism{
		Name:     "or",
		Reversal: func(ctx *pathContext) (morphism, *pathContext) { return orMorphism(p), ctx },
		Apply: func(in shape.Shape, ctx *pathContext) (shape.Shape, *pathContext) {
			return shape.Union{in, p.Shape()}, ctx
		},
	}
}

func followMorphism(p *Path) morphism {
	return morphism{
		Name:     "follow",
		Reversal: func(ctx *pathContext) (morphism, *pathContext) { return followMorphism(p.Reverse()), ctx },
		Apply: func(in shape.Shape, ctx *pathContext) (shape.Shape, *pathContext) {
			return p.ShapeFrom(in), ctx
		},
	}
}

type iteratorBuilder func(qs graph.QuadStore) graph.Iterator

func (s iteratorBuilder) BuildIterator(qs graph.QuadStore) graph.Iterator {
	return s(qs)
}
func (s iteratorBuilder) Optimize(r shape.Optimizer) (shape.Shape, bool) {
	return s, false
}

func followRecursiveMorphism(p *Path, maxDepth int, depthTags []string) morphism {
	return morphism{
		Name: "follow_recursive",
		Reversal: func(ctx *pathContext) (morphism, *pathContext) {
			return followRecursiveMorphism(p.Reverse(), maxDepth, depthTags), ctx
		},
		Apply: func(in shape.Shape, ctx *pathContext) (shape.Shape, *pathContext) {
			return iteratorBuilder(func(qs graph.QuadStore) graph.Iterator {
				in := in.BuildIterator(qs)
				it := iterator.NewRecursive(qs, in, p.Morphism(), maxDepth)
				for _, s := range depthTags {
					it.AddDepthTag(s)
				}
				return it
			}), ctx
		},
	}
}

// exceptMorphism removes all results on p.(*Path) from the current iterators.
func exceptMorphism(p *Path) morphism {
	return morphism{
		Name:     "except",
		Reversal: func(ctx *pathContext) (morphism, *pathContext) { return exceptMorphism(p), ctx },
		Apply: func(in shape.Shape, ctx *pathContext) (shape.Shape, *pathContext) {
			return join(in, shape.Except{From: shape.AllNodes{}, Exclude: p.Shape()}), ctx
		},
	}
}

// uniqueMorphism removes duplicate values from current path.
func uniqueMorphism() morphism {
	return morphism{
		Name:     "unique",
		Reversal: func(ctx *pathContext) (morphism, *pathContext) { return uniqueMorphism(), ctx },
		Apply: func(in shape.Shape, ctx *pathContext) (shape.Shape, *pathContext) {
			return shape.Unique{in}, ctx
		},
	}
}

func saveMorphism(via interface{}, tag string) morphism {
	return morphism{
		Name:     "save",
		Reversal: func(ctx *pathContext) (morphism, *pathContext) { return saveMorphism(via, tag), ctx },
		Apply: func(in shape.Shape, ctx *pathContext) (shape.Shape, *pathContext) {
			return shape.SaveVia(in, buildVia(via), tag, false, false), ctx
		},
		tags: []string{tag},
	}
}

func saveReverseMorphism(via interface{}, tag string) morphism {
	return morphism{
		Name:     "saver",
		Reversal: func(ctx *pathContext) (morphism, *pathContext) { return saveReverseMorphism(via, tag), ctx },
		Apply: func(in shape.Shape, ctx *pathContext) (shape.Shape, *pathContext) {
			return shape.SaveVia(in, buildVia(via), tag, true, false), ctx
		},
		tags: []string{tag},
	}
}

func saveOptionalMorphism(via interface{}, tag string) morphism {
	return morphism{
		Name:     "saveo",
		Reversal: func(ctx *pathContext) (morphism, *pathContext) { return saveOptionalMorphism(via, tag), ctx },
		Apply: func(in shape.Shape, ctx *pathContext) (shape.Shape, *pathContext) {
			return shape.SaveVia(in, buildVia(via), tag, false, true), ctx
		},
		tags: []string{tag},
	}
}

func saveOptionalReverseMorphism(via interface{}, tag string) morphism {
	return morphism{
		Name:     "saveor",
		Reversal: func(ctx *pathContext) (morphism, *pathContext) { return saveOptionalReverseMorphism(via, tag), ctx },
		Apply: func(in shape.Shape, ctx *pathContext) (shape.Shape, *pathContext) {
			return shape.SaveVia(in, buildVia(via), tag, true, true), ctx
		},
		tags: []string{tag},
	}
}

func buildVia(via ...interface{}) shape.Shape {
	if len(via) == 0 {
		return shape.AllNodes{}
	} else if len(via) == 1 {
		v := via[0]
		switch p := v.(type) {
		case nil:
			return shape.AllNodes{}
		case *Path:
			return p.Shape()
		case quad.Value:
			return shape.Lookup{p}
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
	return shape.Lookup(nodes)
}

// skipMorphism will skip a number of values-- if there are none, this function
// acts as a passthrough for the previous iterator.
func skipMorphism(v int64) morphism {
	return morphism{
		Name:     "skip",
		Reversal: func(ctx *pathContext) (morphism, *pathContext) { return skipMorphism(v), ctx },
		Apply: func(in shape.Shape, ctx *pathContext) (shape.Shape, *pathContext) {
			if v == 0 {
				// Acting as a passthrough
				return in, ctx
			}
			return shape.Page{From: in, Skip: v}, ctx
		},
	}
}

// limitMorphism will limit a number of values-- if number is negative or zero, this function
// acts as a passthrough for the previous iterator.
func limitMorphism(v int64) morphism {
	return morphism{
		Name:     "limit",
		Reversal: func(ctx *pathContext) (morphism, *pathContext) { return limitMorphism(v), ctx },
		Apply: func(in shape.Shape, ctx *pathContext) (shape.Shape, *pathContext) {
			if v <= 0 {
				// Acting as a passthrough
				return in, ctx
			}
			return shape.Page{From: in, Limit: v}, ctx
		},
	}
}

// countMorphism will return count of values.
func countMorphism() morphism {
	return morphism{
		Name:     "count",
		Reversal: func(ctx *pathContext) (morphism, *pathContext) { return countMorphism(), ctx },
		Apply: func(in shape.Shape, ctx *pathContext) (shape.Shape, *pathContext) {
			return shape.Count{Values: in}, ctx
		},
	}
}
