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
	"regexp"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/quad"
	"golang.org/x/net/context"
)

type applyMorphism func(graph.QuadStore, graph.Iterator, *pathContext) (graph.Iterator, *pathContext)

type morphism struct {
	Name     string
	Reversal func(*pathContext) (morphism, *pathContext)
	Apply    applyMorphism
	tags     []string
}

// pathContext allows a high-level change to the way paths are constructed. Some
// functions may change the context, causing following chained calls to act
// differently.
//
// In a sense, this is a global state which can be changed as the path
// continues. And as with dealing with any global state, care should be taken:
//
// When modifying the context in Apply(), please copy the passed struct,
// modifying the relevant fields if need be (or pass the given context onward).
//
// Under Reversal(), any functions that wish to change the context should
// appropriately change the passed context (that is, the context that came after
// them will now be what the application of the function would have been) and
// then yield a pointer to their own member context as the return value.
//
// For more examples, look at the morphisms which claim the individual fields.
type pathContext struct {
	// TODO(dennwc): replace with net/context?

	// Represents the path to the limiting set of labels that should be considered under traversal.
	// inMorphism, outMorphism, et al should constrain edges by this set.
	// A nil in this field represents all labels.
	//
	// Claimed by the withLabel morphism
	labelSet *Path
}

func (c pathContext) copy() pathContext {
	return pathContext{
		labelSet: c.labelSet,
	}
}

// Path represents either a morphism (a pre-defined path stored for later use),
// or a concrete path, consisting of a morphism and an underlying QuadStore.
type Path struct {
	stack       []morphism
	qs          graph.QuadStore // Optionally. A nil qs is equivalent to a morphism.
	baseContext pathContext
}

// IsMorphism returns whether this Path is a morphism.
func (p *Path) IsMorphism() bool { return p.qs == nil }

// StartMorphism creates a new Path with no underlying QuadStore.
func StartMorphism(nodes ...quad.Value) *Path {
	return StartPath(nil, nodes...)
}

// StartPath creates a new Path from a set of nodes and an underlying QuadStore.
func StartPath(qs graph.QuadStore, nodes ...quad.Value) *Path {
	return &Path{
		stack: []morphism{
			isMorphism(nodes...),
		},
		qs: qs,
	}
}

func PathFromIterator(qs graph.QuadStore, it graph.Iterator) *Path {
	return &Path{
		stack: []morphism{
			iteratorMorphism(it),
		},
		qs: qs,
	}
}

// NewPath creates a new, empty Path.
func NewPath(qs graph.QuadStore) *Path {
	return &Path{
		qs: qs,
	}
}

// Clone returns a clone of the current path.
func (p *Path) Clone() *Path {
	stack := p.stack
	return &Path{
		stack:       stack[:len(stack):len(stack)],
		qs:          p.qs,
		baseContext: p.baseContext,
	}
}

// Unexported clone method returns a *Path with a copy of the original stack,
// with assumption that the new stack will be appended to.
func (p *Path) clone() *Path {
	stack := p.stack
	p.stack = stack[:len(stack):len(stack)]
	return &Path{
		stack:       stack,
		qs:          p.qs,
		baseContext: p.baseContext,
	}
}

// Reverse returns a new Path that is the reverse of the current one.
func (p *Path) Reverse() *Path {
	newPath := NewPath(p.qs)
	ctx := &newPath.baseContext
	for i := len(p.stack) - 1; i >= 0; i-- {
		var revMorphism morphism
		revMorphism, ctx = p.stack[i].Reversal(ctx)
		newPath.stack = append(newPath.stack, revMorphism)
	}
	return newPath
}

// Is declares that the current nodes in this path are only the nodes
// passed as arguments.
func (p *Path) Is(nodes ...quad.Value) *Path {
	np := p.clone()
	np.stack = append(np.stack, isMorphism(nodes...))
	return np
}

// Regex represents the nodes that are matching provided regexp pattern.
// It will only include Raw and String values.
func (p *Path) Regex(pattern *regexp.Regexp) *Path {
	np := p.clone()
	np.stack = append(np.stack, regexMorphism(pattern, false))
	return np
}

// RegexWithRefs is the same as Regex, but also matches IRIs and BNodes.
//
// Consider using it carefully. In most cases it's better to reconsider
// your graph structure instead of relying on slow unoptimizable regexp.
//
// An example of incorrect usage is to match IRIs:
// 	<http://example.org/page>
// 	<http://example.org/page/foo>
// Via regexp like:
//	http://example.org/page.*
//
// The right way is to explicitly link graph nodes and query them by this relation:
// 	<http://example.org/page/foo> <type> <http://example.org/page>
func (p *Path) RegexWithRefs(pattern *regexp.Regexp) *Path {
	np := p.clone()
	np.stack = append(np.stack, regexMorphism(pattern, true))
	return np
}

// Filter represents the nodes that are passing comparison with provided value.
func (p *Path) Filter(op iterator.Operator, node quad.Value) *Path {
	np := p.clone()
	np.stack = append(np.stack, cmpMorphism(op, node))
	return np
}

// Tag adds tag strings to the nodes at this point in the path for each result
// path in the set.
func (p *Path) Tag(tags ...string) *Path {
	np := p.clone()
	np.stack = append(np.stack, tagMorphism(tags...))
	return np
}

// Out updates this Path to represent the nodes that are adjacent to the
// current nodes, via the given outbound predicate.
//
// For example:
//  // Returns the list of nodes that "B" follows.
//  //
//  // Will return []string{"F"} if there is a predicate (edge) from "B"
//  // to "F" labelled "follows".
//  StartPath(qs, "A").Out("follows")
func (p *Path) Out(via ...interface{}) *Path {
	np := p.clone()
	np.stack = append(np.stack, outMorphism(nil, via...))
	return np
}

// In updates this Path to represent the nodes that are adjacent to the
// current nodes, via the given inbound predicate.
//
// For example:
//  // Return the list of nodes that follow "B".
//  //
//  // Will return []string{"A", "C", "D"} if there are the appropriate
//  // edges from those nodes to "B" labelled "follows".
//  StartPath(qs, "B").In("follows")
func (p *Path) In(via ...interface{}) *Path {
	np := p.clone()
	np.stack = append(np.stack, inMorphism(nil, via...))
	return np
}

// InWithTags is exactly like In, except it tags the value of the predicate
// traversed with the tags provided.
func (p *Path) InWithTags(tags []string, via ...interface{}) *Path {
	np := p.clone()
	np.stack = append(np.stack, inMorphism(tags, via...))
	return np
}

// OutWithTags is exactly like In, except it tags the value of the predicate
// traversed with the tags provided.
func (p *Path) OutWithTags(tags []string, via ...interface{}) *Path {
	np := p.clone()
	np.stack = append(np.stack, outMorphism(tags, via...))
	return np
}

// Both updates this path following both inbound and outbound predicates.
//
// For example:
//  // Return the list of nodes that follow or are followed by "B".
//  //
//  // Will return []string{"A", "C", "D", "F} if there are the appropriate
//  // edges from those nodes to "B" labelled "follows", in either direction.
//  StartPath(qs, "B").Both("follows")
func (p *Path) Both(via ...interface{}) *Path {
	np := p.clone()
	np.stack = append(np.stack, bothMorphism(nil, via...))
	return np
}

// InPredicates updates this path to represent the nodes of the valid inbound
// predicates from the current nodes.
//
// For example:
//  // Returns a list of predicates valid from "bob"
//  //
//  // Will return []string{"follows"} if there are any things that "follow" Bob
//  StartPath(qs, "bob").InPredicates()
func (p *Path) InPredicates() *Path {
	np := p.clone()
	np.stack = append(np.stack, predicatesMorphism(true))
	return np
}

// OutPredicates updates this path to represent the nodes of the valid inbound
// predicates from the current nodes.
//
// For example:
//  // Returns a list of predicates valid from "bob"
//  //
//  // Will return []string{"follows", "status"} if there are edges from "bob"
//  // labelled "follows", and edges from "bob" that describe his "status".
//  StartPath(qs, "bob").OutPredicates()
func (p *Path) OutPredicates() *Path {
	np := p.clone()
	np.stack = append(np.stack, predicatesMorphism(false))
	return np
}

// And updates the current Path to represent the nodes that match both the
// current Path so far, and the given Path.
func (p *Path) And(path *Path) *Path {
	np := p.clone()
	np.stack = append(np.stack, andMorphism(path))
	return np
}

// Or updates the current Path to represent the nodes that match either the
// current Path so far, or the given Path.
func (p *Path) Or(path *Path) *Path {
	np := p.clone()
	np.stack = append(np.stack, orMorphism(path))
	return np
}

// Except updates the current Path to represent the all of the current nodes
// except those in the supplied Path.
//
// For example:
//  // Will return []string{"B"}
//  StartPath(qs, "A", "B").Except(StartPath(qs, "A"))
func (p *Path) Except(path *Path) *Path {
	np := p.clone()
	np.stack = append(np.stack, exceptMorphism(path))
	return np
}

// Unique updates the current Path to contain only unique nodes.
func (p *Path) Unique() *Path {
	np := p.clone()
	np.stack = append(np.stack, uniqueMorphism())
	return np
}

// Follow allows you to stitch two paths together. The resulting path will start
// from where the first path left off and continue iterating down the path given.
func (p *Path) Follow(path *Path) *Path {
	np := p.clone()
	np.stack = append(np.stack, followMorphism(path))
	return np
}

// FollowReverse is the same as follow, except it will iterate backwards up the
// path given as argument.
func (p *Path) FollowReverse(path *Path) *Path {
	np := p.clone()
	np.stack = append(np.stack, followMorphism(path.Reverse()))
	return np
}

// Save will, from the current nodes in the path, retrieve the node
// one linkage away (given by either a path or a predicate), add the given
// tag, and propagate that to the result set.
//
// For example:
//  // Will return []map[string]string{{"social_status: "cool"}}
//  StartPath(qs, "B").Save("status", "social_status"
func (p *Path) Save(via interface{}, tag string) *Path {
	np := p.clone()
	np.stack = append(np.stack, saveMorphism(via, tag))
	return np
}

// SaveReverse is the same as Save, only in the reverse direction
// (the subject of the linkage should be tagged, instead of the object).
func (p *Path) SaveReverse(via interface{}, tag string) *Path {
	np := p.clone()
	np.stack = append(np.stack, saveReverseMorphism(via, tag))
	return np
}

// SaveOptional is the same as Save, but does not require linkage to exist.
func (p *Path) SaveOptional(via interface{}, tag string) *Path {
	np := p.clone()
	np.stack = append(np.stack, saveOptionalMorphism(via, tag))
	return np
}

// SaveOptionalReverse is the same as SaveReverse, but does not require linkage to exist.
func (p *Path) SaveOptionalReverse(via interface{}, tag string) *Path {
	np := p.clone()
	np.stack = append(np.stack, saveOptionalReverseMorphism(via, tag))
	return np
}

// Has limits the paths to be ones where the current nodes have some linkage
// to some known node.
func (p *Path) Has(via interface{}, nodes ...quad.Value) *Path {
	np := p.clone()
	np.stack = append(np.stack, hasMorphism(via, nodes...))
	return np
}

// HasReverse limits the paths to be ones where some known node have some linkage
// to the current nodes.
func (p *Path) HasReverse(via interface{}, nodes ...quad.Value) *Path {
	np := p.clone()
	np.stack = append(np.stack, hasReverseMorphism(via, nodes...))
	return np
}

// LabelContext restricts the following operations (such as In, Out) to only
// traverse edges that match the given set of labels.
func (p *Path) LabelContext(via ...interface{}) *Path {
	np := p.clone()
	np.stack = append(np.stack, labelContextMorphism(nil, via...))
	return np
}

// LabelContextWithTags is exactly like LabelContext, except it tags the value
// of the label used in the traversal with the tags provided.
func (p *Path) LabelContextWithTags(tags []string, via ...interface{}) *Path {
	np := p.clone()
	np.stack = append(np.stack, labelContextMorphism(tags, via...))
	return np
}

// Back returns to a previously tagged place in the path. Any constraints applied after the Tag will remain in effect, but traversal continues from the tagged point instead, not from the end of the chain.
//
// For example:
//  // Will return "bob" iff "bob" is cool
//  StartPath(qs, "bob").Tag("person_tag").Out("status").Is("cool").Back("person_tag")
func (p *Path) Back(tag string) *Path {
	newPath := NewPath(p.qs)
	i := len(p.stack) - 1
	ctx := &newPath.baseContext
	for {
		if i < 0 {
			return p.Reverse()
		}
		if p.stack[i].Name == "tag" {
			for _, x := range p.stack[i].tags {
				if x == tag {
					// Found what we're looking for.
					p.stack = p.stack[:i+1]
					return p.And(newPath)
				}
			}
		}
		var revMorphism morphism
		revMorphism, ctx = p.stack[i].Reversal(ctx)
		newPath.stack = append(newPath.stack, revMorphism)
		i--
	}
}

// BuildIterator returns an iterator from this given Path.  Note that you must
// call this with a full path (not a morphism), since a morphism does not have
// the ability to fetch the underlying quads.  This function will panic if
// called with a morphism (i.e. if p.IsMorphism() is true).
func (p *Path) BuildIterator() graph.Iterator {
	if p.IsMorphism() {
		panic("Building an iterator from a morphism. Bind a QuadStore with BuildIteratorOn(qs)")
	}
	return p.BuildIteratorOn(p.qs)
}

// BuildIteratorOn will return an iterator for this path on the given QuadStore.
func (p *Path) BuildIteratorOn(qs graph.QuadStore) graph.Iterator {
	return p.Morphism()(qs, qs.NodesAllIterator())
}

// Morphism returns the morphism of this path.  The returned value is a
// function that, when given a QuadStore and an existing Iterator, will
// return a new Iterator that yields the subset of values from the existing
// iterator matched by the current Path.
func (p *Path) Morphism() graph.ApplyMorphism {
	return func(qs graph.QuadStore, it graph.Iterator) graph.Iterator {
		i := it.Clone()
		ctx := &p.baseContext
		for _, m := range p.stack {
			i, ctx = m.Apply(qs, i, ctx)
		}
		return i
	}
}

// Skip will omit a number of values from result set.
func (p *Path) Skip(v int64) *Path {
	p.stack = append(p.stack, skipMorphism(v))
	return p
}

// Limit will limit a number of values in result set.
func (p *Path) Limit(v int64) *Path {
	p.stack = append(p.stack, limitMorphism(v))
	return p
}

// Count will count a number of results as it's own result set.
func (p *Path) Count() *Path {
	p.stack = append(p.stack, countMorphism())
	return p
}

// Iterate is an shortcut for graph.Iterate.
func (p *Path) Iterate(ctx context.Context) *graph.IterateChain {
	return graph.Iterate(ctx, p.BuildIterator()).On(p.qs)
}
