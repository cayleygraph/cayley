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
	"reflect"
)

type applyMorphism func(graph.QuadStore, graph.Iterator, *context) (graph.Iterator, *context)

type morphism struct {
	Name     string
	Reversal func(*context) (morphism, *context)
	Apply    applyMorphism
	tags     []string
}

// context allows a high-level change to the way paths are constructed. Some
// functions may change the context, causing following chained calls to act
// cdifferently.
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
type context struct {
	// Represents the path to the limiting set of labels that should be considered under traversal.
	// inMorphism, outMorphism, et al should constrain edges by this set.
	// A nil in this field represents all labels.
	//
	// Claimed by the withLabel morphism
	labelSet *Path
}

func (c context) copy() context {
	return context{
		labelSet: c.labelSet,
	}
}

var (
	_ PathObj = (*Path)(nil)

//	_ Nodes = (*Path)(nil)
)

// Path represents either a morphism (a pre-defined path stored for later use),
// or a concrete path, consisting of a morphism and an underlying QuadStore.
type Path struct {
	nodes       Nodes
	qs          graph.QuadStore // Optionally. A nil qs is equivalent to a morphism.
	baseContext context
}

// IsMorphism returns whether this Path is a morphism.
func (p *Path) IsMorphism() bool { return p.qs == nil }

// StartMorphism creates a new Path with no underlying QuadStore.
func StartMorphism(nodes ...string) *Path {
	return StartPath(nil, nodes...)
}

// StartPath creates a new Path from a set of nodes and an underlying QuadStore.
func StartPath(qs graph.QuadStore, nodes ...string) *Path {
	var p Nodes
	if len(nodes) == 0 {
		p = AllNodes{}
	} else {
		p = Fixed(nodes)
	}
	if qs == nil {
		p = IntersectNodes{
			Start{}, p,
		}
	}
	return &Path{
		nodes: p,
		qs:    qs,
	}
}

func PathFromIterator(qs graph.QuadStore, it graph.Iterator) *Path {
	return &Path{
		nodes: NodeIteratorBuilder{Builder: func() graph.Iterator { return it.Clone() }},
		qs:    qs,
	}
}

// NewPath creates a new, empty Path.
func NewPath(qs graph.QuadStore) *Path {
	return &Path{
		qs: qs,
	}
}

// Reverse returns a new Path that is the reverse of the current one.
func (p *Path) Reverse() *Path {
	panic("not implemented yet") // TODO
	//	newPath := NewPath(p.qs)
	//	ctx := &newPath.baseContext
	//	for i := len(p.stack) - 1; i >= 0; i-- {
	//		var revMorphism morphism
	//		revMorphism, ctx = p.stack[i].Reversal(ctx)
	//		newPath.stack = append(newPath.stack, revMorphism)
	//	}
	//	return newPath
}

// Is declares that the current nodes in this path are only the nodes
// passed as arguments.
func (p *Path) Is(nodes ...string) *Path {
	if p.nodes == nil {
		p.nodes = Fixed(nodes)
	} else {
		p.nodes = IntersectNodes{
			p.nodes,
			Fixed(nodes),
		}
	}
	return p
}

// Tag adds tag strings to the nodes at this point in the path for each result
// path in the set.
func (p *Path) Tag(tags ...string) *Path {
	p.nodes = Tag{
		Nodes: p.nodes,
		Tags:  tags,
	}
	return p
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
	p.nodes = Out{
		From: p.nodes,
		Via:  buildViaPath(via...),
	}
	return p
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
	p.nodes = Out{
		From: p.nodes,
		Via:  buildViaPath(via...),
		Rev:  true,
	}
	return p
}

// InWithTags is exactly like In, except it tags the value of the predicate
// traversed with the tags provided.
func (p *Path) InWithTags(tags []string, via ...interface{}) *Path {
	p.nodes = Out{
		From: p.nodes,
		Via:  buildViaPath(via...),
		Tags: tags,
		Rev:  true,
	}
	return p
}

// OutWithTags is exactly like In, except it tags the value of the predicate
// traversed with the tags provided.
func (p *Path) OutWithTags(tags []string, via ...interface{}) *Path {
	p.nodes = Out{
		From: p.nodes,
		Via:  buildViaPath(via...),
		Tags: tags,
	}
	return p
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
	panic("not implemented yet") // TODO
	//	p.stack = append(p.stack, bothMorphism(nil, via...))
	return p
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
	p.nodes = Predicates{
		From: p.nodes,
		Rev:  true,
	}
	return p
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
	p.nodes = Predicates{
		From: p.nodes,
	}
	return p
}

// And updates the current Path to represent the nodes that match both the
// current Path so far, and the given Path.
func (p *Path) And(path *Path) *Path {
	p.nodes = IntersectNodes{
		p.nodes,
		path.nodes,
	}
	return p
}

// Or updates the current Path to represent the nodes that match either the
// current Path so far, or the given Path.
func (p *Path) Or(path *Path) *Path {
	p.nodes = UnionNodes{
		p.nodes,
		path.nodes,
	}
	return p
}

// Except updates the current Path to represent the all of the current nodes
// except those in the supplied Path.
//
// For example:
//  // Will return []string{"B"}
//  StartPath(qs, "A", "B").Except(StartPath(qs, "A"))
func (p *Path) Except(path *Path) *Path {
	p.nodes = Except{
		From:  p.nodes,
		Nodes: path.nodes,
	}
	return p
}

// Follow allows you to stitch two paths together. The resulting path will start
// from where the first path left off and continue iterating down the path given.
func (p *Path) Follow(path *Path) *Path {
	p.nodes = Follow(p.nodes, path.nodes)
	return p
}

// FollowReverse is the same as follow, except it will iterate backwards up the
// path given as argument.
func (p *Path) FollowReverse(path *Path) *Path {
	panic("not implemented yet") // TODO
	//	p.stack = append(p.stack, followMorphism(path.Reverse()))
	return p
}

// Save will, from the current nodes in the path, retrieve the node
// one linkage away (given by either a path or a predicate), add the given
// tag, and propagate that to the result set.
//
// For example:
//  // Will return []map[string]string{{"social_status: "cool"}}
//  StartPath(qs, "B").Save("status", "social_status"
func (p *Path) Save(via interface{}, tag string) *Path {
	p.nodes = Save{
		From: p.nodes,
		Via:  buildViaPath(via),
		Tags: []string{tag},
	}
	return p
}

// SaveReverse is the same as Save, only in the reverse direction
// (the subject of the linkage should be tagged, instead of the object).
func (p *Path) SaveReverse(via interface{}, tag string) *Path {
	p.nodes = Save{
		From: p.nodes,
		Via:  buildViaPath(via),
		Tags: []string{tag},
		Rev:  true,
	}
	return p
}

// Has limits the paths to be ones where the current nodes have some linkage
// to some known node.
func (p *Path) Has(via interface{}, nodes ...string) *Path {
	var n Nodes
	if len(nodes) == 0 {
		n = AllNodes{}
	} else {
		n = Fixed(nodes)
	}
	p.nodes = Has{
		From:  p.nodes,
		Via:   buildViaPath(via),
		Nodes: n,
	}
	return p
}

// LabelContext restricts the following operations (such as In, Out) to only
// traverse edges that match the given set of labels.
func (p *Path) LabelContext(via ...interface{}) *Path {
	panic("not implemented yet") // TODO
	//	p.stack = append(p.stack, labelContextMorphism(nil, via...))
	return p
}

// LabelContextWithTags is exactly like LabelContext, except it tags the value
// of the label used in the traversal with the tags provided.
func (p *Path) LabelContextWithTags(tags []string, via ...interface{}) *Path {
	panic("not implemented yet") // TODO
	//	p.stack = append(p.stack, labelContextMorphism(tags, via...))
	return p
}

// Back returns to a previously tagged place in the path. Any constraints applied after the Tag will remain in effect, but traversal continues from the tagged point instead, not from the end of the chain.
//
// For example:
//  // Will return "bob" iff "bob" is cool
//  StartPath(qs, "bob").Tag("person_tag").Out("status").Is("cool").Back("person_tag")
func (p *Path) Back(tag string) *Path {
	panic("not implemented yet") // TODO
	//	newPath := NewPath(p.qs)
	//	i := len(p.stack) - 1
	//	ctx := &newPath.baseContext
	//	for {
	//		if i < 0 {
	//			return p.Reverse()
	//		}
	//		if p.stack[i].Name == "tag" {
	//			for _, x := range p.stack[i].tags {
	//				if x == tag {
	//					// Found what we're looking for.
	//					p.stack = p.stack[:i+1]
	//					return p.And(newPath)
	//				}
	//			}
	//		}
	//		var revMorphism morphism
	//		revMorphism, ctx = p.stack[i].Reversal(ctx)
	//		newPath.stack = append(newPath.stack, revMorphism)
	//		i--
	//	}
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
	return OptimizeOn(p.nodes, qs).BuildIterator()
}

// Morphism returns the morphism of this path.  The returned value is a
// function that, when given a QuadStore and an existing Iterator, will
// return a new Iterator that yields the subset of values from the existing
// iterator matched by the current Path.
func (p *Path) Morphism() graph.ApplyMorphism {
	panic("not implemented yet") // TODO
	return func(qs graph.QuadStore, it graph.Iterator) graph.Iterator {
		//		ctx := &p.baseContext
		var path PathObj = IntersectNodes{
			p.nodes,
			NodeIteratorBuilder{Builder: func() graph.Iterator { return it.Clone() }},
		}
		path = OptimizeOn(path, qs)
		return path.BuildIterator()
	}
}

func buildViaPath(via ...interface{}) Nodes {
	if len(via) == 0 {
		return AllNodes{}
	} else if len(via) == 1 {
		v := via[0]
		switch p := v.(type) {
		case nil:
			return AllNodes{}
		case *Path:
			return p.nodes
		case Nodes:
			return p
		case string:
			return Fixed{p}
		default:
			panic(fmt.Sprintln("Invalid type passed to buildViaPath.", reflect.TypeOf(v), p))
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
	return Fixed(strings)
}
