// Copyright 2017 The Cayley Authors. All rights reserved.
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

package gizmo

// Adds special traversal functions to JS Gizmo objects. Most of these just build the chain of objects, and won't often need the session.

import (
	"fmt"

	"github.com/dop251/goja"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/graph/path"
	"github.com/cayleygraph/cayley/graph/shape"
)

// pathObject is a Path object in Gizmo.
//
// Both `.Morphism()` and `.Vertex()` create path objects, which provide the following traversal methods.
// Note that `.Vertex()` returns a query object, which is a subclass of path object.
//
// For these examples, suppose we have the following graph:
//
//	+-------+                        +------+
//	| alice |-----                 ->| fred |<--
//	+-------+     \---->+-------+-/  +------+   \-+-------+
//	              ----->| #bob# |       |         |*emily*|
//	+---------+--/  --->+-------+       |         +-------+
//	| charlie |    /                    v
//	+---------+   /                  +--------+
//	  \---    +--------+             |*#greg#*|
//	      \-->| #dani# |------------>+--------+
//	          +--------+
//
// Where every link is a `<follows>` relationship, and the nodes with an extra `#` in the name have an extra `<status>` link. As in,
//
//	<dani> -- <status> --> "cool_person"
//
// Perhaps these are the influencers in our community. So too are extra `*`s in the name -- these are our smart people,
// according to the `<smart_graph>` label, eg, the quad:
//
//	<greg> <status> "smart_person" <smart_graph> .
type pathObject struct {
	s      *Session
	finals bool
	path   *path.Path
}

func (p *pathObject) new(np *path.Path) *pathObject {
	return &pathObject{
		s:      p.s,
		finals: p.finals,
		path:   np,
	}
}

func (p *pathObject) newVal(np *path.Path) goja.Value {
	return p.s.vm.ToValue(p.new(np))
}
func (p *pathObject) clonePath() *path.Path {
	np := p.path.Clone()
	// most likely path will be continued, so we'll put non-capped stack slice
	// into new path object instead of preserving it in an old one
	p.path, np = np, p.path
	return np
}
func (p *pathObject) buildIteratorTree() graph.Iterator {
	if p.path == nil {
		return iterator.NewNull()
	}
	return p.path.BuildIteratorOn(p.s.qs)
}

// Filter all paths to ones which, at this point, are on the given node.
// Signature: (node, [node..])
//
// Arguments:
//
// * `node`: A string for a node. Can be repeated or a list of strings.
//
// Example:
//	// javascript
//	// Starting from all nodes in the graph, find the paths that follow bob.
//	// Results in three paths for bob (from alice, charlie and dani).all()
//	g.V().out("<follows>").is("<bob>").all()
func (p *pathObject) Is(call goja.FunctionCall) goja.Value {
	args, err := toQuadValues(exportArgs(call.Arguments))
	if err != nil {
		return throwErr(p.s.vm, err)
	}
	np := p.clonePath().Is(args...)
	return p.newVal(np)
}
func (p *pathObject) inout(call goja.FunctionCall, in bool) goja.Value {
	preds, tags, ok := toViaData(exportArgs(call.Arguments))
	if !ok {
		return throwErr(p.s.vm, errNoVia)
	}
	np := p.clonePath()
	if in {
		np = np.InWithTags(tags, preds...)
	} else {
		np = np.OutWithTags(tags, preds...)
	}
	return p.newVal(np)
}

// In is inverse of Out.
// Starting with the nodes in `path` on the object, follow the quads with predicates defined by `predicatePath` to their subjects.
// Signature: ([predicatePath], [tags])
//
// Arguments:
//
// * `predicatePath` (Optional): One of:
//   * null or undefined: All predicates pointing into this node
//   * a string: The predicate name to follow into this node
//   * a list of strings: The predicates to follow into this node
//   * a query path object: The target of which is a set of predicates to follow.
// * `tags` (Optional): One of:
//   * null or undefined: No tags
//   * a string: A single tag to add the predicate used to the output set.
//   * a list of strings: Multiple tags to use as keys to save the predicate used to the output set.
//
// Example:
//
//	// javascript
//	// Find the cool people, bob, dani and greg
//	g.V("cool_person").in("<status>").all()
//	// Find who follows bob, in this case, alice, charlie, and dani
//	g.V("<bob>").in("<follows>").all()
//	// Find who follows the people emily follows, namely, bob and emily
//	g.V("<emily>").out("<follows>").in("<follows>").all()
func (p *pathObject) In(call goja.FunctionCall) goja.Value {
	return p.inout(call, true)
}

// Out is the work-a-day way to get between nodes, in the forward direction.
// Starting with the nodes in `path` on the subject, follow the quads with predicates defined by `predicatePath` to their objects.
// Signature: ([predicatePath], [tags])
//
// Arguments:
//
// * `predicatePath` (Optional): One of:
//   * null or undefined: All predicates pointing out from this node
//   * a string: The predicate name to follow out from this node
//   * a list of strings: The predicates to follow out from this node
//   * a query path object: The target of which is a set of predicates to follow.
// * `tags` (Optional): One of:
//   * null or undefined: No tags
//   * a string: A single tag to add the predicate used to the output set.
//   * a list of strings: Multiple tags to use as keys to save the predicate used to the output set.
//
// Example:
//
//	// javascript
//	// The working set of this is bob and dani
//	g.V("<charlie>").out("<follows>").all()
//	// The working set of this is fred, as alice follows bob and bob follows fred.
//	g.V("<alice>").out("<follows>").out("<follows>").all()
//	// Finds all things dani points at. Result is bob, greg and cool_person
//	g.V("<dani>").out().all()
//	// Finds all things dani points at on the status linkage.
//	// Result is bob, greg and cool_person
//	g.V("<dani>").out(["<follows>", "<status>"]).all()
//	// Finds all things dani points at on the status linkage, given from a separate query path.
//	// Result is {"id": "cool_person", "pred": "<status>"}
//	g.V("<dani>").out(g.V("<status>"), "pred").all()
func (p *pathObject) Out(call goja.FunctionCall) goja.Value {
	return p.inout(call, false)
}

// Both follow the predicate in either direction. Same as Out or In.
// Signature: ([predicatePath], [tags])
//
// Example:
//	// javascript
//	// Find all followers/followees of fred. Returns bob, emily and greg
//	g.V("<fred>").both("<follows>").all()
func (p *pathObject) Both(call goja.FunctionCall) goja.Value {
	preds, tags, ok := toViaData(exportArgs(call.Arguments))
	if !ok {
		return throwErr(p.s.vm, errNoVia)
	}
	np := p.clonePath().BothWithTags(tags, preds...)
	return p.newVal(np)
}
func (p *pathObject) follow(ep *pathObject, rev bool) *pathObject {
	if ep == nil {
		return p
	}
	np := p.clonePath()
	if rev {
		np = np.FollowReverse(ep.path)
	} else {
		np = np.Follow(ep.path)
	}
	return p.new(np)
}

// Follow is the way to use a path prepared with Morphism. Applies the path chain on the morphism object to the current path.
//
// Starts as if at the g.M() and follows through the morphism path.
//
// Example:
// 	// javascript:
//	var friendOfFriend = g.Morphism().Out("<follows>").Out("<follows>")
//	// Returns the followed people of who charlie follows -- a simplistic "friend of my friend"
//	// and whether or not they have a "cool" status. Potential for recommending followers abounds.
//	// Returns bob and greg
//	g.V("<charlie>").follow(friendOfFriend).has("<status>", "cool_person").all()
func (p *pathObject) Follow(path *pathObject) *pathObject {
	return p.follow(path, false)
}

// FollowR is the same as Follow but follows the chain in the reverse direction. Flips "In" and "Out" where appropriate,
// the net result being a virtual predicate followed in the reverse direction.
//
// Starts at the end of the morphism and follows it backwards (with appropriate flipped directions) to the g.M() location.
//
// Example:
// 	// javascript:
//	var friendOfFriend = g.Morphism().Out("<follows>").Out("<follows>")
//	// Returns the third-tier of influencers -- people who follow people who follow the cool people.
//	// Returns charlie (from bob), charlie (from greg), bob and emily
//	g.V().has("<status>", "cool_person").followR(friendOfFriend).all()
func (p *pathObject) FollowR(path *pathObject) *pathObject {
	return p.follow(path, true)
}

// FollowRecursive is the same as Follow but follows the chain recursively.
//
// Starts as if at the g.M() and follows through the morphism path multiple times, returning all nodes encountered.
//
// Example:
// 	// javascript:
//	var friend = g.Morphism().out("<follows>")
//	// Returns all people in Charlie's network.
//	// Returns bob and dani (from charlie), fred (from bob) and greg (from dani).
//	g.V("<charlie>").followRecursive(friend).all()
func (p *pathObject) FollowRecursive(call goja.FunctionCall) goja.Value {
	preds, maxDepth, tags, ok := toViaDepthData(exportArgs(call.Arguments))
	if !ok || len(preds) == 0 {
		return throwErr(p.s.vm, errNoVia)
	} else if len(preds) != 1 {
		return throwErr(p.s.vm, fmt.Errorf("expected one predicate or path for recursive follow"))
	}
	np := p.clonePath()
	np = np.FollowRecursive(preds[0], maxDepth, tags)
	return p.newVal(np)
}

// And is an alias for Intersect.
func (p *pathObject) And(path *pathObject) *pathObject {
	return p.Intersect(path)
}

// Intersect filters all paths by the result of another query path.
//
// This is essentially a join where, at the stage of each path, a node is shared.
// Example:
// 	// javascript
//	var cFollows = g.V("<charlie>").Out("<follows>")
//	var dFollows = g.V("<dani>").Out("<follows>")
//	// People followed by both charlie (bob and dani) and dani (bob and greg) -- returns bob.
//	cFollows.Intersect(dFollows).All()
//	// Equivalently, g.V("<charlie>").Out("<follows>").And(g.V("<dani>").Out("<follows>")).All()
func (p *pathObject) Intersect(path *pathObject) *pathObject {
	if path == nil {
		return p
	}
	np := p.clonePath().And(path.path)
	return p.new(np)
}

// Union returns the combined paths of the two queries.
//
// Notice that it's per-path, not per-node. Once again, if multiple paths reach the same destination,
// they might have had different ways of getting there (and different tags).
// See also: `path.Tag()`
//
// Example:
// 	// javascript
//	var cFollows = g.V("<charlie>").Out("<follows>")
//	var dFollows = g.V("<dani>").Out("<follows>")
//	// People followed by both charlie (bob and dani) and dani (bob and greg) -- returns bob (from charlie), dani, bob (from dani), and greg.
//	cFollows.Union(dFollows).All()
func (p *pathObject) Union(path *pathObject) *pathObject {
	if path == nil {
		return p
	}
	np := p.clonePath().Or(path.path)
	return p.new(np)
}

// Or is an alias for Union.
func (p *pathObject) Or(path *pathObject) *pathObject {
	return p.Union(path)
}

// Back returns current path to a set of nodes on a given tag, preserving all constraints.
//
// If still valid, a path will now consider their vertex to be the same one as the previously tagged one,
// with the added constraint that it was valid all the way here.
// Useful for traversing back in queries and taking another route for things that have matched so far.
//
// Arguments:
//
// * `tag`: A previous tag in the query to jump back to.
//
// Example:
// 	// javascript
//	// Start from all nodes, save them into start, follow any status links,
//	// jump back to the starting node, and find who follows them. Return the result.
//	// Results are:
//	//   {"id": "<alice>", "start": "<bob>"},
//	//   {"id": "<charlie>", "start": "<bob>"},
//	//   {"id": "<charlie>", "start": "<dani>"},
//	//   {"id": "<dani>", "start": "<bob>"},
//	//   {"id": "<dani>", "start": "<greg>"},
//	//   {"id": "<dani>", "start": "<greg>"},
//	//   {"id": "<fred>", "start": "<greg>"},
//	//   {"id": "<fred>", "start": "<greg>"}
//	g.V().tag("start").out("<status>").back("start").in("<follows>").all()
func (p *pathObject) Back(tag string) *pathObject {
	np := p.clonePath().Back(tag)
	return p.new(np)
}

// Tag saves a list of nodes to a given tag.
//
// In order to save your work or learn more about how a path got to the end, we have tags.
// The simplest thing to do is to add a tag anywhere you'd like to put each node in the result set.
//
// Arguments:
//
// * `tag`: A string or list of strings to act as a result key. The value for tag was the vertex the path was on at the time it reached "Tag"
// Example:
// 	// javascript
//	// Start from all nodes, save them into start, follow any status links, and return the result.
//	// Results are:
//	//   {"id": "cool_person", "start": "<bob>"},
//	//   {"id": "cool_person", "start": "<dani>"},
//	//   {"id": "cool_person", "start": "<greg>"},
//	//   {"id": "smart_person", "start": "<emily>"},
//	//   {"id": "smart_person", "start": "<greg>"}
//	g.V().tag("start").out("<status>").All()
func (p *pathObject) Tag(tags ...string) *pathObject {
	np := p.clonePath().Tag(tags...)
	return p.new(np)
}

// As is an alias for Tag.
func (p *pathObject) As(tags ...string) *pathObject {
	return p.Tag(tags...)
}

// Has filters all paths which are, at this point, on the subject for the given predicate and object,
// but do not follow the path, merely filter the possible paths.
//
// Usually useful for starting with all nodes, or limiting to a subset depending on some predicate/value pair.
//
// Signature: (predicate, object)
//
// Arguments:
//
// * `predicate`: A string for a predicate node.
// * `object`: A string for a object node or a set of filters to find it.
//
// Example:
// 	// javascript
//	// Start from all nodes that follow bob -- results in alice, charlie and dani
//	g.V().has("<follows>", "<bob>").all()
//	// People charlie follows who then follow fred. Results in bob.
//	g.V("<charlie>").Out("<follows>").has("<follows>", "<fred>").all()
//	// People with friends who have names sorting lower then "f".
//	g.V().has("<follows>", gt("<f>")).all()
func (p *pathObject) Has(call goja.FunctionCall) goja.Value {
	return p.has(call, false)
}

// HasR is the same as Has, but sets constraint in reverse direction.
func (p *pathObject) HasR(call goja.FunctionCall) goja.Value {
	return p.has(call, true)
}
func (p *pathObject) has(call goja.FunctionCall, rev bool) goja.Value {
	args := exportArgs(call.Arguments)
	if len(args) == 0 {
		return throwErr(p.s.vm, errArgCount{Got: len(args)})
	}
	via := args[0]
	args = args[1:]
	if vp, ok := via.(*pathObject); ok {
		via = vp.path
	} else {
		var err error
		via, err = toQuadValue(via)
		if err != nil {
			return throwErr(p.s.vm, err)
		}
	}
	if len(args) > 0 {
		var filt []shape.ValueFilter
	loop:
		for _, a := range args {
			switch a := a.(type) {
			case valFilter:
				filt = append(filt, a.f)
			case []valFilter:
				for _, s := range a {
					filt = append(filt, s.f)
				}
			default:
				filt = nil
				// failed to collect all argument as filters - switch back to nodes-only mode
				break loop
			}
		}
		if len(filt) > 0 {
			np := p.clonePath()
			np = np.HasFilter(via, rev, filt...)
			return p.newVal(np)
		}
	}
	qv, err := toQuadValues(args)
	if err != nil {
		return throwErr(p.s.vm, err)
	}
	np := p.clonePath()
	if rev {
		np = np.HasReverse(via, qv...)
	} else {
		np = np.Has(via, qv...)
	}
	return p.newVal(np)
}
func (p *pathObject) save(call goja.FunctionCall, rev, opt bool) goja.Value {
	args := exportArgs(call.Arguments)
	if len(args) > 2 || len(args) == 0 {
		return throwErr(p.s.vm, errArgCount{Got: len(args)})
	}
	vtag := args[0]
	if len(args) == 2 {
		vtag = args[1]
	}
	tag, ok := vtag.(string)
	if !ok {
		return throwErr(p.s.vm, fmt.Errorf("expected string, got: %T", vtag))
	}
	via := args[0]
	if vp, ok := via.(*pathObject); ok {
		via = vp.path
	} else {
		var err error
		via, err = toQuadValue(via)
		if err != nil {
			return throwErr(p.s.vm, err)
		}
	}
	np := p.clonePath()
	if opt {
		if rev {
			np = np.SaveOptionalReverse(via, tag)
		} else {
			np = np.SaveOptional(via, tag)
		}
	} else {
		if rev {
			np = np.SaveReverse(via, tag)
		} else {
			np = np.Save(via, tag)
		}
	}
	return p.newVal(np)
}

// Save saves the object of all quads with predicate into tag, without traversal.
// Signature: (predicate, tag)
//
// Arguments:
//
// * `predicate`: A string for a predicate node.
// * `tag`: A string for a tag key to store the object node.
//
// Example:
// 	// javascript
//	// Start from dani and bob and save who they follow into "target"
//	// Returns:
//	//   {"id" : "<bob>", "target": "<fred>" },
//	//   {"id" : "<dani>", "target": "<bob>" },
//	//   {"id" : "<dani>", "target": "<greg>" }
//	g.V("<dani>", "<bob>").Save("<follows>", "target").All()
func (p *pathObject) Save(call goja.FunctionCall) goja.Value {
	return p.save(call, false, false)
}

// SaveR is the same as Save, but tags values via reverse predicate.
func (p *pathObject) SaveR(call goja.FunctionCall) goja.Value {
	return p.save(call, true, false)
}

// SaveOpt is the same as Save, but returns empty tags if predicate does not exists.
func (p *pathObject) SaveOpt(call goja.FunctionCall) goja.Value {
	return p.save(call, false, true)
}

// SaveOptR is the same as SaveOpt, but tags values via reverse predicate.
func (p *pathObject) SaveOptR(call goja.FunctionCall) goja.Value {
	return p.save(call, true, true)
}

// Except removes all paths which match query from current path.
//
// In a set-theoretic sense, this is (A - B). While `g.V().Except(path)` to achieve `U - B = !B` is supported, it's often very slow.
// Example:
// 	// javascript
//	var cFollows = g.V("<charlie>").Out("<follows>")
//	var dFollows = g.V("<dani>").Out("<follows>")
//	// People followed by both charlie (bob and dani) and dani (bob and greg) -- returns bob.
//	cFollows.Except(dFollows).All()   // The set (dani) -- what charlie follows that dani does not also follow.
//	// Equivalently, g.V("<charlie>").Out("<follows>").Except(g.V("<dani>").Out("<follows>")).All()
func (p *pathObject) Except(path *pathObject) *pathObject {
	if path == nil {
		return p
	}
	np := p.clonePath().Except(path.path)
	return p.new(np)
}

// Unique removes duplicate values from the path.
func (p *pathObject) Unique() *pathObject {
	np := p.clonePath().Unique()
	return p.new(np)
}

// Difference is an alias for Except.
func (p *pathObject) Difference(path *pathObject) *pathObject {
	return p.Except(path)
}

// Labels gets the list of inbound and outbound quad labels
func (p *pathObject) Labels() *pathObject {
	np := p.clonePath().Labels()
	return p.new(np)
}

// InPredicates gets the list of predicates that are pointing in to a node.
//
// Example:
// 	// javascript
//	// bob only has "<follows>" predicates pointing inward
//	// returns "<follows>"
//	g.V("<bob>").InPredicates().All()
func (p *pathObject) InPredicates() *pathObject {
	np := p.clonePath().InPredicates()
	return p.new(np)
}

// OutPredicates gets the list of predicates that are pointing out from a node.
//
// Example:
// 	// javascript
//	// bob has "<follows>" and "<status>" edges pointing outwards
//	// returns "<follows>", "<status>"
//	g.V("<bob>").OutPredicates().All()
func (p *pathObject) OutPredicates() *pathObject {
	np := p.clonePath().OutPredicates()
	return p.new(np)
}

// SaveInPredicates tags the list of predicates that are pointing in to a node.
//
// Example:
// 	// javascript
//	// bob only has "<follows>" predicates pointing inward
//	// returns {"id":"<bob>", "pred":"<follows>"}
//	g.V("<bob>").SaveInPredicates("pred").All()
func (p *pathObject) SaveInPredicates(tag string) *pathObject {
	np := p.clonePath().SavePredicates(true, tag)
	return p.new(np)
}

// SaveOutPredicates tags the list of predicates that are pointing out from a node.
//
// Example:
// 	// javascript
//	// bob has "<follows>" and "<status>" edges pointing outwards
//	// returns {"id":"<bob>", "pred":"<follows>"}
//	g.V("<bob>").SaveInPredicates("pred").All()
func (p *pathObject) SaveOutPredicates(tag string) *pathObject {
	np := p.clonePath().SavePredicates(false, tag)
	return p.new(np)
}

// LabelContext sets (or removes) the subgraph context to consider in the following traversals.
// Affects all In(), Out(), and Both() calls that follow it. The default LabelContext is null (all subgraphs).
// Signature: ([labelPath], [tags])
//
// Arguments:
//
// * `predicatePath` (Optional): One of:
//   * null or undefined: In future traversals, consider all edges, regardless of subgraph.
//   * a string: The name of the subgraph to restrict traversals to.
//   * a list of strings: A set of subgraphs to restrict traversals to.
//   * a query path object: The target of which is a set of subgraphs.
// * `tags` (Optional): One of:
//   * null or undefined: No tags
//   * a string: A single tag to add the last traversed label to the output set.
//   * a list of strings: Multiple tags to use as keys to save the label used to the output set.
//
// Example:
// 	// javascript
//	// Find the status of people Dani follows
//	g.V("<dani>").out("<follows>").out("<status>").all()
//	// Find only the statuses provided by the smart_graph
//	g.V("<dani>").out("<follows>").labelContext("<smart_graph>").out("<status>").all()
//	// Find all people followed by people with statuses in the smart_graph.
//	g.V().labelContext("<smart_graph>").in("<status>").labelContext(null).in("<follows>").all()
func (p *pathObject) LabelContext(call goja.FunctionCall) goja.Value {
	labels, tags, ok := toViaData(exportArgs(call.Arguments))
	if !ok {
		return throwErr(p.s.vm, errNoVia)
	}
	np := p.clonePath().LabelContextWithTags(tags, labels...)
	return p.newVal(np)
}

// Filter applies constraints to a set of nodes. Can be used to filter values by range or match strings.
func (p *pathObject) Filter(args ...valFilter) (*pathObject, error) {
	if len(args) == 0 {
		return nil, errArgCount{Got: len(args)}
	}
	filt := make([]shape.ValueFilter, 0, len(args))
	for _, f := range args {
		filt = append(filt, f.f)
	}
	np := p.clonePath().Filters(filt...)
	return p.new(np), nil
}

// Limit limits a number of nodes for current path.
//
// Arguments:
//
// * `limit`: A number of nodes to limit results to.
//
// Example:
// 	// javascript
//	// Start from all nodes that follow bob, and limit them to 2 nodes -- results in alice and charlie
//	g.V().has("<follows>", "<bob>").limit(2).all()
func (p *pathObject) Limit(limit int) *pathObject {
	np := p.clonePath().Limit(int64(limit))
	return p.new(np)
}

// Skip skips a number of nodes for current path.
//
// Arguments:
//
// * `offset`: A number of nodes to skip.
//
// Example:
//	// javascript
//	// Start from all nodes that follow bob, and skip 2 nodes -- results in dani
//	g.V().has("<follows>", "<bob>").skip(2).all()
func (p *pathObject) Skip(offset int) *pathObject {
	np := p.clonePath().Skip(int64(offset))
	return p.new(np)
}

// Backwards compatibility
func (p *pathObject) CapitalizedIs(call goja.FunctionCall) goja.Value {
	return p.Is(call)
}
func (p *pathObject) CapitalizedIn(call goja.FunctionCall) goja.Value {
	return p.In(call)
}
func (p *pathObject) CapitalizedOut(call goja.FunctionCall) goja.Value {
	return p.Out(call)
}
func (p *pathObject) CapitalizedBoth(call goja.FunctionCall) goja.Value {
	return p.Both(call)
}
func (p *pathObject) CapitalizedFollow(path *pathObject) *pathObject {
	return p.Follow(path)
}
func (p *pathObject) CapitalizedFollowR(path *pathObject) *pathObject {
	return p.FollowR(path)
}
func (p *pathObject) CapitalizedFollowRecursive(call goja.FunctionCall) goja.Value {
	return p.FollowRecursive(call)
}
func (p *pathObject) CapitalizedAnd(path *pathObject) *pathObject {
	return p.And(path)
}
func (p *pathObject) CapitalizedIntersect(path *pathObject) *pathObject {
	return p.Intersect(path)
}
func (p *pathObject) CapitalizedUnion(path *pathObject) *pathObject {
	return p.Union(path)
}
func (p *pathObject) CapitalizedOr(path *pathObject) *pathObject {
	return p.Or(path)
}
func (p *pathObject) CapitalizedBack(tag string) *pathObject {
	return p.Back(tag)
}
func (p *pathObject) CapitalizedTag(tags ...string) *pathObject {
	return p.Tag(tags...)
}
func (p *pathObject) CapitalizedAs(tags ...string) *pathObject {
	return p.As(tags...)
}
func (p *pathObject) CapitalizedHas(call goja.FunctionCall) goja.Value {
	return p.Has(call)
}
func (p *pathObject) CapitalizedHasR(call goja.FunctionCall) goja.Value {
	return p.HasR(call)
}
func (p *pathObject) CapitalizedSave(call goja.FunctionCall) goja.Value {
	return p.Save(call)
}
func (p *pathObject) CapitalizedSaveR(call goja.FunctionCall) goja.Value {
	return p.SaveR(call)
}
func (p *pathObject) CapitalizedSaveOpt(call goja.FunctionCall) goja.Value {
	return p.SaveOpt(call)
}
func (p *pathObject) CapitalizedSaveOptR(call goja.FunctionCall) goja.Value {
	return p.SaveOptR(call)
}
func (p *pathObject) CapitalizedExcept(path *pathObject) *pathObject {
	return p.Except(path)
}
func (p *pathObject) CapitalizedUnique() *pathObject {
	return p.Unique()
}
func (p *pathObject) CapitalizedDifference(path *pathObject) *pathObject {
	return p.Difference(path)
}
func (p *pathObject) CapitalizedLabels() *pathObject {
	return p.Labels()
}
func (p *pathObject) CapitalizedInPredicates() *pathObject {
	return p.InPredicates()
}
func (p *pathObject) CapitalizedOutPredicates() *pathObject {
	return p.OutPredicates()
}
func (p *pathObject) CapitalizedSaveInPredicates(tag string) *pathObject {
	return p.SaveInPredicates(tag)
}
func (p *pathObject) CapitalizedSaveOutPredicates(tag string) *pathObject {
	return p.SaveOutPredicates(tag)
}
func (p *pathObject) CapitalizedLabelContext(call goja.FunctionCall) goja.Value {
	return p.LabelContext(call)
}
func (p *pathObject) CapitalizedFilter(args ...valFilter) (*pathObject, error) {
	return p.Filter(args...)
}
func (p *pathObject) CapitalizedLimit(limit int) *pathObject {
	return p.Limit(limit)
}
func (p *pathObject) CapitalizedSkip(offset int) *pathObject {
	return p.Skip(offset)
}
