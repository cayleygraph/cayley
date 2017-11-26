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

package shape_test

import (
	"testing"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/graphmock"
	. "github.com/cayleygraph/cayley/graph/shape"
	"github.com/cayleygraph/cayley/quad"
	"github.com/stretchr/testify/assert"
)

func intVal(v int) graph.Value {
	return graphmock.IntVal(v)
}

var _ Optimizer = ValLookup(nil)
var _ graph.QuadStore = ValLookup(nil)

type ValLookup map[quad.Value]graph.Value

func (qs ValLookup) OptimizeShape(s Shape) (Shape, bool) {
	return s, false // emulate dumb quad store
}
func (qs ValLookup) ValueOf(v quad.Value) graph.Value {
	return qs[v]
}
func (ValLookup) ApplyDeltas(_ []graph.Delta, _ graph.IgnoreOpts) error {
	panic("not implemented")
}
func (ValLookup) Quad(_ graph.Value) quad.Quad {
	panic("not implemented")
}
func (ValLookup) QuadIterator(_ quad.Direction, _ graph.Value) graph.Iterator {
	panic("not implemented")
}
func (ValLookup) NodesAllIterator() graph.Iterator {
	panic("not implemented")
}
func (ValLookup) QuadsAllIterator() graph.Iterator {
	panic("not implemented")
}
func (ValLookup) NameOf(_ graph.Value) quad.Value {
	panic("not implemented")
}
func (ValLookup) Size() int64 {
	panic("not implemented")
}
func (ValLookup) Horizon() graph.PrimaryKey {
	panic("not implemented")
}
func (ValLookup) FixedIterator() graph.FixedIterator {
	panic("not implemented")
}
func (ValLookup) OptimizeIterator(_ graph.Iterator) (graph.Iterator, bool) {
	panic("not implemented")
}
func (ValLookup) Close() error {
	panic("not implemented")
}
func (ValLookup) QuadDirection(_ graph.Value, _ quad.Direction) graph.Value {
	panic("not implemented")
}
func (ValLookup) Type() string {
	panic("not implemented")
}

func emptySet() Shape {
	return NodesFrom{
		Dir: quad.Predicate,
		Quads: Intersect{Quads{
			{Dir: quad.Object,
				Values: Lookup{quad.IRI("not-existent")},
			},
		}},
	}
}

var optimizeCases = []struct {
	name   string
	from   Shape
	expect Shape
	opt    bool
	qs     ValLookup
}{
	{
		name:   "all",
		from:   AllNodes{},
		opt:    false,
		expect: AllNodes{},
	},
	{
		name: "intersect quads and lookup resolution",
		from: Intersect{
			Quads{
				{Dir: quad.Subject, Values: Lookup{quad.IRI("bob")}},
			},
			Quads{
				{Dir: quad.Object, Values: Lookup{quad.IRI("alice")}},
			},
		},
		opt: true,
		expect: Quads{
			{Dir: quad.Subject, Values: Fixed{intVal(1)}},
			{Dir: quad.Object, Values: Fixed{intVal(2)}},
		},
		qs: ValLookup{
			quad.IRI("bob"):   intVal(1),
			quad.IRI("alice"): intVal(2),
		},
	},
	{
		name: "intersect nodes, remove all, join intersects",
		from: Intersect{
			AllNodes{},
			NodesFrom{Dir: quad.Subject, Quads: Quads{}},
			Intersect{
				Lookup{quad.IRI("alice")},
				Unique{NodesFrom{Dir: quad.Object, Quads: Quads{}}},
			},
		},
		opt: true,
		expect: Intersect{
			Fixed{intVal(1)},
			QuadsAction{Result: quad.Subject},
			Unique{QuadsAction{Result: quad.Object}},
		},
		qs: ValLookup{
			quad.IRI("alice"): intVal(1),
		},
	},
	{
		name: "push Save out of intersect",
		from: Intersect{
			Save{
				Tags: []string{"id"},
				From: NodesFrom{Dir: quad.Subject, Quads: Quads{}},
			},
			Unique{NodesFrom{Dir: quad.Object, Quads: Quads{}}},
		},
		opt: true,
		expect: Save{
			Tags: []string{"id"},
			From: Intersect{
				QuadsAction{Result: quad.Subject},
				Unique{QuadsAction{Result: quad.Object}},
			},
		},
	},
	{
		name: "collapse empty set",
		from: Intersect{Quads{
			{Dir: quad.Subject, Values: Union{
				Unique{emptySet()},
			}},
		}},
		opt:    true,
		expect: Null{},
	},
	{ // remove "all nodes" in intersect, merge Fixed and order them first
		name: "remove all in intersect and reorder",
		from: Intersect{
			AllNodes{},
			Fixed{intVal(1), intVal(2)},
			Save{From: AllNodes{}, Tags: []string{"all"}},
			Fixed{intVal(2)},
		},
		opt: true,
		expect: Save{
			From: Intersect{
				Fixed{intVal(1), intVal(2)},
				Fixed{intVal(2)},
			},
			Tags: []string{"all"},
		},
	},
	{
		name: "remove HasA-LinksTo pairs",
		from: NodesFrom{
			Dir: quad.Subject,
			Quads: Quads{{
				Dir:    quad.Subject,
				Values: Fixed{intVal(1)},
			}},
		},
		opt:    true,
		expect: Fixed{intVal(1)},
	},
	{ // pop fixed tags to the top of the tree
		name: "pop fixed tags",
		from: NodesFrom{Dir: quad.Subject, Quads: Quads{
			QuadFilter{Dir: quad.Predicate, Values: Intersect{
				FixedTags{
					Tags: map[string]graph.Value{"foo": intVal(1)},
					On: NodesFrom{Dir: quad.Subject,
						Quads: Quads{
							QuadFilter{Dir: quad.Object, Values: FixedTags{
								Tags: map[string]graph.Value{"bar": intVal(2)},
								On:   Fixed{intVal(3)},
							}},
						},
					},
				},
			}},
		}},
		opt: true,
		expect: FixedTags{
			Tags: map[string]graph.Value{"foo": intVal(1), "bar": intVal(2)},
			On: NodesFrom{Dir: quad.Subject, Quads: Quads{
				QuadFilter{Dir: quad.Predicate, Values: QuadsAction{
					Result: quad.Subject,
					Filter: map[quad.Direction]graph.Value{quad.Object: intVal(3)},
				}},
			}},
		},
	},
	{ // remove optional empty set from intersect
		name: "remove optional empty set",
		from: Intersect{
			AllNodes{},
			Save{From: AllNodes{}, Tags: []string{"all"}},
			Fixed{intVal(2)},
			Optional{Save{
				From: emptySet(),
				Tags: []string{"name"}},
			},
		},
		opt: true,
		expect: Save{
			From: Fixed{intVal(2)},
			Tags: []string{"all"},
		},
	},
	{ // push fixed node from intersect into nodes.quads
		name: "push fixed into nodes.quads",
		from: Intersect{
			Fixed{intVal(1)},
			NodesFrom{
				Dir: quad.Subject,
				Quads: Quads{
					{Dir: quad.Predicate, Values: Fixed{intVal(2)}},
					{
						Dir: quad.Object,
						Values: NodesFrom{
							Dir: quad.Subject,
							Quads: Quads{
								{Dir: quad.Predicate, Values: Fixed{intVal(2)}},
							},
						},
					},
				},
			},
		},
		opt: true,
		expect: NodesFrom{
			Dir: quad.Subject,
			Quads: Quads{
				{Dir: quad.Subject, Values: Fixed{intVal(1)}},
				{Dir: quad.Predicate, Values: Fixed{intVal(2)}},
				{
					Dir: quad.Object,
					Values: QuadsAction{
						Result: quad.Subject,
						Filter: map[quad.Direction]graph.Value{
							quad.Predicate: intVal(2),
						},
					},
				},
			},
		},
	},
}

func TestOptimize(t *testing.T) {
	for _, c := range optimizeCases {
		t.Run(c.name, func(t *testing.T) {
			qs := c.qs
			got, opt := Optimize(c.from, qs)
			assert.Equal(t, c.expect, got)
			assert.Equal(t, c.opt, opt)
		})
	}
}
