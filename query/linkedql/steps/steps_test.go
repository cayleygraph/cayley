package steps

import (
	"context"
	"testing"

	"github.com/cayleygraph/cayley/graph/memstore"
	"github.com/cayleygraph/cayley/query/linkedql"
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/voc"
	"github.com/cayleygraph/quad/voc/xsd"
	"github.com/stretchr/testify/require"
)

var alice = quad.IRI("http://example.org/alice")
var bob = quad.IRI("http://example.org/bob")
var dan = quad.IRI("http://example.org/dan")
var likes = quad.IRI("http://example.org/likes")
var likesName = quad.IRI("http://example.org/likesName")
var name = quad.IRI("http://example.org/name")
var person = quad.IRI("http://example.org/person")
var liker = quad.IRI("http://example.org/liker")
var liked = quad.IRI("http://example.org/liked")

var singleQuadData = []quad.Quad{
	quad.Quad{alice, likes, bob, nil},
}

var testCases = []struct {
	name    string
	data    []quad.Quad
	query   linkedql.IteratorStep
	results []interface{}
}{
	{
		name:  "All Entities",
		data:  singleQuadData,
		query: &Entities{Identifiers: []linkedql.EntityIdentifier{linkedql.EntityIdentifierString(alice)}},
		results: []interface{}{
			map[string]string{"@id": string(alice)},
		},
	},
	{
		name:  "All Vertices",
		data:  singleQuadData,
		query: &Vertex{Values: nil},
		results: []interface{}{
			map[string]string{"@id": string(alice)},
			map[string]string{"@id": string(likes)},
			map[string]string{"@id": string(bob)},
		},
	},
	{
		name: "Select",
		data: singleQuadData,
		query: &Select{
			From: &As{
				From: &Visit{
					From: &As{
						From: &Vertex{},
						Name: string(liker),
					},
					Properties: linkedql.PropertyPath{&Vertex{Values: []quad.Value{likes}}},
				},
				Name: string(liked),
			},
		},
		results: []interface{}{
			map[string]interface{}{
				string(liker): map[string]string{"@id": string(alice)},
				string(liked): map[string]string{"@id": string(bob)},
			},
		},
	},
	{
		name: "Select with tags",
		data: singleQuadData,
		query: &Select{
			Tags: []string{string(liker)},
			From: &As{
				From: &Visit{
					From: &As{
						From: &Vertex{},
						Name: string(liker),
					},
					Properties: linkedql.PropertyPath{&Vertex{Values: []quad.Value{likes}}},
				},
				Name: "liked",
			},
		},
		results: []interface{}{
			map[string]interface{}{
				string(liker): map[string]string{"@id": string(alice)},
			},
		},
	},
	{
		name: "Back",
		data: singleQuadData,
		query: &Back{
			From: &Visit{
				From: &Vertex{
					Values: []quad.Value{alice},
				},
				Properties: linkedql.PropertyPath{&Vertex{
					Values: []quad.Value{
						likes,
					},
				}},
			},
		},
		results: []interface{}{
			map[string]string{"@id": string(alice)},
		},
	},
	{
		name: "Both",
		data: []quad.Quad{
			quad.Quad{dan, likes, bob, nil},
			quad.Quad{bob, likes, alice, nil},
		},
		query: &Both{
			From: &Vertex{
				Values: []quad.Value{bob},
			},
			Properties: linkedql.PropertyPath{&Vertex{Values: []quad.Value{likes}}},
		},
		results: []interface{}{
			map[string]string{"@id": string(dan)},
			map[string]string{"@id": string(alice)},
		},
	},
	{
		name: "Count",
		data: singleQuadData,
		query: &Count{
			From: &Vertex{Values: []quad.Value{}},
		},
		results: []interface{}{
			map[string]string{"@value": "4", "@type": xsd.Integer},
		},
	},
	{
		name: "Difference",
		data: singleQuadData,
		query: &Difference{
			From: &Vertex{
				Values: []quad.Value{alice, likes},
			},
			Steps: []linkedql.PathStep{
				&Vertex{
					Values: []quad.Value{likes},
				},
			},
		},
		results: []interface{}{
			map[string]string{"@id": string(alice)},
		},
	},
	{
		name: "Filter RegExp",
		data: []quad.Quad{
			{Subject: alice, Predicate: name, Object: quad.String("Alice"), Label: nil},
		},
		query: &Filter{
			From:   &Vertex{Values: []quad.Value{}},
			Filter: &RegExp{Pattern: "A"},
		},
		results: []interface{}{
			"Alice",
		},
	},
	{
		name: "Filter Like",
		data: []quad.Quad{
			{Subject: alice, Predicate: name, Object: quad.String("Alice"), Label: nil},
		},
		query: &Filter{
			From:   &Vertex{Values: []quad.Value{}},
			Filter: &Like{Pattern: "%ali%"},
		},
		results: []interface{}{
			map[string]string{"@id": string(alice)},
		},
	},
	{
		name: "Filter LessThan",
		data: []quad.Quad{
			{Subject: alice, Predicate: name, Object: quad.Int(0), Label: nil},
			{Subject: alice, Predicate: name, Object: quad.Int(1), Label: nil},
		},
		query: &LessThan{
			From:  &Vertex{Values: []quad.Value{}},
			Value: quad.Int(1),
		},
		results: []interface{}{
			map[string]string{"@value": "0", "@type": "xsd:integer"},
		},
	},
	{
		name: "Filter GreaterThan",
		data: []quad.Quad{
			{Subject: alice, Predicate: name, Object: quad.Int(0), Label: nil},
			{Subject: alice, Predicate: name, Object: quad.Int(1), Label: nil},
		},
		query: &GreaterThan{
			From:  &Vertex{Values: []quad.Value{}},
			Value: quad.Int(0),
		},
		results: []interface{}{
			map[string]string{"@value": "1", "@type": "xsd:integer"},
		},
	},
	{
		name: "Filter LessThanEquals",
		data: []quad.Quad{
			{Subject: alice, Predicate: name, Object: quad.Int(-1), Label: nil},
			{Subject: alice, Predicate: name, Object: quad.Int(0), Label: nil},
			{Subject: alice, Predicate: name, Object: quad.Int(1), Label: nil},
		},
		query: &LessThanEquals{
			From:  &Vertex{Values: []quad.Value{}},
			Value: quad.Int(0),
		},
		results: []interface{}{
			map[string]string{"@value": "-1", "@type": "xsd:integer"},
			map[string]string{"@value": "0", "@type": "xsd:integer"},
		},
	},
	{
		name: "Filter GreaterThanEquals",
		data: []quad.Quad{
			{Subject: alice, Predicate: name, Object: quad.Int(0), Label: nil},
			{Subject: alice, Predicate: name, Object: quad.Int(1), Label: nil},
			{Subject: alice, Predicate: name, Object: quad.Int(2), Label: nil},
		},
		query: &GreaterThanEquals{
			From:  &Vertex{Values: []quad.Value{}},
			Value: quad.Int(1),
		},
		results: []interface{}{
			map[string]string{"@value": "1", "@type": "xsd:integer"},
			map[string]string{"@value": "2", "@type": "xsd:integer"},
		},
	},
	{
		name: "Has",
		data: singleQuadData,
		query: &Has{
			From: &Vertex{
				Values: []quad.Value{},
			},
			Property: linkedql.PropertyPath{&Vertex{
				Values: []quad.Value{likes},
			}},
			Values: []quad.Value{bob},
		},
		results: []interface{}{
			map[string]string{"@id": string(alice)},
		},
	},
	{
		name: "HasReverse",
		data: singleQuadData,
		query: &HasReverse{
			From: &Vertex{
				Values: []quad.Value{},
			},
			Property: linkedql.PropertyPath{&Vertex{
				Values: []quad.Value{likes},
			}},
			Values: []quad.Value{alice},
		},
		results: []interface{}{
			map[string]string{"@id": string(bob)},
		},
	},
	{
		name: "ViewReverse",
		data: singleQuadData,
		query: &VisitReverse{
			From:       &Vertex{Values: []quad.Value{}},
			Properties: linkedql.PropertyPath{&Vertex{Values: []quad.Value{likes}}},
		},
		results: []interface{}{
			map[string]string{"@id": string(alice)},
		},
	},
	{
		name: "PropertyNames",
		data: singleQuadData,
		query: &PropertyNames{
			From: &Vertex{Values: []quad.Value{}},
		},
		results: []interface{}{
			map[string]string{"@id": string(likes)},
		},
	},
	{
		name: "Intersect",
		data: []quad.Quad{
			quad.Quad{bob, likes, alice, nil},
			quad.Quad{dan, likes, alice, nil},
		},
		query: &Intersect{
			From: &Visit{
				From: &Vertex{Values: []quad.Value{bob}},
				Properties: linkedql.PropertyPath{&Vertex{
					Values: []quad.Value{likes},
				}},
			},
			Steps: []linkedql.PathStep{
				&Visit{
					From:       &Vertex{Values: []quad.Value{bob}},
					Properties: linkedql.PropertyPath{&Vertex{Values: []quad.Value{likes}}},
				},
			},
		},
		results: []interface{}{
			map[string]string{"@id": string(alice)},
		},
	},
	{
		name: "Is",
		data: singleQuadData,
		query: &Is{
			Values: []quad.Value{bob},
			From: &Visit{
				From: &Vertex{Values: []quad.Value{alice}},
				Properties: linkedql.PropertyPath{&Vertex{
					Values: []quad.Value{likes},
				}},
			},
		},
		results: []interface{}{
			map[string]string{"@id": string(bob)},
		},
	},
	{
		name: "Limit",
		data: singleQuadData,
		query: &Limit{
			Limit: 2,
			From: &Vertex{
				Values: []quad.Value{},
			},
		},
		results: []interface{}{
			map[string]string{"@id": string(alice)},
			map[string]string{"@id": string(likes)},
		},
	},
	{
		name: "View",
		data: singleQuadData,
		query: &Visit{
			From:       &Vertex{Values: []quad.Value{}},
			Properties: linkedql.PropertyPath{&Vertex{Values: []quad.Value{likes}}},
		},
		results: []interface{}{
			map[string]string{"@id": string(bob)},
		},
	},
	{
		name: "PropertyNames",
		data: singleQuadData,
		query: &PropertyNames{
			From: &Vertex{Values: []quad.Value{}},
		},
		results: []interface{}{
			map[string]string{"@id": string(likes)},
		},
	},
	{
		name: "Properties",
		data: singleQuadData,
		query: &Select{
			From: &Properties{
				From:  &Vertex{Values: []quad.Value{}},
				Names: []quad.IRI{likes},
			},
		},
		results: []interface{}{map[string]interface{}{string(likes): map[string]string{"@id": string(bob)}}},
	},
	{
		name: "ReversePropertyNamesAs",
		data: singleQuadData,
		query: &Select{
			From: &ReversePropertyNamesAs{
				From: &Vertex{Values: []quad.Value{}},
				Tag:  "predicate",
			},
		},
		results: []interface{}{map[string]interface{}{"predicate": map[string]string{"@id": string(likes)}}},
	},
	{
		name: "PropertyNamesAs",
		data: singleQuadData,
		query: &Select{
			From: &PropertyNamesAs{
				From: &Vertex{Values: []quad.Value{}},
				Tag:  "predicate",
			},
		},
		results: []interface{}{map[string]interface{}{"predicate": map[string]string{"@id": string(likes)}}},
	},
	{
		name: "ReverseProperties",
		data: singleQuadData,
		query: &Select{
			From: &ReverseProperties{
				From:  &Vertex{Values: []quad.Value{}},
				Names: []quad.IRI{likes},
			},
		},
		results: []interface{}{map[string]interface{}{string(likes): map[string]string{"@id": string(alice)}}},
	},
	{
		name: "Skip",
		data: singleQuadData,
		query: &Skip{
			Offset: 2,
			From: &Vertex{
				Values: []quad.Value{},
			},
		},
		results: []interface{}{
			map[string]string{"@id": string(bob)},
		},
	},
	{
		name: "Union",
		data: singleQuadData,
		query: &Union{
			From: &Vertex{
				Values: []quad.Value{alice},
			},
			Steps: []linkedql.PathStep{
				&Vertex{
					Values: []quad.Value{bob},
				},
			},
		},
		results: []interface{}{
			map[string]string{"@id": string(alice)},
			map[string]string{"@id": string(bob)},
		},
	},
	{
		name: "SelectFirst",
		data: singleQuadData,
		query: &SelectFirst{
			From: &As{
				From: &Visit{
					From: &As{
						Name: string(liker),
						From: &Vertex{},
					},
					Properties: linkedql.PropertyPath{&Vertex{Values: []quad.Value{likes}}},
				},
				Name: string(liked),
			},
		},
		results: []interface{}{map[string]interface{}{string(liked): map[string]string{"@id": string(bob)}, string(liker): map[string]string{"@id": string(alice)}}},
	},
	{
		name: "Unique",
		data: singleQuadData,
		query: &Unique{
			From: &Vertex{
				Values: []quad.Value{alice, alice, bob},
			},
		},
		results: []interface{}{
			map[string]string{"@id": string(alice)},
			map[string]string{"@id": string(bob)},
		},
	},
	{
		name: "Order",
		data: singleQuadData,
		query: &Order{
			From: &Vertex{},
		},
		results: []interface{}{
			map[string]string{"@id": string(alice)},
			map[string]string{"@id": string(bob)},
			map[string]string{"@id": string(likes)},
		},
	},
	{
		name: "Optional",
		data: []quad.Quad{
			quad.Quad{alice, likes, bob, nil},
			quad.Quad{alice, name, quad.String("Alice"), nil},
			quad.Quad{bob, name, quad.String("Bob"), nil},
		},
		query: &Select{
			From: &Optional{
				From: &Properties{
					From:  &Vertex{Values: []quad.Value{}},
					Names: []quad.IRI{name},
				},
				Step: &Properties{
					From:  &Placeholder{},
					Names: []quad.IRI{likes},
				},
			},
		},
		results: []interface{}{
			map[string]interface{}{
				string(likes): map[string]string{"@id": string(bob)},
				string(name):  "Alice",
			},
			map[string]interface{}{
				string(name): "Bob",
			},
		},
	},
	{
		name: "Where",
		data: []quad.Quad{
			quad.Quad{alice, likes, bob, nil},
			quad.Quad{alice, name, quad.String("Alice"), nil},
			quad.Quad{bob, name, quad.String("Bob"), nil},
		},
		query: &Select{
			From: &As{
				From: &Where{
					From: &Vertex{},
					Steps: []linkedql.PathStep{
						&As{
							From: &Visit{
								From: &Visit{
									From:       &Placeholder{},
									Properties: linkedql.PropertyPath{&Vertex{Values: []quad.Value{likes}}},
								},
								Properties: linkedql.PropertyPath{&Vertex{Values: []quad.Value{name}}},
							},
							Name: string(likesName),
						},
						&As{
							From: &Visit{
								From:       &Placeholder{},
								Properties: linkedql.PropertyPath{&Vertex{Values: []quad.Value{name}}},
							},
							Name: string(name),
						},
					},
				},
				Name: string(person),
			},
		},
		results: []interface{}{
			map[string]interface{}{
				string(person):    map[string]string{"@id": string(alice)},
				string(name):      "Alice",
				string(likesName): "Bob",
			},
		},
	},
	{
		name: "Documents",
		data: []quad.Quad{
			quad.Quad{alice, likes, bob, nil},
			quad.Quad{alice, name, quad.String("Alice"), nil},
			quad.Quad{bob, name, quad.String("Bob"), nil},
			quad.Quad{bob, likes, alice, nil},
		},
		query: &Documents{
			From: &Properties{
				From:  &Vertex{Values: []quad.Value{}},
				Names: []quad.IRI{name, likes},
			},
		},
		results: []interface{}{
			map[string]interface{}{
				"@id":         string(alice),
				string(name):  []interface{}{"Alice"},
				string(likes): []interface{}{map[string]string{"@id": string(bob)}},
			},
			map[string]interface{}{
				"@id":         string(bob),
				string(name):  []interface{}{"Bob"},
				string(likes): []interface{}{map[string]string{"@id": string(alice)}},
			},
		},
	},
	{
		name: "Context",
		data: []quad.Quad{
			quad.Quad{alice, likes, bob, nil},
			quad.Quad{bob, likes, alice, nil},
		},
		query: &Context{
			From: &Has{
				From:     &Vertex{},
				Property: linkedql.PropertyPath{linkedql.PropertyIRI("likes")},
				Values:   []quad.Value{bob},
			},
			Rules: map[string]string{
				"bob":   string(bob),
				"likes": string(likes),
			},
		},
		results: []interface{}{
			map[string]string{"@id": string(alice)},
		},
	},
}

func TestLinkedQL(t *testing.T) {
	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			store := memstore.New(c.data...)
			voc := voc.Namespaces{}
			ctx := context.TODO()
			iterator, err := c.query.BuildIterator(store, &voc)
			require.NoError(t, err)
			var results []interface{}
			for iterator.Next(ctx) {
				results = append(results, iterator.Result())
			}
			require.NoError(t, iterator.Err())
			require.Equal(t, c.results, results)
		})
	}
}
