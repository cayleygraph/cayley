package linkedql

import (
	"context"
	"testing"

	"github.com/cayleygraph/cayley/graph/memstore"
	"github.com/cayleygraph/quad"
	"github.com/stretchr/testify/require"
)

var singleQuadData = []quad.Quad{
	quad.MakeIRI("alice", "likes", "bob", ""),
}

var testCases = []struct {
	name    string
	data    []quad.Quad
	query   IteratorStep
	results []interface{}
}{
	{
		name:  "All Entities",
		data:  singleQuadData,
		query: &Entities{Identifiers: []EntityIdentifier{EntityIdentifierString("alice")}},
		results: []interface{}{
			map[string]string{"@id": "alice"},
		},
	},
	{
		name:  "All Vertices",
		data:  singleQuadData,
		query: &Vertex{Values: nil},
		results: []interface{}{
			map[string]string{"@id": "alice"},
			map[string]string{"@id": "likes"},
			map[string]string{"@id": "bob"},
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
						Name: "liker",
					},
					Properties: &Vertex{Values: []quad.Value{quad.IRI("likes")}},
				},
				Name: "liked",
			},
		},
		results: []interface{}{
			map[string]interface{}{
				"liker": map[string]string{"@id": "alice"},
				"liked": map[string]string{"@id": "bob"},
			},
		},
	},
	{
		name: "Select with tags",
		data: singleQuadData,
		query: &Select{
			Tags: []string{"liker"},
			From: &As{
				From: &Visit{
					From: &As{
						From: &Vertex{},
						Name: "liker",
					},
					Properties: &Vertex{Values: []quad.Value{quad.IRI("likes")}},
				},
				Name: "liked",
			},
		},
		results: []interface{}{
			map[string]interface{}{
				"liker": map[string]string{"@id": "alice"},
			},
		},
	},
	{
		name: "Back",
		data: singleQuadData,
		query: &Back{
			From: &Visit{
				From: &Vertex{
					Values: []quad.Value{quad.IRI("alice")},
				},
				Properties: &Vertex{
					Values: []quad.Value{
						quad.IRI("likes"),
					},
				},
			},
		},
		results: []interface{}{
			map[string]string{"@id": "alice"},
		},
	},
	{
		name: "Both",
		data: []quad.Quad{
			quad.MakeIRI("alice", "likes", "bob", ""),
			quad.MakeIRI("bob", "likes", "dan", ""),
		},
		query: &Both{
			From: &Vertex{
				Values: []quad.Value{quad.IRI("bob")},
			},
			Properties: &Vertex{Values: []quad.Value{quad.IRI("likes")}},
		},
		results: []interface{}{
			map[string]string{"@id": "alice"},
			map[string]string{"@id": "dan"},
		},
	},
	{
		name: "Count",
		data: singleQuadData,
		query: &Count{
			From: &Vertex{Values: []quad.Value{}},
		},
		results: []interface{}{
			map[string]string{"@value": "4", "@type": "schema:Integer"},
		},
	},
	{
		name: "Difference",
		data: singleQuadData,
		query: &Difference{
			From: &Vertex{
				Values: []quad.Value{quad.IRI("alice"), quad.IRI("likes")},
			},
			Steps: []PathStep{
				&Vertex{
					Values: []quad.Value{quad.IRI("likes")},
				},
			},
		},
		results: []interface{}{
			map[string]string{"@id": "alice"},
		},
	},
	{
		name: "Filter RegExp",
		data: []quad.Quad{
			{Subject: quad.IRI("alice"), Predicate: quad.IRI("name"), Object: quad.String("Alice"), Label: nil},
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
			{Subject: quad.IRI("alice"), Predicate: quad.IRI("name"), Object: quad.String("Alice"), Label: nil},
		},
		query: &Filter{
			From:   &Vertex{Values: []quad.Value{}},
			Filter: &Like{Pattern: "a%"},
		},
		results: []interface{}{
			map[string]string{"@id": "alice"},
		},
	},
	{
		name: "Filter LessThan",
		data: []quad.Quad{
			{Subject: quad.IRI("alice"), Predicate: quad.IRI("name"), Object: quad.Int(0), Label: nil},
			{Subject: quad.IRI("alice"), Predicate: quad.IRI("name"), Object: quad.Int(1), Label: nil},
		},
		query: &LessThan{
			From:  &Vertex{Values: []quad.Value{}},
			Value: quad.Int(1),
		},
		results: []interface{}{
			map[string]string{"@value": "0", "@type": "schema:Integer"},
		},
	},
	{
		name: "Filter GreaterThan",
		data: []quad.Quad{
			{Subject: quad.IRI("alice"), Predicate: quad.IRI("name"), Object: quad.Int(0), Label: nil},
			{Subject: quad.IRI("alice"), Predicate: quad.IRI("name"), Object: quad.Int(1), Label: nil},
		},
		query: &GreaterThan{
			From:  &Vertex{Values: []quad.Value{}},
			Value: quad.Int(0),
		},
		results: []interface{}{
			map[string]string{"@value": "1", "@type": "schema:Integer"},
		},
	},
	{
		name: "Filter LessThanEquals",
		data: []quad.Quad{
			{Subject: quad.IRI("alice"), Predicate: quad.IRI("name"), Object: quad.Int(-1), Label: nil},
			{Subject: quad.IRI("alice"), Predicate: quad.IRI("name"), Object: quad.Int(0), Label: nil},
			{Subject: quad.IRI("alice"), Predicate: quad.IRI("name"), Object: quad.Int(1), Label: nil},
		},
		query: &LessThanEquals{
			From:  &Vertex{Values: []quad.Value{}},
			Value: quad.Int(0),
		},
		results: []interface{}{
			map[string]string{"@value": "-1", "@type": "schema:Integer"},
			map[string]string{"@value": "0", "@type": "schema:Integer"},
		},
	},
	{
		name: "Filter GreaterThanEquals",
		data: []quad.Quad{
			{Subject: quad.IRI("alice"), Predicate: quad.IRI("name"), Object: quad.Int(0), Label: nil},
			{Subject: quad.IRI("alice"), Predicate: quad.IRI("name"), Object: quad.Int(1), Label: nil},
			{Subject: quad.IRI("alice"), Predicate: quad.IRI("name"), Object: quad.Int(2), Label: nil},
		},
		query: &GreaterThanEquals{
			From:  &Vertex{Values: []quad.Value{}},
			Value: quad.Int(1),
		},
		results: []interface{}{
			map[string]string{"@value": "1", "@type": "schema:Integer"},
			map[string]string{"@value": "2", "@type": "schema:Integer"},
		},
	},
	{
		name: "Has",
		data: singleQuadData,
		query: &Has{
			From: &Vertex{
				Values: []quad.Value{},
			},
			Property: &Vertex{
				Values: []quad.Value{quad.IRI("likes")},
			},
			Values: []quad.Value{quad.IRI("bob")},
		},
		results: []interface{}{
			map[string]string{"@id": "alice"},
		},
	},
	{
		name: "HasReverse",
		data: singleQuadData,
		query: &HasReverse{
			From: &Vertex{
				Values: []quad.Value{},
			},
			Property: &Vertex{
				Values: []quad.Value{quad.IRI("likes")},
			},
			Values: []quad.Value{quad.IRI("alice")},
		},
		results: []interface{}{
			map[string]string{"@id": "bob"},
		},
	},
	{
		name: "ViewReverse",
		data: singleQuadData,
		query: &VisitReverse{
			From:       &Vertex{Values: []quad.Value{}},
			Properties: &Vertex{Values: []quad.Value{quad.IRI("likes")}},
		},
		results: []interface{}{
			map[string]string{"@id": "alice"},
		},
	},
	{
		name: "PropertyNames",
		data: singleQuadData,
		query: &PropertyNames{
			From: &Vertex{Values: []quad.Value{}},
		},
		results: []interface{}{
			map[string]string{"@id": "likes"},
		},
	},
	{
		name: "Intersect",
		data: []quad.Quad{
			quad.MakeIRI("bob", "likes", "alice", ""),
			quad.MakeIRI("dani", "likes", "alice", ""),
		},
		query: &Intersect{
			From: &Visit{
				From: &Vertex{Values: []quad.Value{quad.IRI("bob")}},
				Properties: &Vertex{
					Values: []quad.Value{quad.IRI("likes")},
				},
			},
			Steps: []PathStep{
				&Visit{
					From:       &Vertex{Values: []quad.Value{quad.IRI("bob")}},
					Properties: &Vertex{Values: []quad.Value{quad.IRI("likes")}},
				},
			},
		},
		results: []interface{}{
			map[string]string{"@id": "alice"},
		},
	},
	{
		name: "Is",
		data: singleQuadData,
		query: &Is{
			Values: []quad.Value{quad.IRI("bob")},
			From: &Visit{
				From: &Vertex{Values: []quad.Value{quad.IRI("alice")}},
				Properties: &Vertex{
					Values: []quad.Value{quad.IRI("likes")},
				},
			},
		},
		results: []interface{}{
			map[string]string{"@id": "bob"},
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
			map[string]string{"@id": "alice"},
			map[string]string{"@id": "likes"},
		},
	},
	{
		name: "View",
		data: singleQuadData,
		query: &Visit{
			From:       &Vertex{Values: []quad.Value{}},
			Properties: &Vertex{Values: []quad.Value{quad.IRI("likes")}},
		},
		results: []interface{}{
			map[string]string{"@id": "bob"},
		},
	},
	{
		name: "PropertyNames",
		data: singleQuadData,
		query: &PropertyNames{
			From: &Vertex{Values: []quad.Value{}},
		},
		results: []interface{}{
			map[string]string{"@id": "likes"},
		},
	},
	{
		name: "Properties",
		data: singleQuadData,
		query: &Select{
			From: &Properties{
				From:  &Vertex{Values: []quad.Value{}},
				Names: []quad.IRI{quad.IRI("likes")},
			},
		},
		results: []interface{}{map[string]interface{}{"likes": map[string]string{"@id": "bob"}}},
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
		results: []interface{}{map[string]interface{}{"predicate": map[string]string{"@id": "likes"}}},
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
		results: []interface{}{map[string]interface{}{"predicate": map[string]string{"@id": "likes"}}},
	},
	{
		name: "ReverseProperties",
		data: singleQuadData,
		query: &Select{
			From: &ReverseProperties{
				From:  &Vertex{Values: []quad.Value{}},
				Names: []quad.IRI{quad.IRI("likes")},
			},
		},
		results: []interface{}{map[string]interface{}{"likes": map[string]string{"@id": "alice"}}},
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
			map[string]string{"@id": "bob"},
		},
	},
	{
		name: "Union",
		data: singleQuadData,
		query: &Union{
			From: &Vertex{
				Values: []quad.Value{quad.IRI("alice")},
			},
			Steps: []PathStep{
				&Vertex{
					Values: []quad.Value{quad.IRI("bob")},
				},
			},
		},
		results: []interface{}{
			map[string]string{"@id": "alice"},
			map[string]string{"@id": "bob"},
		},
	},
	{
		name: "SelectFirst",
		data: singleQuadData,
		query: &SelectFirst{
			From: &As{
				From: &Visit{
					From: &As{
						Name: "liker",
						From: &Vertex{},
					},
					Properties: &Vertex{Values: []quad.Value{quad.IRI("likes")}},
				},
				Name: "liked",
			},
		},
		results: []interface{}{map[string]interface{}{"liked": map[string]string{"@id": "bob"}, "liker": map[string]string{"@id": "alice"}}},
	},
	{
		name: "Unique",
		data: singleQuadData,
		query: &Unique{
			From: &Vertex{
				Values: []quad.Value{quad.IRI("alice"), quad.IRI("alice"), quad.IRI("bob")},
			},
		},
		results: []interface{}{
			map[string]string{"@id": "alice"},
			map[string]string{"@id": "bob"},
		},
	},
	{
		name: "Order",
		data: singleQuadData,
		query: &Order{
			From: &Vertex{},
		},
		results: []interface{}{
			map[string]string{"@id": "alice"},
			map[string]string{"@id": "bob"},
			map[string]string{"@id": "likes"},
		},
	},
	{
		name: "Optional",
		data: []quad.Quad{
			quad.MakeIRI("alice", "likes", "bob", ""),
			quad.MakeIRI("alice", "name", "Alice", ""),
			quad.MakeIRI("bob", "name", "Bob", ""),
		},
		query: &Select{
			From: &Optional{
				From: &Properties{
					From:  &Vertex{Values: []quad.Value{}},
					Names: []quad.IRI{quad.IRI("name")},
				},
				Step: &Properties{
					From:  &Placeholder{},
					Names: []quad.IRI{quad.IRI("likes")},
				},
			},
		},
		results: []interface{}{
			map[string]interface{}{
				"likes": map[string]string{"@id": "bob"},
				"name":  map[string]string{"@id": "Alice"},
			},
			map[string]interface{}{
				"name": map[string]string{"@id": "Bob"},
			},
		},
	},
	{
		name: "Where",
		data: []quad.Quad{
			quad.MakeIRI("alice", "likes", "bob", ""),
			quad.MakeIRI("alice", "name", "Alice", ""),
			quad.MakeIRI("bob", "name", "Bob", ""),
		},
		query: &Select{
			From: &As{
				From: &Where{
					From: &Vertex{},
					Steps: []PathStep{
						&As{
							From: &Visit{
								From: &Visit{
									From:       &Placeholder{},
									Properties: &Vertex{Values: []quad.Value{quad.IRI("likes")}},
								},
								Properties: &Vertex{Values: []quad.Value{quad.IRI("name")}},
							},
							Name: "likesName",
						},
						&As{
							From: &Visit{
								From:       &Placeholder{},
								Properties: &Vertex{Values: []quad.Value{quad.IRI("name")}},
							},
							Name: "name",
						},
					},
				},
				Name: "person",
			},
		},
		results: []interface{}{
			map[string]interface{}{
				"person":    map[string]string{"@id": "alice"},
				"name":      map[string]string{"@id": "Alice"},
				"likesName": map[string]string{"@id": "Bob"},
			},
		},
	},
	{
		name: "Documents",
		data: []quad.Quad{
			quad.MakeIRI("alice", "likes", "bob", ""),
			quad.MakeIRI("alice", "name", "Alice", ""),
			quad.MakeIRI("bob", "name", "Bob", ""),
			quad.MakeIRI("bob", "likes", "alice", ""),
		},
		query: &Documents{
			From: &Properties{
				From:  &Vertex{Values: []quad.Value{}},
				Names: []quad.IRI{quad.IRI("name"), quad.IRI("likes")},
			},
		},
		results: []interface{}{
			map[string]interface{}{
				"@id":   "alice",
				"name":  []interface{}{map[string]string{"@id": "Alice"}},
				"likes": []interface{}{map[string]string{"@id": "bob"}},
			},
			map[string]interface{}{
				"@id":   "bob",
				"name":  []interface{}{map[string]string{"@id": "Bob"}},
				"likes": []interface{}{map[string]string{"@id": "alice"}},
			},
		},
	},
}

func TestLinkedQL(t *testing.T) {
	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			store := memstore.New(c.data...)
			ctx := context.TODO()
			iterator, err := c.query.BuildIterator(store)
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
