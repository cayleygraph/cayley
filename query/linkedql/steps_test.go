package linkedql

import (
	"context"
	"testing"

	"github.com/cayleygraph/cayley/graph/memstore"
	"github.com/cayleygraph/quad"
	"github.com/stretchr/testify/require"
)

var testCases = []struct {
	name    string
	data    []quad.Quad
	query   Step
	results []interface{}
}{
	{
		name: "All Vertices",
		data: []quad.Quad{
			quad.MakeIRI("alice", "likes", "bob", ""),
		},
		query: &Vertex{Values: []quad.Value{}},
		results: []interface{}{
			quad.IRI("alice"),
			quad.IRI("likes"),
			quad.IRI("bob"),
		},
	},
	{
		name: "TagArray",
		data: []quad.Quad{
			quad.MakeIRI("alice", "likes", "bob", ""),
		},
		query: &TagArray{
			From: &As{
				Tags: []string{"liked"},
				From: &Out{
					From: &As{
						Tags: []string{"liker"},
						From: &Vertex{},
					},
					Via: &Vertex{Values: []quad.Value{quad.IRI("likes")}},
				},
			},
		},
		results: []interface{}{
			map[string]quad.Value{
				"liker": quad.IRI("alice"),
				"liked": quad.IRI("bob"),
			},
		},
	},
	{
		name: "Back",
		data: []quad.Quad{
			quad.MakeIRI("alice", "likes", "bob", ""),
		},
		query: &Back{
			From: &Out{
				From: &Vertex{
					Values: []quad.Value{quad.IRI("alice")},
				},
				Via: &Vertex{
					Values: []quad.Value{
						quad.IRI("likes"),
					},
				},
			},
		},
		results: []interface{}{
			quad.IRI("alice"),
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
			Via: &Vertex{Values: []quad.Value{quad.IRI("likes")}},
		},
		results: []interface{}{
			quad.IRI("alice"),
			quad.IRI("dan"),
		},
	},
	{
		name: "Count",
		data: []quad.Quad{
			quad.MakeIRI("alice", "likes", "bob", ""),
		},
		query: &Count{
			From: &Vertex{Values: []quad.Value{}},
		},
		results: []interface{}{
			quad.Int(4),
		},
	},
	{
		name: "Except",
		data: []quad.Quad{
			quad.MakeIRI("alice", "likes", "bob", ""),
		},
		query: &Except{
			From: &Vertex{
				Values: []quad.Value{quad.IRI("alice"), quad.IRI("likes")},
			},
			Excepted: &Vertex{
				Values: []quad.Value{quad.IRI("likes")},
			},
		},
		results: []interface{}{
			quad.IRI("alice"),
		},
	},
	{
		name: "Filter",
		data: []quad.Quad{
			{Subject: quad.IRI("alice"), Predicate: quad.IRI("name"), Object: quad.String("Alice"), Label: nil},
		},
		query: &Filter{
			From:   &Vertex{Values: []quad.Value{}},
			Filter: &RegExp{Expression: "A"},
		},
		results: []interface{}{
			quad.String("Alice"),
		},
	},
	{
		name: "Has",
		data: []quad.Quad{
			quad.MakeIRI("alice", "likes", "bob", ""),
		},
		query: &Has{
			From: &Vertex{
				Values: []quad.Value{},
			},
			Via: &Vertex{
				Values: []quad.Value{quad.IRI("likes")},
			},
			Values: []quad.Value{quad.IRI("bob")},
		},
		results: []interface{}{
			quad.IRI("alice"),
		},
	},
	{
		name: "HasReverse",
		data: []quad.Quad{
			quad.MakeIRI("alice", "likes", "bob", ""),
		},
		query: &HasReverse{
			From: &Vertex{
				Values: []quad.Value{},
			},
			Via: &Vertex{
				Values: []quad.Value{quad.IRI("likes")},
			},
			Values: []quad.Value{quad.IRI("alice")},
		},
		results: []interface{}{
			quad.IRI("bob"),
		},
	},
	{
		name: "In",
		data: []quad.Quad{
			quad.MakeIRI("alice", "likes", "bob", ""),
		},
		query: &In{
			From: &Vertex{Values: []quad.Value{}},
			Via:  &Vertex{Values: []quad.Value{quad.IRI("likes")}},
		},
		results: []interface{}{
			quad.IRI("alice"),
		},
	},
	{
		name: "InPredicates",
		data: []quad.Quad{
			quad.MakeIRI("alice", "likes", "bob", ""),
		},
		query: &InPredicates{
			From: &Vertex{Values: []quad.Value{}},
		},
		results: []interface{}{
			quad.IRI("likes"),
		},
	},
	{
		name: "Intersect",
		data: []quad.Quad{
			quad.MakeIRI("bob", "likes", "alice", ""),
			quad.MakeIRI("dani", "likes", "alice", ""),
		},
		query: &Intersect{
			From: &Out{
				From: &Vertex{Values: []quad.Value{quad.IRI("bob")}},
				Via: &Vertex{
					Values: []quad.Value{quad.IRI("likes")},
				},
			},
			Intersectee: &Out{
				From: &Vertex{Values: []quad.Value{quad.IRI("bob")}},
				Via:  &Vertex{Values: []quad.Value{quad.IRI("likes")}},
			},
		},
		results: []interface{}{
			quad.IRI("alice"),
		},
	},
	{
		name: "Is",
		data: []quad.Quad{
			quad.MakeIRI("alice", "likes", "bob", ""),
		},
		query: &Is{
			Values: []quad.Value{quad.IRI("bob")},
			From: &Out{
				From: &Vertex{Values: []quad.Value{quad.IRI("alice")}},
				Via: &Vertex{
					Values: []quad.Value{quad.IRI("likes")},
				},
			},
		},
		results: []interface{}{
			quad.IRI("bob"),
		},
	},
	{
		name: "Limit",
		data: []quad.Quad{
			quad.MakeIRI("alice", "likes", "bob", ""),
		},
		query: &Limit{
			Limit: 2,
			From: &Vertex{
				Values: []quad.Value{},
			},
		},
		results: []interface{}{
			quad.IRI("alice"),
			quad.IRI("likes"),
		},
	},
	{
		name: "Out",
		data: []quad.Quad{
			quad.MakeIRI("alice", "likes", "bob", ""),
		},
		query: &Out{
			From: &Vertex{Values: []quad.Value{}},
			Via:  &Vertex{Values: []quad.Value{quad.IRI("likes")}},
		},
		results: []interface{}{
			quad.IRI("bob"),
		},
	},
	{
		name: "OutPredicates",
		data: []quad.Quad{
			quad.MakeIRI("alice", "likes", "bob", ""),
		},
		query: &OutPredicates{
			From: &Vertex{Values: []quad.Value{}},
		},
		results: []interface{}{
			quad.IRI("likes"),
		},
	},
	{
		name: "Save",
		data: []quad.Quad{
			quad.MakeIRI("alice", "likes", "bob", ""),
		},
		query: &TagArray{
			From: &Save{
				From: &Vertex{Values: []quad.Value{}},
				Via:  &Vertex{Values: []quad.Value{quad.IRI("likes")}},
				Tag:  "likes",
			},
		},
		results: []interface{}{
			map[string]quad.Value{
				"likes": quad.IRI("bob"),
			},
		},
	},
	{
		name: "SaveInPredicates",
		data: []quad.Quad{
			quad.MakeIRI("alice", "likes", "bob", ""),
		},
		query: &TagArray{
			From: &SaveInPredicates{
				From: &Vertex{Values: []quad.Value{}},
				Tag:  "predicate",
			},
		},
		results: []interface{}{
			map[string]quad.Value{
				"predicate": quad.IRI("likes"),
			},
		},
	},
	{
		name: "SaveOptional",
		data: []quad.Quad{
			quad.MakeIRI("alice", "likes", "bob", ""),
			quad.Quad{Subject: quad.IRI("alice"), Predicate: quad.IRI("name"), Object: quad.String("Alice"), Label: nil},
			quad.MakeIRI("bob", "likes", "alice", ""),
		},
		query: &TagArray{
			From: &SaveOptional{
				From: &Vertex{Values: []quad.Value{}},
				Via:  &Vertex{Values: []quad.Value{quad.IRI("name")}},
				Tag:  "name",
			},
		},
		results: []interface{}{
			map[string]quad.Value{
				"name": quad.String("Alice"),
			},
			map[string]quad.Value{},
			map[string]quad.Value{},
			map[string]quad.Value{},
			map[string]quad.Value{},
		},
	},
	{
		name: "SaveOptionalReverse",
		data: []quad.Quad{
			quad.MakeIRI("alice", "likes", "bob", ""),
			quad.Quad{Subject: quad.IRI("alice"), Predicate: quad.IRI("name"), Object: quad.String("Alice"), Label: nil},
			quad.MakeIRI("bob", "likes", "alice", ""),
		},
		query: &TagArray{
			From: &SaveOptionalReverse{
				From: &Vertex{Values: []quad.Value{}},
				Via:  &Vertex{Values: []quad.Value{quad.IRI("name")}},
				Tag:  "name",
			},
		},
		results: []interface{}{
			map[string]quad.Value{},
			map[string]quad.Value{},
			map[string]quad.Value{},
			map[string]quad.Value{},
			map[string]quad.Value{
				"name": quad.IRI("alice"),
			},
		},
	},
	{
		name: "SaveOutPredicates",
		data: []quad.Quad{
			quad.MakeIRI("alice", "likes", "bob", ""),
		},
		query: &TagArray{
			From: &SaveOutPredicates{
				From: &Vertex{Values: []quad.Value{}},
				Tag:  "predicate",
			},
		},
		results: []interface{}{
			map[string]quad.Value{
				"predicate": quad.IRI("likes"),
			},
		},
	},
	{
		name: "SaveReverse",
		data: []quad.Quad{
			quad.MakeIRI("alice", "likes", "bob", ""),
		},
		query: &TagArray{
			From: &SaveReverse{
				From: &Vertex{Values: []quad.Value{}},
				Via:  &Vertex{Values: []quad.Value{quad.IRI("likes")}},
				Tag:  "likes",
			},
		},
		results: []interface{}{
			map[string]quad.Value{
				"likes": quad.IRI("alice"),
			},
		},
	},
	{
		name: "Skip",
		data: []quad.Quad{
			quad.MakeIRI("alice", "likes", "bob", ""),
		},
		query: &Skip{
			Offset: 2,
			From: &Vertex{
				Values: []quad.Value{},
			},
		},
		results: []interface{}{
			quad.IRI("bob"),
		},
	},
}

func TestLinkedQL(t *testing.T) {
	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			store := memstore.New(c.data...)
			ctx := context.TODO()
			iterator := c.query.BuildIterator(store)
			var results []interface{}
			for iterator.Next(ctx) {
				results = append(results, iterator.Result())
			}
			require.NoError(t, iterator.Err())
			require.Equal(t, c.results, results)
		})
	}
}
