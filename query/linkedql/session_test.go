package linkedql

import (
	"context"
	"testing"

	"github.com/cayleygraph/cayley/graph/memstore"
	"github.com/cayleygraph/cayley/query"
	"github.com/cayleygraph/quad"
	"github.com/stretchr/testify/require"
)

var testCases = []struct {
	name    string
	data    []quad.Quad
	query   string
	results []interface{}
}{
	{
		name: "All Vertices",
		data: []quad.Quad{
			quad.MakeIRI("alice", "likes", "bob", ""),
		},
		query: `{
			"@type": "linkedql:Vertex"
		}`,
		results: []interface{}{
			quad.IRI("alice"),
			quad.IRI("likes"),
			quad.IRI("bob"),
		},
	},
	{
		name: "Out",
		data: []quad.Quad{
			quad.MakeIRI("alice", "likes", "bob", ""),
		},
		query: `
		{
			"@type": "linkedql:Out",
			"from": {
				"@type": "linkedql:Vertex"
			},
			"via": {
				"@type": "linkedql:Vertex",
				"values": [
					{
						"@id": "likes"
					}
				]
			}
		}`,
		results: []interface{}{
			quad.IRI("bob"),
		},
	},
	{
		name: "TagArray",
		data: []quad.Quad{
			quad.MakeIRI("alice", "likes", "bob", ""),
		},
		query: `
		{
			"@type": "linkedql:TagArray",
			"from": {
				"@type": "linkedql:As",
				"tags": ["liked"],
				"from": {
					"@type": "linkedql:Out",
					"from": {
						"@type": "linkedql:As",
						"tags": ["liker"],
						"from": {
							"@type": "linkedql:Vertex"
						}
					},
					"via": {
						"@type": "linkedql:Vertex",
						"values": [
							{
								"@id": "likes"
							}
						]
					}
				}
			}
		}
		`,
		results: []interface{}{
			map[string]quad.Value{
				"liker": quad.IRI("alice"),
				"liked": quad.IRI("bob"),
			},
		},
	},
	{
		name: "Intersect",
		data: []quad.Quad{
			quad.MakeIRI("bob", "likes", "alice", ""),
			quad.MakeIRI("dani", "likes", "alice", ""),
		},
		query: `
		{
			"@type": "linkedql:Intersect",
			"from": {
				"@type": "linkedql:Out",
				"from": {
					"@type": "linkedql:Vertex",
					"values": [{ "@id": "bob" }]
				},
				"via": {
					"@type": "linkedql:Vertex",
					"values": [
						{
							"@id": "likes"
						}
					]
				}
			},
			"intersectee": {
				"@type": "linkedql:Out",
				"from": {
					"@type": "linkedql:Vertex",
					"values": [{ "@id": "bob" }]
				},
				"via": {
					"@type": "linkedql:Vertex",
					"values": [
						{
							"@id": "likes"
						}
					]
				}
			}
		}
		`,
		results: []interface{}{
			quad.IRI("alice"),
		},
	},
	{
		name: "Is",
		data: []quad.Quad{
			quad.MakeIRI("alice", "likes", "bob", ""),
		},
		query: `
		{
			"@type": "linkedql:Is",
			"values": [{ "@id": "bob" }],
			"from": {
				"@type": "linkedql:Out",
				"from": {
					"@type": "linkedql:Vertex",
					"values": [{ "@id": "alice" }]
				},
				"via": {
					"@type": "linkedql:Vertex",
					"values": [
						{
							"@id": "likes"
						}
					]
				}
			}
		}
		`,
		results: []interface{}{
			quad.IRI("bob"),
		},
	},
	{
		name: "Back",
		data: []quad.Quad{
			quad.MakeIRI("alice", "likes", "bob", ""),
		},
		query: `
		{
			"@type": "linkedql:Back",
			"from": {
				"@type": "linkedql:Out",
				"from": {
					"@type": "linkedql:Vertex",
					"values": [{ "@id": "alice" }]
				},
				"via": {
					"@type": "linkedql:Vertex",
					"values": [
						{
							"@id": "likes"
						}
					]
				}
			}
		}
		`,
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
		query: `
		{
			"@type": "linkedql:Both",
			"from": {
				"@type": "linkedql:Vertex",
				"values": [{ "@id": "bob" }]
			},
			"via": {
				"@type": "linkedql:Vertex",
				"values": [
					{
						"@id": "likes"
					}
				]
			}
		}
		`,
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
		query: `
		{
			"@type": "linkedql:Count",
			"from": {
				"@type": "linkedql:Vertex"
			}
		}
		`,
		results: []interface{}{
			quad.Int(4),
		},
	},
	{
		name: "Except",
		data: []quad.Quad{
			quad.MakeIRI("alice", "likes", "bob", ""),
		},
		query: `
		{
			"@type": "linkedql:Except",
			"from": {
				"@type": "linkedql:Vertex",
				"values": [{ "@id": "alice" }, { "@id": "likes" }]
			},
			"excepted": {
				"@type": "linkedql:Vertex",
				"values": [{ "@id": "likes" }]
			}
		}
		`,
		results: []interface{}{
			quad.IRI("alice"),
		},
	},
}

func TestLinkedQL(t *testing.T) {
	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			store := memstore.New(c.data...)
			session := NewSession(store)
			ctx := context.TODO()
			iterator, err := session.Execute(ctx, c.query, query.Options{})
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
