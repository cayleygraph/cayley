package search

import (
	"context"
	"testing"

	"github.com/cayleygraph/cayley/graph/memstore"
	"github.com/cayleygraph/quad"
	"github.com/stretchr/testify/require"
)

type testCase struct {
	Name     string
	Data     []quad.Quad
	Config   IndexConfig
	Expected []document
	Entities []quad.Value
}

var testCases = []testCase{
	{
		Name: "Simple",
		Data: []quad.Quad{
			quad.Quad{
				Subject:   quad.IRI("alice"),
				Predicate: quad.IRI("name"),
				Object:    quad.String("Alice Liddell"),
				Label:     quad.IRI(""),
			},
		},
		Config: IndexConfig{
			Name: "people",
			Properties: []PropertyConfig{
				{
					Name: quad.IRI("name"),
				},
			},
		},
		Expected: []document{
			{
				id: "alice",
				data: map[string]interface{}{
					"_type": "people",
					"name":  []interface{}{"Alice Liddell"},
				},
			},
		},
	},
	{
		Name: "With Match",
		Data: []quad.Quad{
			quad.Quad{
				Subject:   quad.IRI("alice"),
				Predicate: quad.IRI("name"),
				Object:    quad.String("Alice Liddell"),
				Label:     quad.IRI(""),
			},
			quad.Quad{
				Subject:   quad.IRI("alice"),
				Predicate: quad.IRI("rdf:type"),
				Object:    quad.IRI("Person"),
				Label:     quad.IRI(""),
			},
			quad.Quad{
				Subject:   quad.IRI("bob"),
				Predicate: quad.IRI("name"),
				Object:    quad.String("Bob Cool"),
				Label:     quad.IRI(""),
			},
		},
		Config: IndexConfig{
			Name: "people",
			Match: map[quad.IRI][]quad.Value{
				quad.IRI("rdf:type"): {
					quad.IRI("Person"),
				},
			},
			Properties: []PropertyConfig{
				{
					Name: quad.IRI("name"),
				},
			},
		},
		Expected: []document{
			{
				id: "alice",
				data: map[string]interface{}{
					"_type": "people",
					"name":  []interface{}{"Alice Liddell"},
				},
			},
		},
	},
	{
		Name: "With Entities",
		Data: []quad.Quad{
			quad.Quad{
				Subject:   quad.IRI("alice"),
				Predicate: quad.IRI("name"),
				Object:    quad.String("Alice Liddell"),
				Label:     quad.IRI(""),
			},
			quad.Quad{
				Subject:   quad.IRI("alice"),
				Predicate: quad.IRI("rdf:type"),
				Object:    quad.IRI("Person"),
				Label:     quad.IRI(""),
			},
			quad.Quad{
				Subject:   quad.IRI("bob"),
				Predicate: quad.IRI("name"),
				Object:    quad.String("Bob Cool"),
				Label:     quad.IRI(""),
			},
		},
		Config: IndexConfig{
			Name: "people",
			Properties: []PropertyConfig{
				{
					Name: quad.IRI("name"),
				},
			},
		},
		Expected: []document{
			{
				id: "alice",
				data: map[string]interface{}{
					"_type": "people",
					"name":  []interface{}{"Alice Liddell"},
				},
			},
		},
		Entities: []quad.Value{
			quad.IRI("alice"),
		},
	},
}

func TestGetDocuments(t *testing.T) {
	for _, c := range testCases {
		t.Run(c.Name, func(t *testing.T) {
			qs := memstore.New(c.Data...)
			documents, err := getDocuments(context.TODO(), qs, c.Config, c.Entities)
			require.NoError(t, err)
			require.Equal(t, c.Expected, documents)
		})
	}
}

func TestSearch(t *testing.T) {
	data := []quad.Quad{
		quad.Quad{
			Subject:   quad.IRI("alice"),
			Predicate: quad.IRI("name"),
			Object:    quad.String("Alice Liddell"),
			Label:     quad.IRI(""),
		},
	}
	configs := []IndexConfig{
		{
			Name: "people",
			Properties: []PropertyConfig{
				{
					Name: quad.IRI("name"),
				},
			},
		},
	}
	qs := memstore.New(data...)
	ClearIndex()
	index, err := NewIndex(context.TODO(), qs, configs)
	require.NoError(t, err)

	results, err := Search(index, "Alice Liddell")
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, quad.IRI("alice"), results[0])

	results, err = Search(index, "Liddell")
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, quad.IRI("alice"), results[0])

	results, err = Search(index, "Alice")
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, quad.IRI("alice"), results[0])
}
