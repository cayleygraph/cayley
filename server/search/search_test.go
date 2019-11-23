package search

import (
	"context"
	"testing"

	"github.com/cayleygraph/cayley/graph/memstore"
	"github.com/cayleygraph/quad"
	"github.com/stretchr/testify/require"
)

func TestGetDocuments(t *testing.T) {
	data := []quad.Quad{
		quad.Quad{
			Subject:   quad.IRI("alice"),
			Predicate: quad.IRI("name"),
			Object:    quad.String("Alice Liddell"),
			Label:     quad.IRI(""),
		},
	}
	config := IndexConfig{
		Name: "people",
		Properties: []quad.IRI{
			quad.IRI("name"),
		},
	}
	qs := memstore.New(data...)
	documents, err := getDocuments(context.TODO(), qs, config)
	expectedDocuments := []document{
		{
			id: "alice",
			data: map[string]interface{}{
				"_type": "people",
				"name":  []interface{}{"Alice Liddell"},
			},
		},
	}
	require.NoError(t, err)
	require.Equal(t, expectedDocuments, documents)
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
			Properties: []quad.IRI{
				quad.IRI("name"),
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
