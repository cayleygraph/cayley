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
			Object:    quad.String("Alice"),
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
	require.NoError(t, err)
	require.Equal(t, []Document{
		{
			ID: quad.IRI("alice"),
			Fields: Fields{
				"_type": "people",
				"name":  "Alice",
			},
		},
	}, documents)
}

func TestSearch(t *testing.T) {
	data := []quad.Quad{
		quad.Quad{
			Subject:   quad.IRI("alice"),
			Predicate: quad.IRI("name"),
			Object:    quad.String("Alice"),
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
	results, err := Search(index, "alice")
	require.NoError(t, err)
	require.NotEmpty(t, results)
	require.Len(t, results, 1)
}
