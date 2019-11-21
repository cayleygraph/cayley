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
	qs := memstore.New(data...)
	documents, err := getDocuments(context.TODO(), qs, []quad.IRI{})
	require.NoError(t, err)
	require.NotEmpty(t, documents)
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
	qs := memstore.New(data...)
	ClearIndex()
	index, err := NewIndex(context.TODO(), qs)
	require.NoError(t, err)
	results, err := Search(index, "al")
	require.NoError(t, err)
	require.NotEmpty(t, results)
}
