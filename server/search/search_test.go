package search

import (
	"context"
	"testing"

	"github.com/blevesearch/bleve/document"
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
	expectedDocument := document.NewDocument("alice")
	expectedDocument.AddField(document.NewTextField("name", nil, []byte("Alice Liddell")))
	require.NoError(t, err)
	require.Equal(t, []*document.Document{expectedDocument}, documents)
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
	fields, err := index.Fields()
	require.Equal(t, fields, []string{"name"})
	expectedDocuments, err := getDocuments(context.TODO(), qs, configs[0])
	for _, expectedDocument := range expectedDocuments {
		d, err := index.Document(expectedDocument.ID)
		require.NoError(t, err)
		require.Equal(t, expectedDocument, d)
	}
	results, err := Search(index, "alice")
	require.NoError(t, err)
	require.Len(t, results, 1)
}
