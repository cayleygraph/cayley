package search

import (
	"context"

	"github.com/blevesearch/bleve"
	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/refs"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad"
)

// IndexPath is the path to the directory the search index will be stored at
const IndexPath = "searchIndex.bleve"

// ID is the identifier for the data indexed by the search
type ID = quad.IRI

// Properties are the data indexed by the search
type Properties = map[quad.Value][]quad.Value

// Document is a container around ID and it's associated Properties
type Document = struct {
	ID
	Properties
}

// getDocuments for given IDs reterives documents from the graph
func getDocuments(ctx context.Context, qs graph.QuadStore, ids []quad.IRI) ([]Document, error) {
	var values []quad.Value
	for _, iri := range ids {
		values = append(values, iri)
	}
	p := path.StartPath(qs, values...).OutWithTags([]string{"key"}, path.StartPath(qs)).Tag("value").Back("")
	scanner := p.BuildIterator(ctx).Iterate()
	idToProperties := make(map[quad.IRI]Properties)
	for scanner.Next(ctx) {
		err := scanner.Err()
		if err != nil {
			return nil, err
		}
		tags := make(map[string]refs.Ref)
		scanner.TagResults(tags)
		name := qs.NameOf(scanner.Result())
		// Should this be BNode as well?
		iri, ok := name.(quad.IRI)
		if !ok {
			continue
		}
		properties, ok := idToProperties[iri]
		if !ok {
			properties = make(Properties)
			idToProperties[iri] = properties
		}
		key := qs.NameOf(tags["key"])
		value := qs.NameOf(tags["value"])
		properties[key] = append(properties[key], value)
	}
	var documents []Document
	for iri, properties := range idToProperties {
		document := Document{
			ID:         iri,
			Properties: properties,
		}
		documents = append(documents, document)
	}
	return documents, nil
}

// OpenIndex opens an existing index
func OpenIndex() (bleve.Index, error) {
	return bleve.Open(IndexPath)
}

// NewIndex builds a new search index
func NewIndex(ctx context.Context, qs graph.QuadStore) (bleve.Index, error) {
	mapping := bleve.NewIndexMapping()
	clog.Infof("Building search index...")
	index, err := bleve.New(IndexPath, mapping)
	if err != nil {
		return nil, err
	}
	clog.Infof("Retreiving documents...")
	documents, err := getDocuments(ctx, qs, []quad.IRI{})
	for _, document := range documents {
		index.Index(string(document.ID), document.Properties)
	}
	clog.Infof("Retrieved %v documents...", len(documents))
	clog.Infof("Built search index")
	return index, nil
}

// GetIndex attempts to open an existing index, if it doesn't exist it creates a new one
func GetIndex(ctx context.Context, qs graph.QuadStore) (bleve.Index, error) {
	index, err := OpenIndex()
	if err == bleve.ErrorIndexPathDoesNotExist {
		return NewIndex(ctx, qs)
	}
	return index, err
}
