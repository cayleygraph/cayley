package search

import (
	"context"
	"os"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/analysis/analyzer/simple"
	"github.com/blevesearch/bleve/mapping"
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

// Fields are the data indexed by the search
type Fields = map[string]interface{}

// Document is a container around ID and it's associated Properties
type Document = struct {
	ID
	Fields
}

type properties map[string][]quad.Value

// newPath for given quad store and IDs returns path to get the data required for the search
func newPath(qs graph.QuadStore, ids []quad.IRI) *path.Path {
	var values []quad.Value
	for _, iri := range ids {
		values = append(values, iri)
	}
	return path.StartPath(qs, values...).OutWithTags([]string{"key"}, path.StartPath(qs)).Tag("value").Back("")
}

// getDocuments for given IDs reterives documents from the graph
func getDocuments(ctx context.Context, qs graph.QuadStore, ids []quad.IRI) ([]Document, error) {
	p := newPath(qs, ids)
	scanner := p.BuildIterator(ctx).Iterate()
	idToFields := make(map[quad.IRI]properties)
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
		fields, ok := idToFields[iri]
		if !ok {
			fields = make(properties)
			idToFields[iri] = fields
		}
		keyIRI, ok := qs.NameOf(tags["key"]).(quad.IRI)
		if !ok {
			continue
		}
		key := string(keyIRI)
		value := qs.NameOf(tags["value"])
		fields[key] = append(fields[key], value)
	}
	err := scanner.Close()
	if err != nil {
		return nil, err
	}
	var documents []Document
	for iri, properties := range idToFields {
		f := make(Fields)
		for property, values := range properties {
			var nativeValues []interface{}
			for _, value := range values {
				nativeValues = append(nativeValues, value.Native())
			}
			f[property] = interface{}(nativeValues)
		}
		document := Document{
			ID:     iri,
			Fields: f,
		}
		documents = append(documents, document)
	}
	return documents, nil
}

// OpenIndex opens an existing index
func OpenIndex() (bleve.Index, error) {
	return bleve.Open(IndexPath)
}

func newIndexMapping() mapping.IndexMapping {
	mapping := bleve.NewIndexMapping()
	documentMapping := bleve.NewDocumentMapping()
	mapping.AddDocumentMapping("document", documentMapping)
	nameFieldMapping := bleve.NewTextFieldMapping()
	nameFieldMapping.Analyzer = simple.Name
	documentMapping.AddFieldMappingsAt("name", nameFieldMapping)
	return mapping
}

// NewIndex builds a new search index
func NewIndex(ctx context.Context, qs graph.QuadStore) (bleve.Index, error) {
	mapping := newIndexMapping()
	clog.Infof("Building search index...")
	index, err := bleve.New(IndexPath, mapping)
	if err != nil {
		return nil, err
	}
	clog.Infof("Retreiving documents...")
	documents, err := getDocuments(ctx, qs, []quad.IRI{})
	for _, document := range documents {
		index.Index(string(document.ID), document.Fields)
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

// ClearIndex removes the existing index
func ClearIndex() error {
	err := os.RemoveAll(IndexPath)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

// Search for given index and query creates a search request and translates the results to Documents
func Search(index bleve.Index, query string) ([]Document, error) {
	matchQuery := bleve.NewMatchQuery(query)
	search := bleve.NewSearchRequest(matchQuery)
	searchResults, err := index.Search(search)
	if err != nil {
		return nil, err
	}
	var documents []Document
	for _, hit := range searchResults.Hits {
		documents = append(documents, Document{
			ID:     quad.IRI(hit.ID),
			Fields: hit.Fields,
		})
	}
	return documents, nil
}
