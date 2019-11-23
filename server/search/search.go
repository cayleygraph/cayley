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

type properties map[string][]quad.Value

// Document is a container around ID and it's associated Properties
type Document = struct {
	ID
	Fields
}

func newDocumentFromProperties(id ID, t string, props properties) Document {
	f := Fields{
		"_type": t,
	}
	for property, values := range props {
		if len(values) == 1 {
			f[property] = values[0].Native()
		} else {
			var nativeValues []interface{}
			for _, value := range values {
				nativeValues = append(nativeValues, value.Native())
			}
			f[property] = interface{}(nativeValues)
		}
	}
	return Document{
		ID:     id,
		Fields: f,
	}
}

// IndexConfig specifies a single index type.
// Each Cayley instance can have multiple index configs defined
type IndexConfig struct {
	// TODO(iddan): customize matching
	Name       string
	Properties []quad.IRI
}

// newPath for given quad store and IDs returns path to get the data required for the search
func newPath(qs graph.QuadStore, config IndexConfig) *path.Path {
	p := path.StartPath(qs)
	for _, property := range config.Properties {
		p = p.Save(property, string(property))
	}
	return p
}

// getDocuments for given IDs reterives documents from the graph
func getDocuments(ctx context.Context, qs graph.QuadStore, config IndexConfig) ([]Document, error) {
	p := newPath(qs, config)
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
		for key, ref := range tags {
			value := qs.NameOf(ref)
			fields[key] = append(fields[key], value)
		}

	}
	err := scanner.Close()
	if err != nil {
		return nil, err
	}
	var documents []Document
	for iri, properties := range idToFields {
		documents = append(documents, newDocumentFromProperties(iri, config.Name, properties))
	}
	return documents, nil
}

// OpenIndex opens an existing index
func OpenIndex() (bleve.Index, error) {
	return bleve.Open(IndexPath)
}

func newIndexMapping(configs []IndexConfig) *mapping.IndexMappingImpl {
	indexMapping := bleve.NewIndexMapping()
	// Disable default mapping as all search types are strictly defined
	indexMapping.DefaultMapping = bleve.NewDocumentDisabledMapping()
	// Use simple analyzer by default
	// TODO(iddan): make this configurable
	indexMapping.DefaultAnalyzer = simple.Name
	indexMapping.StoreDynamic = false
	indexMapping.IndexDynamic = false
	indexMapping.DocValuesDynamic = false
	for _, config := range configs {
		// Use static document mapping as fields of the document are predefined
		documentMapping := bleve.NewDocumentStaticMapping()
		indexMapping.AddDocumentMapping(config.Name, documentMapping)
		for _, property := range config.Properties {
			fieldMapping := bleve.NewTextFieldMapping()
			documentMapping.AddFieldMappingsAt(string(property), fieldMapping)
		}
	}
	return indexMapping
}

// NewIndex builds a new search index
func NewIndex(ctx context.Context, qs graph.QuadStore, configs []IndexConfig) (bleve.Index, error) {
	clog.Infof("Building search index...")
	indexMapping := newIndexMapping(configs)
	index, err := bleve.New(IndexPath, indexMapping)
	if err != nil {
		return nil, err
	}
	for _, config := range configs {
		clog.Infof("Retreiving for \"%s\" documents...", config.Name)
		documents, err := getDocuments(ctx, qs, config)
		if err != nil {
			return nil, err
		}
		for _, document := range documents {
			index.Index(string(document.ID), document.Fields)
		}
		clog.Infof("Retrieved %v documents for \"%s\"", len(documents), config.Name)
	}
	clog.Infof("Built search index")
	return index, nil
}

// GetIndex attempts to open an existing index, if it doesn't exist it creates a new one
func GetIndex(ctx context.Context, qs graph.QuadStore, configs []IndexConfig) (bleve.Index, error) {
	index, err := OpenIndex()
	if err == bleve.ErrorIndexPathDoesNotExist {
		return NewIndex(ctx, qs, configs)
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

func searchResultsToDocuments(searchResults *bleve.SearchResult) []Document {
	var documents []Document
	for _, hit := range searchResults.Hits {
		documents = append(documents, Document{
			ID:     quad.IRI(hit.ID),
			Fields: hit.Fields,
		})
	}
	return documents
}

// Search for given index and query creates a search request and translates the results to Documents
func Search(index bleve.Index, query string) ([]Document, error) {
	matchQuery := bleve.NewMatchQuery(query)
	search := bleve.NewSearchRequest(matchQuery)
	searchResults, err := index.Search(search)
	if err != nil {
		return nil, err
	}
	return searchResultsToDocuments(searchResults), nil
}
