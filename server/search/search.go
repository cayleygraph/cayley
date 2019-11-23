package search

import (
	"context"
	"os"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/mapping"
	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/refs"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad"
)

// IndexPath is the path to the directory the search index will be stored at
const IndexPath = "searchIndex.bleve"

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

type data map[string]interface{}

type document struct {
	id   string
	data data
}

// TODO(iddan): support more types
func parseValue(value quad.Value) interface{} {
	stringValue, ok := value.Native().(string)
	if !ok {
		return nil
	}
	return stringValue
}

// getDocuments for given IDs reterives documents from the graph
func getDocuments(ctx context.Context, qs graph.QuadStore, config IndexConfig) ([]document, error) {
	p := newPath(qs, config)
	scanner := p.BuildIterator(ctx).Iterate()
	idToData := make(map[string]data)
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
		id := string(iri)
		fields, ok := idToData[id]
		if !ok {
			fields = data{
				"_type": config.Name,
			}
			idToData[id] = fields
		}
		for key, ref := range tags {
			value := qs.NameOf(ref)
			current := fields[key]
			var values []interface{}
			if current == nil {
				fields[key] = values
			} else {
				values = current.([]interface{})
			}
			parsed := parseValue(value)
			if parsed == nil {
				continue
			}
			fields[key] = append(values, parsed)
		}

	}
	err := scanner.Close()
	if err != nil {
		return nil, err
	}
	var documents []document
	for id, d := range idToData {
		documents = append(documents, document{id: id, data: d})
	}
	return documents, nil
}

// OpenIndex opens an existing index
func OpenIndex() (bleve.Index, error) {
	return bleve.Open(IndexPath)
}

func newIndexMapping(configs []IndexConfig) *mapping.IndexMappingImpl {
	indexMapping := bleve.NewIndexMapping()
	for _, config := range configs {
		documentMapping := bleve.NewDocumentMapping()
		for _, property := range config.Properties {
			documentMapping.AddFieldMappingsAt(string(property), bleve.NewTextFieldMapping())
		}
		indexMapping.AddDocumentMapping("people", documentMapping)
	}
	// Disable default mapping as mapping is done manually.
	indexMapping.DefaultMapping = bleve.NewDocumentDisabledMapping()
	indexMapping.StoreDynamic = false
	indexMapping.DocValuesDynamic = false
	indexMapping.IndexDynamic = false
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
	batch := index.NewBatch()
	for _, config := range configs {
		clog.Infof("Retreiving for \"%s\" documents...", config.Name)
		documents, err := getDocuments(ctx, qs, config)
		if err != nil {
			return nil, err
		}
		for _, document := range documents {
			id := document.id
			batch.Index(id, document.data)
			// batch.Index(document.ID, document)
			// batch.IndexAdvanced(document)
		}
		clog.Infof("Retrieved %v documents for \"%s\"", len(documents), config.Name)
	}
	index.Batch(batch)
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

func toIRIs(searchResults *bleve.SearchResult) []quad.IRI {
	var results []quad.IRI
	for _, hit := range searchResults.Hits {
		results = append(results, quad.IRI(hit.ID))
	}
	return results
}

// Search for given index and query creates a search request and translates the results to Documents
func Search(index bleve.Index, query string) ([]quad.IRI, error) {
	matchQuery := bleve.NewMatchQuery(query)
	search := bleve.NewSearchRequest(matchQuery)
	searchResults, err := index.Search(search)
	if err != nil {
		return nil, err
	}
	return toIRIs(searchResults), nil
}
