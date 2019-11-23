package search

import (
	"context"
	"fmt"
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

// PropertyConfig how to index a property of a matched entity
type PropertyConfig struct {
	Name     quad.IRI
	Type     string
	Analyzer string
}

// documentConfig specifies a single index type.
// Each Cayley instance can have multiple index configs defined
type documentConfig struct {
	// TODO(iddan): customize matching
	Name       string
	Match      map[quad.IRI][]quad.Value
	Properties []PropertyConfig
}

// Configuration specifies the search indexes of a database
type Configuration []documentConfig

// newPath for given quad store and IDs returns path to get the data required for the search
func newPath(qs graph.QuadStore, config documentConfig, entities []quad.Value) *path.Path {
	p := path.StartPath(qs, entities...)
	for predicate, object := range config.Match {
		p = p.Has(predicate, object...)
	}
	for _, property := range config.Properties {
		p = p.Save(property.Name, string(property.Name))
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

func identifierToString(identifier quad.Value) (string, error) {
	switch v := identifier.(type) {
	case quad.IRI:
		return string(v), nil
	case quad.BNode:
		return string(v), nil
	default:
		return "", fmt.Errorf("Given quad.Value is not an identifier")
	}
}

// getDocuments for config reterives documents from the graph
// If provided with entities reterives documents only of given entities
func getDocuments(ctx context.Context, qs graph.QuadStore, config documentConfig, entities []quad.Value) ([]document, error) {
	p := newPath(qs, config, entities)
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
		id, err := identifierToString(name)
		if err != nil {
			continue
		}
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

const defaultFieldType = "string"

func resolveFieldConstructor(t string) (func() *mapping.FieldMapping, error) {
	if t == "" {
		t = defaultFieldType
	}
	switch t {
	case "string":
		return bleve.NewTextFieldMapping, nil
	case "boolean":
		return bleve.NewBooleanFieldMapping, nil
	case "number":
		return bleve.NewNumericFieldMapping, nil
	case "datatime":
		return bleve.NewDateTimeFieldMapping, nil
	case "geopoint":
		return bleve.NewGeoPointFieldMapping, nil
	default:
		return nil, fmt.Errorf("Unknown search document field type \"%v\"", t)
	}
}

func newDocumentMapping(config documentConfig) *mapping.DocumentMapping {
	documentMapping := bleve.NewDocumentMapping()
	for _, property := range config.Properties {
		constructor, err := resolveFieldConstructor(property.Type)
		if err != nil {
			panic(err)
		}
		fieldMapping := constructor()
		if property.Analyzer != "" {
			fieldMapping.Analyzer = property.Analyzer
		}
		documentMapping.AddFieldMappingsAt(string(property.Name), fieldMapping)
	}
	return documentMapping
}

func newIndexMapping(configs []documentConfig) mapping.IndexMapping {
	indexMapping := bleve.NewIndexMapping()
	for _, config := range configs {
		indexMapping.AddDocumentMapping(config.Name, newDocumentMapping(config))
	}
	// Disable default mapping as mapping is done manually.
	indexMapping.DefaultMapping = bleve.NewDocumentDisabledMapping()
	indexMapping.StoreDynamic = false
	indexMapping.DocValuesDynamic = false
	indexMapping.IndexDynamic = false
	return indexMapping
}

// Index documents from given quad store by given configuration to given index
// If entities is empty index all the matching documents in the quad store
// Otherwise only load documents of the provided entities
func Index(ctx context.Context, qs graph.QuadStore, configs []documentConfig, index bleve.Index, entities []quad.Value) error {
	batch := index.NewBatch()
	for _, config := range configs {
		clog.Infof("Retreiving documents for \"%s\"...", config.Name)
		documents, err := getDocuments(ctx, qs, config, nil)
		if err != nil {
			return err
		}
		for _, document := range documents {
			id := document.id
			batch.Index(id, document.data)
		}
		clog.Infof("Retrieved %v documents for \"%s\"", len(documents), config.Name)
	}
	clog.Infof("Indexing %v documents", batch.Size())
	index.Batch(batch)
	return nil
}

// Delete documents from given quad store by given configuration for given index
// If entities is empty delete all matching documents in the quad store
// Otherwise only delete documents of the provided entities
func Delete(ctx context.Context, qs graph.QuadStore, configs []documentConfig, index bleve.Index, entities []quad.Value) error {
	batch := index.NewBatch()
	for _, config := range configs {
		clog.Infof("Retreiving documents for \"%s\"...", config.Name)
		documents, err := getDocuments(ctx, qs, config, nil)
		if err != nil {
			return err
		}
		for _, document := range documents {
			id := document.id
			batch.Delete(id)
		}
		clog.Infof("Retrieved %v documents for \"%s\"", len(documents), config.Name)
	}
	clog.Infof("Deleting %v documents", batch.Size())
	index.Batch(batch)
	return nil
}

// NewIndex builds a new search index
func NewIndex(ctx context.Context, qs graph.QuadStore, configs []documentConfig) (bleve.Index, error) {
	clog.Infof("Building search index...")
	indexMapping := newIndexMapping(configs)
	index, err := bleve.New(IndexPath, indexMapping)
	if err != nil {
		return nil, err
	}
	err = Index(ctx, qs, configs, index, nil)
	if err != nil {
		return nil, err
	}
	clog.Infof("Built search index")
	return index, nil
}

// GetIndex attempts to open an existing index, if it doesn't exist it creates a new one
func GetIndex(ctx context.Context, qs graph.QuadStore, configs []documentConfig) (bleve.Index, error) {
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
