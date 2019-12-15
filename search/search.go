package search

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/mapping"
	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/refs"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad"
)

// Index is the search index
type Index = bleve.Index

type PropertyID quad.IRI

func (p *PropertyID) UnmarshalJSON(data []byte) error {
	var raw struct {
		ID string `json:"@id"`
	}
	err := json.Unmarshal(data, &raw)
	*p += PropertyID(raw.ID)
	return err
}

// PropertyConfig how to index a property of a matched entity
type PropertyConfig struct {
	// Name for the search configuration
	Name string `json:"name"`
	// A map of properties to values that objects need to have to be included for the search document
	Match map[quad.IRI][]quad.Value `json:"match"`
	// Name of the property
	Property PropertyID `json:"property"`
	// Type of the values the property holds. string by default.
	// Valid values: "string", "boolean", "number", "datatime", "geopoint"
	Type string `json:"type"`
	// Analyzer to be used for the property.
	Analyzer string `json:"analyzer"`
}

// Configuration specifies the search indexes of a database
type Configuration []PropertyConfig

// newPath for given quad store and IDs returns path to get the data required for the search
func newPath(qs graph.QuadStore, config PropertyConfig, entities []quad.Value) *path.Path {
	p := path.StartPath(qs, entities...)
	for predicate, object := range config.Match {
		p = p.Has(quad.IRI(predicate), object...)
	}
	p = p.Save(quad.IRI(config.Property), string(config.Property))
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
func getDocuments(ctx context.Context, qs graph.QuadStore, config PropertyConfig, entities []quad.Value) ([]document, error) {
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

func newDocumentMapping(config PropertyConfig) *mapping.DocumentMapping {
	documentMapping := bleve.NewDocumentMapping()
	constructor, err := resolveFieldConstructor(config.Type)
	if err != nil {
		panic(err)
	}
	fieldMapping := constructor()
	if config.Analyzer != "" {
		fieldMapping.Analyzer = config.Analyzer
	}
	documentMapping.AddFieldMappingsAt(string(config.Name), fieldMapping)
	return documentMapping
}

// NewIndexMapping creates an IndexMapping out of given configuration
func NewIndexMapping(configs Configuration) mapping.IndexMapping {
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

// IndexEntities documents from given quad store by given configuration to given index
// If entities is empty index all the matching documents in the quad store
// Otherwise only load documents of the provided entities
func IndexEntities(ctx context.Context, qs graph.QuadStore, configs Configuration, index Index, entities ...quad.Value) error {
	batch := index.NewBatch()
	for _, config := range configs {
		clog.Infof("Retreiving documents for \"%s\"...", config.Name)
		documents, err := getDocuments(ctx, qs, config, entities)
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
func Delete(ctx context.Context, qs graph.QuadStore, configs Configuration, index Index, entities ...quad.Value) error {
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

// InitIndex indexes all existing data
func InitIndex(ctx context.Context, index Index, qs graph.QuadStore, configs Configuration) error {
	clog.Infof("Building search index...")
	err := IndexEntities(ctx, qs, configs, index)
	if err != nil {
		return err
	}
	clog.Infof("Built search index")
	return nil
}

func toIRIs(searchResults *bleve.SearchResult) []quad.IRI {
	var results []quad.IRI
	for _, hit := range searchResults.Hits {
		results = append(results, quad.IRI(hit.ID))
	}
	return results
}

// Search for given index and query creates a search request and translates the results to Documents
func Search(index Index, query string) ([]quad.IRI, error) {
	matchQuery := bleve.NewMatchQuery(query)
	search := bleve.NewSearchRequest(matchQuery)
	searchResults, err := index.Search(search)
	if err != nil {
		return nil, err
	}
	return toIRIs(searchResults), nil
}

// GetConfiguration returns search configuration from graph options
// If configuration is not defined correctly returns an error
// If no configuration is defined returns nil configuration
func GetConfiguration(opt graph.Options) (Configuration, error) {
	var config Configuration
	raw, ok := opt["search"]
	if !ok {
		return config, nil
	}
	marshaled, _ := json.Marshal(raw)
	err := json.Unmarshal(marshaled, &config)
	if err != nil {
		return nil, err
	}
	return config, nil
}
