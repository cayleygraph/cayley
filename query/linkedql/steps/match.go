package steps

import (
	"fmt"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query"
	"github.com/cayleygraph/cayley/query/linkedql"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/jsonld"
	"github.com/cayleygraph/quad/voc"
	"github.com/cayleygraph/quad/voc/rdf"
	"github.com/cayleygraph/quad/voc/rdfs"
)

func init() {
	linkedql.Register(&Match{})
}

var _ linkedql.IteratorStep = (*Match)(nil)
var _ linkedql.PathStep = (*Match)(nil)

// Match corresponds to .has().
type Match struct {
	From    linkedql.PathStep     `json:"from"`
	Pattern linkedql.GraphPattern `json:"pattern"`
}

// Description implements Step.
func (s *Match) Description() string {
	return "filters all paths which are, at this point, on the subject for the given predicate and object, but do not follow the path, merely filter the possible paths. Usually useful for starting with all nodes, or limiting to a subset depending on some predicate/value pair."
}

// BuildIterator implements linkedql.IteratorStep.
func (s *Match) BuildIterator(qs graph.QuadStore, ns *voc.Namespaces) (query.Iterator, error) {
	return linkedql.NewValueIteratorFromPathStep(s, qs, ns)
}

// BuildPath implements linkedql.PathStep.
func (s *Match) BuildPath(qs graph.QuadStore, ns *voc.Namespaces) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs, ns)
	if err != nil {
		return nil, err
	}
	path := fromPath

	// Get quads
	quads, err := parsePattern(s.Pattern, ns)

	if err != nil {
		return nil, err
	}

	// Group quads to subtrees
	entities := make(map[quad.Value]map[quad.Value][]quad.Value)
	for _, q := range quads {
		entity := linkedql.AbsoluteValue(q.Subject, ns)
		property := linkedql.AbsoluteValue(q.Predicate, ns)
		value := linkedql.AbsoluteValue(q.Object, ns)
		var properties map[quad.Value][]quad.Value
		properties, ok := entities[entity]
		if !ok {
			properties = make(map[quad.Value][]quad.Value)
			entities[entity] = properties
		}
		if isSingleEntityQuad(q) {
			continue
		}
		values, _ := properties[property]
		properties[property] = append(values, value)
	}

	for entity, properties := range entities {
		if iri, ok := entity.(quad.IRI); ok {
			path = path.Is(iri)
		}
		for property, values := range properties {
			path = path.Has(property, values...)
		}
	}

	return path, nil
}

func parsePattern(pattern linkedql.GraphPattern, ns *voc.Namespaces) ([]quad.Quad, error) {
	context := make(map[string]interface{})
	for _, namespace := range ns.List() {
		context[namespace.Prefix] = namespace.Full
	}
	patternClone := linkedql.GraphPattern{
		"@context": context,
	}
	for key, value := range pattern {
		patternClone[key] = value
	}
	reader := jsonld.NewReaderFromMap(patternClone)
	quads, err := quad.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	if len(quads) == 0 && len(pattern) != 0 {
		return nil, fmt.Errorf("Pattern does not parse to any quad. `{}` is the only pattern allowed to not parse to any quad")
	}
	if id, ok := patternClone["@id"]; ok && len(quads) == 0 {
		idString, ok := id.(string)
		if !ok {
			return nil, fmt.Errorf("Unexpected type for @id %T", idString)
		}
		quads = append(quads, makeSingleEntityQuad(quad.IRI(idString)))
	}
	return quads, nil
}

func makeSingleEntityQuad(id quad.IRI) quad.Quad {
	return quad.Quad{Subject: id, Predicate: quad.IRI(rdf.Type), Object: quad.IRI(rdfs.Resource)}
}

func isSingleEntityQuad(q quad.Quad) bool {
	// rdf:type rdfs:Resource is always true but not expressed in the graph.
	// it is used to specify an entity without specifying a property.
	return q.Predicate == quad.IRI(rdf.Type) && q.Object == quad.IRI(rdfs.Resource)
}
