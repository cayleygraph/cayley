package steps

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query"
	"github.com/cayleygraph/cayley/query/linkedql"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad"
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
	From    linkedql.PathStep `json:"from"`
	Pattern []quad.Quad       `json:"pattern"`
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

	// Group quads to subtrees
	entities := make(map[quad.Value]map[quad.Value][]quad.Value)
	for _, q := range s.Pattern {
		entity := linkedql.AbsoluteValue(q.Subject, ns)
		property := linkedql.AbsoluteValue(q.Predicate, ns)
		value := linkedql.AbsoluteValue(q.Object, ns)
		var properties map[quad.Value][]quad.Value
		properties, ok := entities[entity]
		if !ok {
			properties = make(map[quad.Value][]quad.Value)
			entities[entity] = properties
		}
		// rdf:type rdfs:Resource is always true but not expressed in the graph.
		// it is used to specify an entity without specifying a property.
		if property == quad.IRI(rdf.Type) && value == quad.IRI(rdfs.Resource) {
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
