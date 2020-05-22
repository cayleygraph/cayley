package steps

import (
	"fmt"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query/linkedql"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/jsonld"
	"github.com/cayleygraph/quad/voc"
)

func init() {
	linkedql.Register(&Match{})
}

var _ linkedql.PathStep = (*Match)(nil)

// Match corresponds to .has().
type Match struct {
	From    linkedql.PathStep     `json:"from" minCardinality:"0"`
	Pattern linkedql.GraphPattern `json:"pattern"`
}

// Description implements Step.
func (s *Match) Description() string {
	return "filters all paths which are, at this point, on the subject for the given predicate and object, but do not follow the path, merely filter the possible paths. Usually useful for starting with all nodes, or limiting to a subset depending on some predicate/value pair."
}

// BuildPath implements linkedql.PathStep.
func (s *Match) BuildPath(qs graph.QuadStore, ns *voc.Namespaces) (*path.Path, error) {
	var p *path.Path
	if s.From != nil {
		fromPath, err := s.From.BuildPath(qs, ns)
		if err != nil {
			return nil, err
		}
		p = fromPath
	} else {
		p = path.StartPath(qs)
	}

	// Get quads
	quads, err := parsePattern(s.Pattern, ns)

	if err != nil {
		return nil, err
	}

	return p.Follow(buildPatternPath(quads, ns)), nil
}

type entityProperties = map[quad.Value]map[quad.Value][]quad.Value

func entityPropertiesToPath(entity quad.Value, entities entityProperties) *path.Path {
	p := path.StartMorphism()
	if iri, ok := entity.(quad.IRI); ok {
		p = p.Is(iri)
	}
	for property, values := range entities[entity] {
		for _, value := range values {
			if _, ok := value.(quad.BNode); ok {
				p = p.Out(property).Follow(entityPropertiesToPath(value, entities)).Back("")
			} else {
				p = p.Has(property, value)
			}
		}
	}
	return p
}

func groupEntities(pattern []quad.Quad, ns *voc.Namespaces) (entityProperties, map[quad.Value]struct{}) {
	// referenced holds entities used as values for properties of other entities
	referenced := make(map[quad.Value]struct{})

	entities := make(entityProperties)

	for _, q := range pattern {
		entity := linkedql.AbsoluteValue(q.Subject, ns)
		property := linkedql.AbsoluteValue(q.Predicate, ns)
		value := linkedql.AbsoluteValue(q.Object, ns)

		if _, ok := value.(quad.BNode); ok {
			referenced[value] = struct{}{}
		}

		properties, ok := entities[entity]
		if !ok {
			properties = make(map[quad.Value][]quad.Value)
			entities[entity] = properties
		}
		if linkedql.IsSingleEntityQuad(q) {
			continue
		}
		properties[property] = append(properties[property], value)
	}

	return entities, referenced
}

// buildPath for given pattern constructs a Path object
func buildPatternPath(pattern []quad.Quad, ns *voc.Namespaces) *path.Path {
	entities, referenced := groupEntities(pattern, ns)
	p := path.StartMorphism()

	for entity := range entities {
		// Apply only on entities which are not referenced as values
		if _, ok := referenced[entity]; !ok {
			p = p.Follow(entityPropertiesToPath(entity, entities))
		}
	}

	return p
}

func contextualizePattern(pattern linkedql.GraphPattern, ns *voc.Namespaces) linkedql.GraphPattern {
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
	return pattern
}

func quadsFromMap(o interface{}) ([]quad.Quad, error) {
	reader := jsonld.NewReaderFromMap(o)
	return quad.ReadAll(reader)
}

func normalizeQuads(quads []quad.Quad, pattern linkedql.GraphPattern) ([]quad.Quad, error) {
	if id, ok := pattern["@id"]; ok && len(quads) == 0 {
		idString, ok := id.(string)
		if !ok {
			return nil, fmt.Errorf("Unexpected type for @id %T", idString)
		}
		quads = append(quads, linkedql.MakeSingleEntityQuad(quad.IRI(idString)))
	}
	return quads, nil
}

func parsePattern(pattern linkedql.GraphPattern, ns *voc.Namespaces) ([]quad.Quad, error) {
	contextualizedPattern := contextualizePattern(pattern, ns)
	quads, err := quadsFromMap(contextualizedPattern)
	if err != nil {
		return nil, err
	}
	quads, err = normalizeQuads(quads, contextualizedPattern)
	if err != nil {
		return nil, err
	}
	if len(quads) == 0 && len(pattern) != 0 {
		return nil, fmt.Errorf("Pattern does not parse to any quad. `{}` is the only pattern allowed to not parse to any quad")
	}
	return quads, nil
}
