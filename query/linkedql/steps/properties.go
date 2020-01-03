package steps

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query"
	"github.com/cayleygraph/cayley/query/linkedql"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/voc"
)

func init() {
	linkedql.Register(&Properties{})
}

var _ linkedql.IteratorStep = (*Properties)(nil)
var _ linkedql.PathStep = (*Properties)(nil)

// Properties corresponds to .properties().
type Properties struct {
	From linkedql.PathStep `json:"from"`
	// TODO(iddan): Use linkedql.PropertyPath
	Names []quad.IRI `json:"names"`
}

// Description implements Step.
func (s *Properties) Description() string {
	return "adds tags for all properties of the current entity"
}

// BuildIterator implements linkedql.IteratorStep.
// TODO(iddan): Default tag to Via.
func (s *Properties) BuildIterator(qs graph.QuadStore, ns *voc.Namespaces) (query.Iterator, error) {
	return linkedql.NewValueIteratorFromPathStep(s, qs, ns)
}

// BuildPath implements linkedql.PathStep.
func (s *Properties) BuildPath(qs graph.QuadStore, ns *voc.Namespaces) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs, ns)
	if err != nil {
		return nil, err
	}
	p := fromPath
	if s.Names != nil {
		for _, name := range s.Names {
			name = name.FullWith(ns)
			tag := string(name)
			p = p.Save(name, tag)
		}
	} else {
		panic("Not implemented: should tag all properties")
	}
	return p, nil
}
