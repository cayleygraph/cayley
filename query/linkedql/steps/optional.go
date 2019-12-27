package steps

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/voc"
	"github.com/cayleygraph/cayley/query/linkedql"
)

func init() {
	linkedql.Register(&Optional{})
}

var _ linkedql.IteratorStep = (*Optional)(nil)
var _ linkedql.PathStep = (*Optional)(nil)

// Optional corresponds to .optional().
type Optional struct {
	From linkedql.PathStep `json:"from"`
	Step linkedql.PathStep `json:"step"`
}

// Type implements Step.
func (s *Optional) Type() quad.IRI {
	return linkedql.Prefix + "Optional"
}

// Description implements Step.
func (s *Optional) Description() string {
	return "attempts to follow the given path from the current entity / value, if fails the entity / value will still be kept in the results"
}

// BuildIterator implements linkedql.IteratorStep.
func (s *Optional) BuildIterator(qs graph.QuadStore, ns *voc.Namespaces) (query.Iterator, error) {
	return linkedql.NewValueIteratorFromPathStep(s, qs, ns)
}

// BuildPath implements linkedql.PathStep.
func (s *Optional) BuildPath(qs graph.QuadStore, ns *voc.Namespaces) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs, ns)
	if err != nil {
		return nil, err
	}
	p, err := s.Step.BuildPath(qs, ns)
	if err != nil {
		return nil, err
	}
	return fromPath.Optional(p), nil
}

