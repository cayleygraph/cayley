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
	linkedql.Register(&Where{})
}

var _ linkedql.IteratorStep = (*Where)(nil)
var _ linkedql.PathStep = (*Where)(nil)

// Where corresponds to .where().
type Where struct {
	From  linkedql.PathStep   `json:"from"`
	Steps []linkedql.PathStep `json:"steps"`
}

// Type implements Step.
func (s *Where) Type() quad.IRI {
	return linkedql.Prefix + "Where"
}

// Description implements Step.
func (s *Where) Description() string {
	return "applies each provided step in steps in isolation on from"
}

// BuildIterator implements linkedql.IteratorStep.
func (s *Where) BuildIterator(qs graph.QuadStore, ns *voc.Namespaces) (query.Iterator, error) {
	return linkedql.NewValueIteratorFromPathStep(s, qs, ns)
}

// BuildPath implements linkedql.PathStep.
func (s *Where) BuildPath(qs graph.QuadStore, ns *voc.Namespaces) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs, ns)
	if err != nil {
		return nil, err
	}
	p := fromPath
	for _, step := range s.Steps {
		stepPath, err := step.BuildPath(qs, ns)
		if err != nil {
			return nil, err
		}
		p = p.And(stepPath.Reverse())
	}
	return p, nil
}

