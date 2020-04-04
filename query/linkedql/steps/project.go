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
	linkedql.Register(&Project{})
}

var _ linkedql.IteratorStep = (*Project)(nil)
var _ linkedql.PathStep = (*Project)(nil)

// Project corresponds to .project().
type Project struct {
	From linkedql.PathStep `json:"from"`
	Name quad.IRI          `json:"name"`
	Step linkedql.PathStep `json:"step"`
}

// Description implements Step.
func (s *Project) Description() string {
	return "Sets the result of step to name"
}

// BuildIterator implements linkedql.IteratorStep.
func (s *Project) BuildIterator(qs graph.QuadStore, ns *voc.Namespaces) (query.Iterator, error) {
	return linkedql.NewValueIteratorFromPathStep(s, qs, ns)
}

// BuildPath implements linkedql.PathStep.
func (s *Project) BuildPath(qs graph.QuadStore, ns *voc.Namespaces) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs, ns)
	if err != nil {
		return nil, err
	}
	step, err := s.Step.BuildPath(qs, ns)
	if err != nil {
		return nil, err
	}
	return fromPath.Follow(step).Tag(string(s.Name)).Back(""), nil
}
