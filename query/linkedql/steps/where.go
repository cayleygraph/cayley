package steps

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query/linkedql"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad/voc"
)

func init() {
	linkedql.Register(&Where{})
}

var _ linkedql.PathStep = (*Where)(nil)

// Where corresponds to .where().
type Where struct {
	From      linkedql.PathStep `json:"from"`
	Condition linkedql.PathStep `json:"condition"`
}

// Description implements Step.
func (s *Where) Description() string {
	return "filters results that fulfill a specified condition"
}

// BuildPath implements linkedql.PathStep.
func (s *Where) BuildPath(qs graph.QuadStore, ns *voc.Namespaces) (*path.Path, error) {
	fromPath, err := linkedql.BuildFromPath(qs, ns, s.From)
	if err != nil {
		return nil, err
	}
	stepPath, err := s.Condition.BuildPath(qs, ns)
	if err != nil {
		return nil, err
	}
	return fromPath.And(stepPath.Reverse()), nil
}
