package steps

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query/linkedql"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad/voc"
)

func init() {
	linkedql.Register(&PropertyNamesAs{})
}

var _ linkedql.PathStep = (*PropertyNamesAs)(nil)

// PropertyNamesAs corresponds to .propertyNamesAs().
type PropertyNamesAs struct {
	From linkedql.PathStep `json:"from"`
	Tag  string            `json:"tag"`
}

// Description implements Step.
func (s *PropertyNamesAs) Description() string {
	return "tags the list of predicates that are pointing out from a node."
}

// BuildPath implements linkedql.PathStep.
func (s *PropertyNamesAs) BuildPath(qs graph.QuadStore, ns *voc.Namespaces) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs, ns)
	if err != nil {
		return nil, err
	}
	return fromPath.SavePredicates(false, s.Tag), nil
}
