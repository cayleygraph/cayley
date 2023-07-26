package steps

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query/linkedql"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad/voc"
)

func init() {
	linkedql.Register(&Labels{})
}

var _ linkedql.PathStep = (*Labels)(nil)

// Labels corresponds to .labels().
type Labels struct {
	From linkedql.PathStep `json:"from" minCardinality:"0"`
}

// Description implements Step.
func (s *Labels) Description() string {
	return "gets the list of inbound and outbound quad labels"
}

// BuildPath implements linkedql.PathStep.
func (s *Labels) BuildPath(qs graph.QuadStore, ns *voc.Namespaces) (*path.Path, error) {
	fromPath, err := linkedql.BuildFromPath(qs, ns, s.From)
	if err != nil {
		return nil, err
	}
	return fromPath.Labels(), nil
}
