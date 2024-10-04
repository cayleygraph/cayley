package steps

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query/linkedql"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad/voc"
)

func init() {
	linkedql.Register(&PropertyNames{})
}

var _ linkedql.PathStep = (*PropertyNames)(nil)

// PropertyNames corresponds to .propertyNames().
type PropertyNames struct {
	From linkedql.PathStep `json:"from" minCardinality:"0"`
}

// Description implements Step.
func (s *PropertyNames) Description() string {
	return "gets the list of predicates that are pointing out from a node."
}

// BuildPath implements linkedql.PathStep.
func (s *PropertyNames) BuildPath(qs graph.QuadStore, ns *voc.Namespaces) (*path.Path, error) {
	fromPath, err := linkedql.BuildFromPath(qs, ns, s.From)
	if err != nil {
		return nil, err
	}
	return fromPath.OutPredicates(), nil
}
