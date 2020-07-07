package steps

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query/linkedql"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad/voc"
)

func init() {
	linkedql.Register(&Union{})
}

var _ linkedql.PathStep = (*Union)(nil)

// Union corresponds to .union() and .or().
type Union struct {
	From  linkedql.PathStep   `json:"from" minCardinality:"0"`
	Steps []linkedql.PathStep `json:"steps"`
}

// Description implements Step.
func (s *Union) Description() string {
	return "returns the combined paths of the two queries. Notice that it's per-path, not per-node. Once again, if multiple paths reach the same destination, they might have had different ways of getting there (and different tags)."
}

// BuildPath implements linkedql.PathStep.
func (s *Union) BuildPath(qs graph.QuadStore, ns *voc.Namespaces) (*path.Path, error) {
	fromPath, err := linkedql.BuildFromPath(qs, ns, s.From)
	if err != nil {
		return nil, err
	}
	p := fromPath
	for _, step := range s.Steps {
		valuePath, err := step.BuildPath(qs, ns)
		if err != nil {
			return nil, err
		}
		p = p.Or(valuePath)
	}
	return p, nil
}
