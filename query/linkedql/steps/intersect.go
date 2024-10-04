package steps

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query/linkedql"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad/voc"
)

func init() {
	linkedql.Register(&Intersect{})
}

var _ linkedql.PathStep = (*Intersect)(nil)

// Intersect represents .intersect() and .and().
type Intersect struct {
	From  linkedql.PathStep   `json:"from"`
	Steps []linkedql.PathStep `json:"steps"`
}

// Description implements Step.
func (s *Intersect) Description() string {
	return "resolves to all the same values resolved by the from step and the provided steps."
}

// BuildPath implements linkedql.PathStep.
func (s *Intersect) BuildPath(qs graph.QuadStore, ns *voc.Namespaces) (*path.Path, error) {
	fromPath, err := linkedql.BuildFromPath(qs, ns, s.From)
	if err != nil {
		return nil, err
	}
	p := fromPath
	for _, step := range s.Steps {
		stepPath, err := step.BuildPath(qs, ns)
		if err != nil {
			return nil, err
		}
		p = p.And(stepPath)
	}
	return p, nil
}
