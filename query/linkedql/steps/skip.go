package steps

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query/linkedql"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad/voc"
)

func init() {
	linkedql.Register(&Skip{})
}

var _ linkedql.PathStep = (*Skip)(nil)

// Skip corresponds to .skip().
type Skip struct {
	From   linkedql.PathStep `json:"from"`
	Offset int64             `json:"offset"`
}

// Description implements Step.
func (s *Skip) Description() string {
	return "skips a number of nodes for current path."
}

// BuildPath implements linkedql.PathStep.
func (s *Skip) BuildPath(qs graph.QuadStore, ns *voc.Namespaces) (*path.Path, error) {
	fromPath, err := linkedql.BuildFromPath(qs, ns, s.From)
	if err != nil {
		return nil, err
	}
	return fromPath.Skip(s.Offset), nil
}
