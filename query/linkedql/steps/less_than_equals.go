package steps

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/query/linkedql"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/voc"
)

func init() {
	linkedql.Register(&LessThanEquals{})
}

var _ linkedql.PathStep = (*LessThanEquals)(nil)

// LessThanEquals corresponds to lte().
type LessThanEquals struct {
	From  linkedql.PathStep `json:"from" minCardinality:"0"`
	Value quad.Value        `json:"value"`
}

// Description implements Step.
func (s *LessThanEquals) Description() string {
	return "Less than equals filters out values that are not less than or equal given value"
}

// BuildPath implements linkedql.PathStep.
func (s *LessThanEquals) BuildPath(qs graph.QuadStore, ns *voc.Namespaces) (*path.Path, error) {
	fromPath, err := linkedql.BuildFromPath(qs, ns, s.From)
	if err != nil {
		return nil, err
	}
	return fromPath.Filter(iterator.CompareLTE, linkedql.AbsoluteValue(s.Value, ns)), nil
}
