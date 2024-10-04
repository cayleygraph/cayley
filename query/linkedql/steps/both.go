package steps

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query/linkedql"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad/voc"
)

func init() {
	linkedql.Register(&Both{})
}

var _ linkedql.PathStep = (*Both)(nil)

// Both corresponds to .both().
type Both struct {
	From       linkedql.PathStep      `json:"from" minCardinality:"0"`
	Properties *linkedql.PropertyPath `json:"properties"`
}

// Description implements Step.
func (s *Both) Description() string {
	return "is like View but resolves to both the object values and references to the values of the given properties in via. It is the equivalent for the Union of View and ViewReverse of the same property."
}

// BuildPath implements linkedql.PathStep.
func (s *Both) BuildPath(qs graph.QuadStore, ns *voc.Namespaces) (*path.Path, error) {
	fromPath, err := linkedql.BuildFromPath(qs, ns, s.From)
	if err != nil {
		return nil, err
	}
	viaPath, err := s.Properties.BuildPath(qs, ns)
	if err != nil {
		return nil, err
	}
	return fromPath.Both(viaPath), nil
}
