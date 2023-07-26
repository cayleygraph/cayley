package steps

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query/linkedql"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad/voc"
)

func init() {
	linkedql.Register(&VisitReverse{})
}

var _ linkedql.PathStep = (*VisitReverse)(nil)

// VisitReverse corresponds to .viewReverse().
type VisitReverse struct {
	From       linkedql.PathStep      `json:"from" minCardinality:"0"`
	Properties *linkedql.PropertyPath `json:"properties"`
}

// Description implements Step.
func (s *VisitReverse) Description() string {
	return "is the inverse of View. Starting with the nodes in `path` on the object, follow the quads with predicates defined by `predicatePath` to their subjects."
}

// BuildPath implements linkedql.PathStep.
func (s *VisitReverse) BuildPath(qs graph.QuadStore, ns *voc.Namespaces) (*path.Path, error) {
	fromPath, err := linkedql.BuildFromPath(qs, ns, s.From)
	if err != nil {
		return nil, err
	}
	viaPath, err := s.Properties.BuildPath(qs, ns)
	if err != nil {
		return nil, err
	}
	return fromPath.In(viaPath), nil
}
