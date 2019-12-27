package steps

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/voc"
	"github.com/cayleygraph/cayley/query/linkedql"
)

func init() {
	linkedql.Register(&VisitReverse{})
}

var _ linkedql.IteratorStep = (*VisitReverse)(nil)
var _ linkedql.PathStep = (*VisitReverse)(nil)

// VisitReverse corresponds to .viewReverse().
type VisitReverse struct {
	From       linkedql.PathStep     `json:"from"`
	Properties linkedql.PropertyPath `json:"properties"`
}

// Type implements Step.
func (s *VisitReverse) Type() quad.IRI {
	return linkedql.Prefix + "VisitReverse"
}

// Description implements Step.
func (s *VisitReverse) Description() string {
	return "is the inverse of View. Starting with the nodes in `path` on the object, follow the quads with predicates defined by `predicatePath` to their subjects."
}

// BuildIterator implements linkedql.IteratorStep.
func (s *VisitReverse) BuildIterator(qs graph.QuadStore, ns *voc.Namespaces) (query.Iterator, error) {
	return linkedql.NewValueIteratorFromPathStep(s, qs, ns)
}

// BuildPath implements linkedql.PathStep.
func (s *VisitReverse) BuildPath(qs graph.QuadStore, ns *voc.Namespaces) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs, ns)
	if err != nil {
		return nil, err
	}
	viaPath, err := s.Properties.BuildPath(qs, ns)
	if err != nil {
		return nil, err
	}
	return fromPath.In(viaPath), nil
}
