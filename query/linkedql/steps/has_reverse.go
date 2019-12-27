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
	linkedql.Register(&HasReverse{})
}

var _ linkedql.IteratorStep = (*HasReverse)(nil)
var _ linkedql.PathStep = (*HasReverse)(nil)

// HasReverse corresponds to .hasR().
type HasReverse struct {
	From     linkedql.PathStep     `json:"from"`
	Property linkedql.PropertyPath `json:"property"`
	Values   []quad.Value `json:"values"`
}

// Type implements Step.
func (s *HasReverse) Type() quad.IRI {
	return linkedql.Prefix + "HasReverse"
}

// Description implements Step.
func (s *HasReverse) Description() string {
	return "is the same as Has, but sets constraint in reverse direction."
}

// BuildIterator implements linkedql.IteratorStep.
func (s *HasReverse) BuildIterator(qs graph.QuadStore, ns *voc.Namespaces) (query.Iterator, error) {
	return linkedql.NewValueIteratorFromPathStep(s, qs, ns)
}

// BuildPath implements linkedql.PathStep.
func (s *HasReverse) BuildPath(qs graph.QuadStore, ns *voc.Namespaces) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs, ns)
	if err != nil {
		return nil, err
	}
	viaPath, err := s.Property.BuildPath(qs, ns)
	if err != nil {
		return nil, err
	}
	return fromPath.HasReverse(viaPath, s.Values...), nil
}

