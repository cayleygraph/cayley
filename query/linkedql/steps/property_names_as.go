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
	linkedql.Register(&PropertyNamesAs{})
}

var _ linkedql.IteratorStep = (*PropertyNamesAs)(nil)
var _ linkedql.PathStep = (*PropertyNamesAs)(nil)

// PropertyNamesAs corresponds to .propertyNamesAs().
type PropertyNamesAs struct {
	From linkedql.PathStep `json:"from"`
	Tag  string   `json:"tag"`
}

// Type implements Step.
func (s *PropertyNamesAs) Type() quad.IRI {
	return linkedql.Prefix + "PropertyNamesAs"
}

// Description implements Step.
func (s *PropertyNamesAs) Description() string {
	return "tags the list of predicates that are pointing out from a node."
}

// BuildIterator implements linkedql.IteratorStep.
func (s *PropertyNamesAs) BuildIterator(qs graph.QuadStore, ns *voc.Namespaces) (query.Iterator, error) {
	return linkedql.NewValueIteratorFromPathStep(s, qs, ns)
}

// BuildPath implements linkedql.PathStep.
func (s *PropertyNamesAs) BuildPath(qs graph.QuadStore, ns *voc.Namespaces) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs, ns)
	if err != nil {
		return nil, err
	}
	return fromPath.SavePredicates(false, s.Tag), nil
}
