package steps

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query"
	"github.com/cayleygraph/cayley/query/linkedql"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad/voc"
)

func init() {
	linkedql.Register(&ReversePropertyNames{})
}

var _ linkedql.IteratorStep = (*ReversePropertyNames)(nil)
var _ linkedql.PathStep = (*ReversePropertyNames)(nil)

// ReversePropertyNames corresponds to .reversePropertyNames().
type ReversePropertyNames struct {
	From linkedql.PathStep `json:"from"`
}

// Description implements Step.
func (s *ReversePropertyNames) Description() string {
	return "gets the list of predicates that are pointing in to a node."
}

// BuildIterator implements linkedql.IteratorStep.
func (s *ReversePropertyNames) BuildIterator(qs graph.QuadStore, ns *voc.Namespaces) (query.Iterator, error) {
	return linkedql.NewValueIteratorFromPathStep(s, qs, ns)
}

// BuildPath implements linkedql.PathStep.
func (s *ReversePropertyNames) BuildPath(qs graph.QuadStore, ns *voc.Namespaces) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs, ns)
	if err != nil {
		return nil, err
	}
	return fromPath.InPredicates(), nil
}
