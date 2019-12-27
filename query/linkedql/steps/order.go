package steps

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query"
	"github.com/cayleygraph/cayley/query/linkedql"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad/voc"
)

func init() {
	linkedql.Register(&Order{})
}

var _ linkedql.IteratorStep = (*Order)(nil)
var _ linkedql.PathStep = (*Order)(nil)

// Order corresponds to .order().
type Order struct {
	From linkedql.PathStep `json:"from"`
}

// Description implements Step.
func (s *Order) Description() string {
	return "sorts the results in ascending order according to the current entity / value"
}

// BuildIterator implements linkedql.IteratorStep.
func (s *Order) BuildIterator(qs graph.QuadStore, ns *voc.Namespaces) (query.Iterator, error) {
	return linkedql.NewValueIteratorFromPathStep(s, qs, ns)
}

// BuildPath implements linkedql.PathStep.
func (s *Order) BuildPath(qs graph.QuadStore, ns *voc.Namespaces) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs, ns)
	if err != nil {
		return nil, err
	}
	return fromPath.Order(), nil
}
