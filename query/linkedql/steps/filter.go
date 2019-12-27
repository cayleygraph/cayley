package steps

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query"
	"github.com/cayleygraph/cayley/query/linkedql"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad/voc"
)

func init() {
	linkedql.Register(&Filter{})
}

var _ linkedql.IteratorStep = (*Filter)(nil)
var _ linkedql.PathStep = (*Filter)(nil)

// Filter corresponds to filter().
type Filter struct {
	From   linkedql.PathStep `json:"from"`
	Filter linkedql.Operator `json:"filter"`
}

// Description implements Step.
func (s *Filter) Description() string {
	return "applies constraints to a set of nodes. Can be used to filter values by range or match strings."
}

// BuildIterator implements linkedql.IteratorStep.
func (s *Filter) BuildIterator(qs graph.QuadStore, ns *voc.Namespaces) (query.Iterator, error) {
	return linkedql.NewValueIteratorFromPathStep(s, qs, ns)
}

// BuildPath implements linkedql.PathStep.
func (s *Filter) BuildPath(qs graph.QuadStore, ns *voc.Namespaces) (*path.Path, error) {
	fromIt, err := s.From.BuildPath(qs, ns)
	if err != nil {
		return nil, err
	}
	return s.Filter.Apply(fromIt)
}
