package steps

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query"
	"github.com/cayleygraph/cayley/query/linkedql"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad/voc"
)

func init() {
	linkedql.Register(&Count{})
}

var _ linkedql.IteratorStep = (*Count)(nil)
var _ linkedql.PathStep = (*Count)(nil)

// Count corresponds to .count().
type Count struct {
	From linkedql.PathStep `json:"from"`
}

// Description implements Step.
func (s *Count) Description() string {
	return "resolves to the number of the resolved values of the from step"
}

// BuildIterator implements linkedql.IteratorStep.
func (s *Count) BuildIterator(qs graph.QuadStore, ns *voc.Namespaces) (query.Iterator, error) {
	return linkedql.NewValueIteratorFromPathStep(s, qs, ns)
}

// BuildPath implements linkedql.PathStep.
func (s *Count) BuildPath(qs graph.QuadStore, ns *voc.Namespaces) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs, ns)
	if err != nil {
		return nil, err
	}
	return fromPath.Count(), nil
}
