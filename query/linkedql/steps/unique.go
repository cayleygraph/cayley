package steps

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query"
	"github.com/cayleygraph/cayley/query/linkedql"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad/voc"
)

func init() {
	linkedql.Register(&Unique{})
}

var _ linkedql.IteratorStep = (*Unique)(nil)
var _ linkedql.PathStep = (*Unique)(nil)

// Unique corresponds to .unique().
type Unique struct {
	From linkedql.PathStep `json:"from"`
}

// Description implements Step.
func (s *Unique) Description() string {
	return "removes duplicate values from the path."
}

// BuildIterator implements linkedql.IteratorStep.
func (s *Unique) BuildIterator(qs graph.QuadStore, ns *voc.Namespaces) (query.Iterator, error) {
	return linkedql.NewValueIteratorFromPathStep(s, qs, ns)
}

// BuildPath implements linkedql.PathStep.
func (s *Unique) BuildPath(qs graph.QuadStore, ns *voc.Namespaces) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs, ns)
	if err != nil {
		return nil, err
	}
	return fromPath.Unique(), nil
}
