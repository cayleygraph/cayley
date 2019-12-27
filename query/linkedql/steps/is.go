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
	linkedql.Register(&Is{})
}

var _ linkedql.IteratorStep = (*Is)(nil)
var _ linkedql.PathStep = (*Is)(nil)

// Is corresponds to .back().
type Is struct {
	From   linkedql.PathStep     `json:"from"`
	Values []quad.Value `json:"values"`
}

// Type implements Step.
func (s *Is) Type() quad.IRI {
	return linkedql.Prefix + "Is"
}

// Description implements Step.
func (s *Is) Description() string {
	return "resolves to all the values resolved by the from step which are included in provided values."
}

// BuildIterator implements linkedql.IteratorStep.
func (s *Is) BuildIterator(qs graph.QuadStore, ns *voc.Namespaces) (query.Iterator, error) {
	return linkedql.NewValueIteratorFromPathStep(s, qs, ns)
}

// BuildPath implements linkedql.PathStep.
func (s *Is) BuildPath(qs graph.QuadStore, ns *voc.Namespaces) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs, ns)
	if err != nil {
		return nil, err
	}
	return fromPath.Is(s.Values...), nil
}

