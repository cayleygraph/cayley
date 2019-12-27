package steps

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query"
	"github.com/cayleygraph/cayley/query/linkedql"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/voc"
)

func init() {
	linkedql.Register(&Labels{})
}

var _ linkedql.IteratorStep = (*Labels)(nil)
var _ linkedql.PathStep = (*Labels)(nil)

// Labels corresponds to .labels().
type Labels struct {
	From linkedql.PathStep `json:"from"`
}

// Type implements Step.
func (s *Labels) Type() quad.IRI {
	return linkedql.Prefix + "Labels"
}

// Description implements Step.
func (s *Labels) Description() string {
	return "gets the list of inbound and outbound quad labels"
}

// BuildIterator implements linkedql.IteratorStep.
func (s *Labels) BuildIterator(qs graph.QuadStore, ns *voc.Namespaces) (query.Iterator, error) {
	return linkedql.NewValueIteratorFromPathStep(s, qs, ns)
}

// BuildPath implements linkedql.PathStep.
func (s *Labels) BuildPath(qs graph.QuadStore, ns *voc.Namespaces) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs, ns)
	if err != nil {
		return nil, err
	}
	return fromPath.Labels(), nil
}
