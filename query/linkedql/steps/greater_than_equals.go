
package steps

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/query"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/voc"
	"github.com/cayleygraph/cayley/query/linkedql"
)

func init() {
	linkedql.Register(&GreaterThanEquals{})
}

var _ linkedql.IteratorStep = (*GreaterThanEquals)(nil)
var _ linkedql.PathStep = (*GreaterThanEquals)(nil)

// GreaterThanEquals corresponds to gte().
type GreaterThanEquals struct {
	From  linkedql.PathStep   `json:"from"`
	Value quad.Value `json:"value"`
}

// Type implements Step.
func (s *GreaterThanEquals) Type() quad.IRI {
	return linkedql.Prefix + "GreaterThanEquals"
}

// Description implements Step.
func (s *GreaterThanEquals) Description() string {
	return "Greater than equals filters out values that are not greater than or equal given value"
}

// BuildIterator implements linkedql.IteratorStep.
func (s *GreaterThanEquals) BuildIterator(qs graph.QuadStore, ns *voc.Namespaces) (query.Iterator, error) {
	return linkedql.NewValueIteratorFromPathStep(s, qs, ns)
}

// BuildPath implements linkedql.PathStep.
func (s *GreaterThanEquals) BuildPath(qs graph.QuadStore, ns *voc.Namespaces) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs, ns)
	if err != nil {
		return nil, err
	}
	return fromPath.Filter(iterator.CompareGTE, s.Value), nil
}

