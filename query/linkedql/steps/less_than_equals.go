
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
	linkedql.Register(&LessThanEquals{})
}

var _ linkedql.IteratorStep = (*LessThanEquals)(nil)
var _ linkedql.PathStep = (*LessThanEquals)(nil)

// LessThanEquals corresponds to lte().
type LessThanEquals struct {
	From  linkedql.PathStep   `json:"from"`
	Value quad.Value `json:"value"`
}




// Description implements Step.
func (s *LessThanEquals) Description() string {
	return "Less than equals filters out values that are not less than or equal given value"
}

// BuildIterator implements linkedql.IteratorStep.
func (s *LessThanEquals) BuildIterator(qs graph.QuadStore, ns *voc.Namespaces) (query.Iterator, error) {
	return linkedql.NewValueIteratorFromPathStep(s, qs, ns)
}

// BuildPath implements linkedql.PathStep.
func (s *LessThanEquals) BuildPath(qs graph.QuadStore, ns *voc.Namespaces) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs, ns)
	if err != nil {
		return nil, err
	}
	return fromPath.Filter(iterator.CompareLTE, s.Value), nil
}

