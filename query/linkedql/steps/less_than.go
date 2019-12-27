
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
	linkedql.Register(&LessThan{})
}

var _ linkedql.IteratorStep = (*LessThan)(nil)
var _ linkedql.PathStep = (*LessThan)(nil)

// LessThan corresponds to lt().
type LessThan struct {
	From  linkedql.PathStep   `json:"from"`
	Value quad.Value `json:"value"`
}




// Description implements Step.
func (s *LessThan) Description() string {
	return "Less than filters out values that are not less than given value"
}

// BuildIterator implements linkedql.IteratorStep.
func (s *LessThan) BuildIterator(qs graph.QuadStore, ns *voc.Namespaces) (query.Iterator, error) {
	return linkedql.NewValueIteratorFromPathStep(s, qs, ns)
}

// BuildPath implements linkedql.PathStep.
func (s *LessThan) BuildPath(qs graph.QuadStore, ns *voc.Namespaces) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs, ns)
	if err != nil {
		return nil, err
	}
	return fromPath.Filter(iterator.CompareLT, s.Value), nil
}

