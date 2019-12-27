package steps

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query"
	"github.com/cayleygraph/cayley/query/linkedql"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad/voc"
)

func init() {
	linkedql.Register(&Limit{})
}

var _ linkedql.IteratorStep = (*Limit)(nil)
var _ linkedql.PathStep = (*Limit)(nil)

// Limit corresponds to .limit().
type Limit struct {
	From  linkedql.PathStep `json:"from"`
	Limit int64             `json:"limit"`
}

// Description implements Step.
func (s *Limit) Description() string {
	return "limits a number of nodes for current path."
}

// BuildIterator implements linkedql.IteratorStep.
func (s *Limit) BuildIterator(qs graph.QuadStore, ns *voc.Namespaces) (query.Iterator, error) {
	return linkedql.NewValueIteratorFromPathStep(s, qs, ns)
}

// BuildPath implements linkedql.PathStep.
func (s *Limit) BuildPath(qs graph.QuadStore, ns *voc.Namespaces) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs, ns)
	if err != nil {
		return nil, err
	}
	return fromPath.Limit(s.Limit), nil
}
