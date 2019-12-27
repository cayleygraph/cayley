package steps

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query"
	"github.com/cayleygraph/cayley/query/linkedql"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad/voc"
)

func init() {
	linkedql.Register(&Follow{})
}

var _ linkedql.IteratorStep = (*Follow)(nil)
var _ linkedql.PathStep = (*Follow)(nil)

// Follow corresponds to .follow().
type Follow struct {
	From     linkedql.PathStep `json:"from"`
	Followed linkedql.PathStep `json:"followed"`
}

// Description implements Step.
func (s *Follow) Description() string {
	return "is the way to use a path prepared with Morphism. Applies the path chain on the morphism object to the current path. Starts as if at the g.M() and follows through the morphism path."
}

// BuildIterator implements linkedql.IteratorStep.
func (s *Follow) BuildIterator(qs graph.QuadStore, ns *voc.Namespaces) (query.Iterator, error) {
	return linkedql.NewValueIteratorFromPathStep(s, qs, ns)
}

// BuildPath implements linkedql.PathStep.
func (s *Follow) BuildPath(qs graph.QuadStore, ns *voc.Namespaces) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs, ns)
	if err != nil {
		return nil, err
	}
	p, err := s.Followed.BuildPath(qs, ns)
	if err != nil {
		return nil, err
	}
	return fromPath.Follow(p), nil
}
