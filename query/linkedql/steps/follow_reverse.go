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
	linkedql.Register(&FollowReverse{})
}

var _ linkedql.IteratorStep = (*FollowReverse)(nil)
var _ linkedql.PathStep = (*FollowReverse)(nil)

// FollowReverse corresponds to .followR().
type FollowReverse struct {
	From     linkedql.PathStep `json:"from"`
	Followed linkedql.PathStep `json:"followed"`
}

// Type implements Step.
func (s *FollowReverse) Type() quad.IRI {
	return linkedql.Prefix + "FollowReverse"
}

// Description implements Step.
func (s *FollowReverse) Description() string {
	return "is the same as follow but follows the chain in the reverse direction. Flips View and ViewReverse where appropriate, the net result being a virtual predicate followed in the reverse direction. Starts at the end of the morphism and follows it backwards (with appropriate flipped directions) to the g.M() location."
}

// BuildIterator implements linkedql.IteratorStep.
func (s *FollowReverse) BuildIterator(qs graph.QuadStore, ns *voc.Namespaces) (query.Iterator, error) {
	return linkedql.NewValueIteratorFromPathStep(s, qs, ns)
}

// BuildPath implements linkedql.PathStep.
func (s *FollowReverse) BuildPath(qs graph.QuadStore, ns *voc.Namespaces) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs, ns)
	if err != nil {
		return nil, err
	}
	p, err := s.Followed.BuildPath(qs, ns)
	if err != nil {
		return nil, err
	}
	return fromPath.FollowReverse(p), nil
}

