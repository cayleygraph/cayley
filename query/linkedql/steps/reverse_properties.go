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
	linkedql.Register(&ReverseProperties{})
}

var _ linkedql.IteratorStep = (*ReverseProperties)(nil)
var _ linkedql.PathStep = (*ReverseProperties)(nil)

// ReverseProperties corresponds to .reverseProperties().
type ReverseProperties struct {
	From linkedql.PathStep `json:"from"`
	// TODO(iddan): Use property path
	Names []quad.IRI `json:"names"`
}

// Description implements Step.
func (s *ReverseProperties) Description() string {
	return "gets all the properties the current entity / value is referenced at"
}

// BuildIterator implements linkedql.IteratorStep.
func (s *ReverseProperties) BuildIterator(qs graph.QuadStore, ns *voc.Namespaces) (query.Iterator, error) {
	return linkedql.NewValueIteratorFromPathStep(s, qs, ns)
}

// BuildPath implements linkedql.PathStep.
func (s *ReverseProperties) BuildPath(qs graph.QuadStore, ns *voc.Namespaces) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs, ns)
	if err != nil {
		return nil, err
	}
	p := fromPath
	for _, name := range s.Names {
		p = fromPath.SaveReverse(name, string(name))
	}
	return p, nil
}
