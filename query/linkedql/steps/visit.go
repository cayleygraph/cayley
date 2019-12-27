package steps

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query"
	"github.com/cayleygraph/cayley/query/linkedql"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad/voc"
)

func init() {
	linkedql.Register(&Visit{})
}

var _ linkedql.IteratorStep = (*Visit)(nil)
var _ linkedql.PathStep = (*Visit)(nil)

// Visit corresponds to .view().
type Visit struct {
	From       linkedql.PathStep     `json:"from"`
	Properties linkedql.PropertyPath `json:"properties"`
}

// Description implements Step.
func (s *Visit) Description() string {
	return "resolves to the values of the given property or properties in via of the current objects. If via is a path it's resolved values will be used as properties."
}

// BuildIterator implements linkedql.IteratorStep.
func (s *Visit) BuildIterator(qs graph.QuadStore, ns *voc.Namespaces) (query.Iterator, error) {
	return linkedql.NewValueIteratorFromPathStep(s, qs, ns)
}

// BuildPath implements linkedql.PathStep.
func (s *Visit) BuildPath(qs graph.QuadStore, ns *voc.Namespaces) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs, ns)
	if err != nil {
		return nil, err
	}
	viaPath, err := s.Properties.BuildPath(qs, ns)
	if err != nil {
		return nil, err
	}
	return fromPath.Out(viaPath), nil
}
