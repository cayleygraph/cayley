package steps

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query"
	"github.com/cayleygraph/cayley/query/linkedql"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad/voc"
)

func init() {
	linkedql.Register(&Entity{})
}

var _ linkedql.IteratorStep = (*Entity)(nil)
var _ linkedql.PathStep = (*Entity)(nil)

// Entity corresponds to g.Entity().
type Entity struct {
	Identifier linkedql.EntityIdentifier `json:"identifier"`
}

// Description implements Step.
func (s *Entity) Description() string {
	return "resolves to the object matching given identifier in the graph."
}

// BuildIterator implements linkedql.IteratorStep.
func (s *Entity) BuildIterator(qs graph.QuadStore, ns *voc.Namespaces) (query.Iterator, error) {
	return linkedql.NewValueIteratorFromPathStep(s, qs, ns)
}

// BuildPath implements linkedql.PathStep.
func (s *Entity) BuildPath(qs graph.QuadStore, ns *voc.Namespaces) (*path.Path, error) {
	identifier, err := s.Identifier.BuildIdentifier()
	if err != nil {
		return nil, err
	}
	return path.StartPath(qs, identifier), nil
}
