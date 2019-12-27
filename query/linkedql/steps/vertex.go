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
	linkedql.Register(&Vertex{})
}

var _ linkedql.IteratorStep = (*Vertex)(nil)
var _ linkedql.PathStep = (*Vertex)(nil)

// Vertex corresponds to g.Vertex() and g.V().
type Vertex struct {
	Values []quad.Value `json:"values"`
}

// Description implements Step.
func (s *Vertex) Description() string {
	return "resolves to all the existing objects and primitive values in the graph. If provided with values resolves to a sublist of all the existing values in the graph."
}

// BuildIterator implements linkedql.IteratorStep.
func (s *Vertex) BuildIterator(qs graph.QuadStore, ns *voc.Namespaces) (query.Iterator, error) {
	return linkedql.NewValueIteratorFromPathStep(s, qs, ns)
}

// BuildPath implements linkedql.PathStep.
func (s *Vertex) BuildPath(qs graph.QuadStore, ns *voc.Namespaces) (*path.Path, error) {
	return path.StartPath(qs, linkedql.AbsoluteValues(s.Values, ns)...), nil
}
