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
	return path.StartPath(qs, s.Values...), nil
}

var _ linkedql.PathStep = (*Placeholder)(nil)

// Placeholder corresponds to .Placeholder().
type Placeholder struct{}




// Description implements Step.
func (s *Placeholder) Description() string {
	return "is like Vertex but resolves to the values in the context it is placed in. It should only be used where a linkedql.PathStep is expected and can't be resolved on its own."
}

// BuildPath implements linkedql.PathStep.
func (s *Placeholder) BuildPath(qs graph.QuadStore, ns *voc.Namespaces) (*path.Path, error) {
	return path.StartMorphism(), nil
}
