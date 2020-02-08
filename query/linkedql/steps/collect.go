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
	linkedql.Register(&Collect{})
}

var _ linkedql.IteratorStep = (*Collect)(nil)
var _ linkedql.PathStep = (*Collect)(nil)

// Collect corresponds to .view().
type Collect struct {
	From linkedql.PathStep `json:"from"`
}

// Description implements Step.
func (s *Collect) Description() string {
	return "Recursively resolves values of a list (also known as RDF collection)"
}

// BuildIterator implements linkedql.IteratorStep.
func (s *Collect) BuildIterator(qs graph.QuadStore, ns *voc.Namespaces) (query.Iterator, error) {
	return linkedql.NewValueIteratorFromPathStep(s, qs, ns)
}

var (
	first  = quad.IRI("rdf:first").Full()
	rest   = quad.IRI("rdf:rest").Full()
	rdfNil = quad.IRI("rdf:nil").Full()
)

// BuildPath implements linkedql.PathStep.
func (s *Collect) BuildPath(qs graph.QuadStore, ns *voc.Namespaces) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs, ns)
	if err != nil {
		return nil, err
	}
	m := path.StartMorphism().Save(first, string(first)).Out(rest).Tag(string(rest))
	return fromPath.FollowRecursive(m, -1, nil), nil
}
