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
	linkedql.Register(&Has{})
}

var _ linkedql.IteratorStep = (*Has)(nil)
var _ linkedql.PathStep = (*Has)(nil)

// Has corresponds to .has().
type Has struct {
	From     linkedql.PathStep     `json:"from"`
	Property linkedql.PropertyPath `json:"property"`
	Values   []quad.Value `json:"values"`
}




// Description implements Step.
func (s *Has) Description() string {
	return "filters all paths which are, at this point, on the subject for the given predicate and object, but do not follow the path, merely filter the possible paths. Usually useful for starting with all nodes, or limiting to a subset depending on some predicate/value pair."
}

// BuildIterator implements linkedql.IteratorStep.
func (s *Has) BuildIterator(qs graph.QuadStore, ns *voc.Namespaces) (query.Iterator, error) {
	return linkedql.NewValueIteratorFromPathStep(s, qs, ns)
}

// BuildPath implements linkedql.PathStep.
func (s *Has) BuildPath(qs graph.QuadStore, ns *voc.Namespaces) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs, ns)
	if err != nil {
		return nil, err
	}
	viaPath, err := s.Property.BuildPath(qs, ns)
	if err != nil {
		return nil, err
	}
	return fromPath.Has(viaPath, s.Values...), nil
}
