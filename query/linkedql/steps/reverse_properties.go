package steps

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query/linkedql"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/voc"
)

func init() {
	linkedql.Register(&ReverseProperties{})
}

var _ linkedql.PathStep = (*ReverseProperties)(nil)

// ReverseProperties corresponds to .reverseProperties().
type ReverseProperties struct {
	From  linkedql.PathStep      `json:"from"`
	Names *linkedql.PropertyPath `json:"names"`
}

// Description implements Step.
func (s *ReverseProperties) Description() string {
	return "gets all the properties the current entity / value is referenced at"
}

// BuildPath implements linkedql.PathStep.
func (s *ReverseProperties) BuildPath(qs graph.QuadStore, ns *voc.Namespaces) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs, ns)
	if err != nil {
		return nil, err
	}
	p := fromPath
	names, err := resolveNames(s.Names)
	if err != nil {
		return nil, err
	}
	for _, n := range names {
		name := quad.IRI(n).FullWith(ns)
		tag := string(name)
		p = fromPath.SaveReverse(name, tag)
	}
	return p, nil
}
