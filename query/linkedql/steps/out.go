package steps

import (
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/cayley/query/linkedql"
)

func init() {
	linkedql.Register(&Out{})
}

var _ linkedql.IteratorStep = (*Out)(nil)
var _ linkedql.PathStep = (*Out)(nil)

// Out is an alias for View.
type Out struct {
	Visit
}

// Type implements Step.
func (s *Out) Type() quad.IRI {
	return linkedql.Prefix + "Out"
}

// Description implements Step.
func (s *Out) Description() string {
	return "aliases for View"
}
