package steps

import (
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

// Description implements Step.
func (s *Out) Description() string {
	return "aliases for View"
}
