package graph

import (
	"github.com/twinj/uuid"
)

type Var struct {
	name string
}

// NewVar creates a variable that can be used in place of a graph.Value
func NewVar() Var {
	return Var{
		name: string(uuid.NewV4()),
	}
}

func (v Var) String() string {
	return v.name
}

func (v Var) Native() interface{} {
	return v.name
}
