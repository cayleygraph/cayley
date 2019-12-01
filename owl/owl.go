package owl

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/quad"
)

type Class struct {
	qs         graph.QuadStore
	Identifier quad.Value
}

func (c *Class) Properties() []Property {
	panic("Not implemented")
}

func (c *Class) CardinalityOf(property Property) (int, error) {
	panic("Not implemented")
}

func (c *Class) MaxCardinalityOf(property Property) (int, error) {
	panic("Not implemented")
}

func (c *Class) SubClasses() []Class {
	panic("Not implemented")
}

func GetClass(qs graph.QuadStore, identifier quad.Value) Class {
	return Class{Identifier: identifier}
}

type Property struct {
	qs         graph.QuadStore
	Identifier quad.IRI
}

func (p *Property) Range(property Property) quad.Value {
	panic("Not implemented")
}
