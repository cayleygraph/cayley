package client

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/quad"
)

type Class quad.Value
type Property quad.IRI

func getProperties(qs graph.QuadStore, class Class) []Property {
	panic("Not implemented")
}

func getCardinality(qs graph.QuadStore, class Class, property Property) (int, error) {
	panic("Not implemented")
}

func getMaxCardinality(qs graph.QuadStore, class Class, property Property) (int, error) {
	panic("Not implemented")
}

func getRange(qs graph.QuadStore, property Property) quad.Value {
	panic("Not implemented")
}

func getAllClasses(qs graph.QuadStore) []Class {
	panic("Not implemented")
}
