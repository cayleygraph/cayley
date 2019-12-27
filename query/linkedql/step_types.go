package linkedql

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad/voc"
)

// Step is the tree representation of a call in a Path context.
//
// Example:
// 		g.V(g.IRI("alice"))
// 		is represented as
// 		&Vertex{ Values: []quad.Value{quad.IRI("alice")} }
//
// 		g.V().out(g.IRI("likes"))
// 		is represented as
// 		&Out{ Properties: []quad.Value{quad.IRI("likes")}, From: &Vertex{} }
type Step interface {
	RegistryItem
}

// IteratorStep is a step that can build an Iterator.
type IteratorStep interface {
	Step
	BuildIterator(qs graph.QuadStore, ns *voc.Namespaces) (query.Iterator, error)
}

// PathStep is a Step that can build a Path.
type PathStep interface {
	Step
	BuildPath(qs graph.QuadStore, ns *voc.Namespaces) (*path.Path, error)
}
