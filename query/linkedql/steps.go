package linkedql

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/voc"
)

const namespace = "http://cayley.io/linkedql#"
const prefix = "linkedql:"

func init() {
	voc.Register(voc.Namespace{Full: namespace, Prefix: prefix})
	Register(&NewVertex{})
}

// NewVertex corresponds to g.V()
type NewVertex struct{}

// Type returns the name of NewVertex
func (s *NewVertex) Type() quad.IRI {
	return prefix + "NewVertex"
}

// BuildPath returns a path of NewVertex
func (s *NewVertex) BuildPath(qs graph.QuadStore) *path.Path {
	return path.StartPath(qs)
}
