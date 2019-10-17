package linkedql

import (
	"encoding/json"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/voc"
)

const namespace = "http://cayley.io/linkedql#"
const prefix = "linkedql:"

func init() {
	voc.Register(voc.Namespace{Full: namespace, Prefix: prefix})
	Register(&Vertex{})
	Register(&Out{})
}

// Vertex corresponds to g.V()
type Vertex struct {
	Values []json.RawMessage `json:"values"`
}

// Type implements Step
func (s *Vertex) Type() quad.IRI {
	return prefix + "Vertex"
}

// BuildPath implements Step
func (s *Vertex) BuildPath(qs graph.QuadStore) *path.Path {
	return path.StartPath(qs)
}

// Out corresponds to .out()
type Out struct {
	From Step     `json:"from"`
	Via  Step     `json:"via"`
	Tags []string `json:"tags"`
}

// Type implements Step
func (s *Out) Type() quad.IRI {
	return prefix + "Out"
}

// BuildPath implements Step
func (s *Out) BuildPath(qs graph.QuadStore) *path.Path {
	return path.StartPath(qs)
}
