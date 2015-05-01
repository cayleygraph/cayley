package cayley

import (
	"github.com/google/cayley/graph"
	_ "github.com/google/cayley/graph/memstore"
	"github.com/google/cayley/graph/path"
	"github.com/google/cayley/quad"
	_ "github.com/google/cayley/writer"
)

type Iterator graph.Iterator
type QuadStore graph.QuadStore
type QuadWriter graph.QuadWriter

type Path path.Path

type Quad quad.Quad

var StartMorphism = path.StartMorphism
var StartPath = path.StartPath

var RawNext = graph.Next

type Handle struct {
	graph.QuadStore
	graph.QuadWriter
}

func NewMemoryGraph() (*Handle, error) {
	qs, err := graph.NewQuadStore("memstore", "", nil)
	if err != nil {
		return nil, err
	}
	qw, err := graph.NewQuadWriter("single", qs, nil)
	if err != nil {
		return nil, err
	}
	return &Handle{qs, qw}, nil
}

func (h *Handle) Close() {
	h.QuadStore.Close()
	h.QuadWriter.Close()
}
