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

var (
	StartMorphism = path.StartMorphism
	StartPath     = path.StartPath

	RawNext        = graph.Next
	NewTransaction = graph.NewTransaction
)

type Handle struct {
	graph.QuadStore
	graph.QuadWriter
}

func Triple(subject, predicate, object string) quad.Quad {
	return quad.Quad{subject, predicate, object, ""}
}

func Quad(subject, predicate, object, label string) quad.Quad {
	return quad.Quad{subject, predicate, object, label}
}

func NewGraph(name, dbpath string, opts graph.Options) (*Handle, error) {
	qs, err := graph.NewQuadStore(name, dbpath, opts)
	if err != nil {
		return nil, err
	}
	qw, err := graph.NewQuadWriter("single", qs, nil)
	if err != nil {
		return nil, err
	}
	return &Handle{qs, qw}, nil
}

func NewMemoryGraph() (*Handle, error) {
	return NewGraph("memstore", "", nil)
}

func (h *Handle) Close() {
	h.QuadStore.Close()
	h.QuadWriter.Close()
}
