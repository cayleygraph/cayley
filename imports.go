package cayley

import (
	"github.com/codelingo/cayley/graph"
	_ "github.com/codelingo/cayley/graph/memstore"
	"github.com/codelingo/cayley/graph/path"
	"github.com/codelingo/cayley/quad"
	_ "github.com/codelingo/cayley/writer"
)

type Iterator graph.Iterator
type QuadStore graph.QuadStore
type QuadWriter graph.QuadWriter

type Path path.Path

var (
	StartMorphism = path.StartMorphism
	StartPath     = path.StartPath

	NewTransaction = graph.NewTransaction
)

type Handle struct {
	graph.QuadStore
	graph.QuadWriter
}

func Triple(subject, predicate, object interface{}) quad.Quad {
	return Quad(subject, predicate, object, nil)
}

func Quad(subject, predicate, object, label interface{}) quad.Quad {
	return quad.Make(subject, predicate, object, label)
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

func (h *Handle) Close() error {
	err := h.QuadWriter.Close()
	h.QuadStore.Close()
	return err
}
