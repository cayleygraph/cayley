package mongo

import (
	"testing"

	"github.com/codelingo/cayley/graph"
	"github.com/codelingo/cayley/graph/graphtest"
	"github.com/codelingo/cayley/graph/path/pathtest"
	"github.com/codelingo/cayley/internal/dock"

	"github.com/codelingo/cayley/graph"
	"github.com/codelingo/cayley/graph/graphtest"
	"github.com/codelingo/cayley/internal/dock"
)

func makeMongo(t testing.TB) (graph.QuadStore, graph.Options, func()) {
	var conf dock.Config

	conf.Image = "mongo:3"
	conf.OpenStdin = true
	conf.Tty = true

	addr, closer := dock.Run(t, conf)
	addr = addr + ":27017"
	if err := createNewMongoGraph(addr, nil); err != nil {
		closer()
		t.Fatal(err)
	}
	qs, err := newQuadStore(addr, nil)
	if err != nil {
		closer()
		t.Fatal(err)
	}
	return qs, nil, func() {
		qs.Close()
		closer()
	}
}

func TestMongoAll(t *testing.T) {
	graphtest.TestAll(t, makeMongo, &graphtest.Config{
		TimeInMs:                 true,
		OptimizesComparison:      true,
		SkipDeletedFromIterator:  true,
		SkipSizeCheckAfterDelete: true,
		SkipNodeDelAfterQuadDel:  true,
	})
}

func TestMongoPaths(t *testing.T) {
	pathtest.RunTestMorphisms(t, makeMongo)
}
