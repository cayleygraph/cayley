package rethinkdb

import (
	"testing"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/graphtest"
	"github.com/cayleygraph/cayley/internal/dock"
)

func makeRethinkDB(t testing.TB) (graph.QuadStore, graph.Options, func()) {
	clog.SetV(5)

	var conf dock.Config

	conf.Image = "rethinkdb:latest"
	conf.OpenStdin = true
	conf.Tty = true

	addr, closer := dock.Run(t, conf)
	addr = addr + ":28015"
	if err := createNewRethinkDBGraph(addr, nil); err != nil {
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
	graphtest.TestAll(t, makeRethinkDB, &graphtest.Config{
		TimeInMs:                 true,
		OptimizesComparison:      true,
		SkipDeletedFromIterator:  true,
		SkipSizeCheckAfterDelete: true,
		SkipNodeDelAfterQuadDel:  true,
	})
}
