// +build docker

package elastic

import (
	"testing"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/graphtest"
	"github.com/cayleygraph/cayley/graph/path/pathtest"
	"github.com/cayleygraph/cayley/internal/dock"
)

func makeElastic(t testing.TB) (graph.QuadStore, graph.Options, func()) {
	var conf dock.Config

	conf.Image = "elastic:3"
	conf.OpenStdin = true
	conf.Tty = true

	addr, closer := dock.Run(t, conf)
	addr = addr + ":9200"
	if err := createNewElasticGraph(addr, nil); err != nil {
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

func TestElasticAll(t *testing.T) {
	graphtest.TestAll(t, makeElastic, &graphtest.Config{
		NoPrimitives:   true,
		SkipIntHorizon: true,
	})
}

func TestElasticPaths(t *testing.T) {
	pathtest.RunTestMorphisms(t, makeElastic)
}
