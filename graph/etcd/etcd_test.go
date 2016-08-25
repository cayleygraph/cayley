package etcd

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/graphtest"
	"testing"
)

func makeEtcd(t testing.TB) (graph.QuadStore, graph.Options, func()) {
	etc, closer := runEtcd3(t)
	qs, err := Create(etc, "cayley")
	if err != nil {
		closer()
		t.Fatal(err)
	}
	return qs, nil, func() {
		qs.Close()
		closer()
	}
}

func TestEtcd(t *testing.T) {
	graphtest.TestAll(t, makeEtcd, &graphtest.Config{
		CustomHorizon:           true,
		SkipDeletedFromIterator: true,
		SkipNodeDelAfterQuadDel: true,
	})
}
