package rethinkdb

import (
	"testing"
	"time"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/graphtest"
	"github.com/cayleygraph/cayley/internal/dock"
	"github.com/fsouza/go-dockerclient"
)

func makeRethinkDB(t testing.TB) (graph.QuadStore, graph.Options, func()) {
	clog.SetV(5)

	var conf dock.Config

	conf.Image = "rethinkdb:latest"
	conf.OpenStdin = true
	conf.Tty = true
	conf.ExposedPorts = map[docker.Port]struct{}{
		"28015": {},
	}
	conf.PortBindings = map[docker.Port][]docker.PortBinding{
		"28015": []docker.PortBinding{
			{
				HostPort: "28015",
			},
		},
	}

	addr, closer := dock.Run(t, conf)
	addr = "localhost" + ":28015"

	t.Logf("Connecting to RethinkDB at: %s", addr)

	// Retry connections, docker container might not be ready
	var err error
	for i := 0; i < 10; i++ {
		err = createNewRethinkDBGraph(addr, nil)
		if err == nil {
			break
		}
		time.Sleep(time.Second * 1)
	}

	if err != nil {
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

func TestRethinkDBAll(t *testing.T) {
	graphtest.TestAll(t, makeRethinkDB, &graphtest.Config{
		TimeInMs:                true,
		OptimizesComparison:     true,
		SkipDeletedFromIterator: true,
		SkipNodeDelAfterQuadDel: true,
		SkipIntHorizon:          true,
	})
}
