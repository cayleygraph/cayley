package rethinkdb

import (
	"flag"
	"fmt"
	"math/rand"
	"runtime"
	"testing"
	"time"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/internal/dock"
	"github.com/cayleygraph/cayley/quad"
	docker "github.com/fsouza/go-dockerclient"
)

var debug = flag.Bool("debug", false, "clog debug output")

func init() {
	flag.Parse()

	if *debug {
		clog.SetV(5)
	}
}

func makeRethinkDB(t testing.TB) (graph.QuadStore, graph.Options, func()) {
	var conf dock.Config

	conf.Image = "rethinkdb:latest"
	conf.OpenStdin = true
	conf.Tty = true

	var (
		addr   string
		closer func()
	)

	if runtime.GOOS == "linux" {
		// Normal Linux bridged network mode
		addr, closer = dock.Run(t, conf)
		addr = fmt.Sprintf("%s:%d", addr, 28015)
	} else {
		// If running test on Mac (Mac for Docker) we need to bind ports to host
		// See: https://docs.docker.com/docker-for-mac/networking/#/use-cases-and-workarounds
		conf.ExposedPorts = map[docker.Port]struct{}{
			"28015/tcp": {},
		}

		randPort := func() int {
			rand.Seed(time.Now().Unix())
			return rand.Intn(1000) + 45000
		}()

		conf.PortBindings = map[docker.Port][]docker.PortBinding{
			"28015/tcp": []docker.PortBinding{
				{
					HostPort: fmt.Sprintf("%d", randPort),
				},
			},
		}

		_, closer = dock.Run(t, conf)
		addr = fmt.Sprintf("%s:%d", "localhost", randPort)
	}

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

/*func TestRethinkDBAll(t *testing.T) {
	graphtest.TestAll(t, makeRethinkDB, &graphtest.Config{
		TimeInMs:                true,
		TimeRound:               true,
		OptimizesComparison:     true,
		SkipDeletedFromIterator: true,
		SkipIntHorizon:          true,
	})
}
*/

func makeQuadSet() []quad.Quad {
	return []quad.Quad{
		quad.Make("A", "follows", "B", nil),
		quad.Make("C", "follows", "B", nil),
		quad.Make("C", "follows", "D", nil),
		quad.Make("D", "follows", "B", nil),
		quad.Make("B", "follows", "F", nil),
		quad.Make("F", "follows", "G", nil),
		quad.Make("D", "follows", "G", nil),
		quad.Make("E", "follows", "F", nil),
		quad.Make("B", "status", "cool", "status_graph"),
		quad.Make("D", "status", "cool", "status_graph"),
		quad.Make("G", "status", "cool", "status_graph"),
	}
}

/*func TestIterator(t *testing.T) {
	qs, opts, closer := makeRethinkDB(t)
	defer closer()

	expectIteratedQuads := func(it graph.Iterator, exp []quad.Quad) {
		graphtest.ExpectIteratedQuads(t, qs, it, exp, false)
	}

	graphtest.MakeWriter(t, qs, opts, makeQuadSet()...)
	require.Equal(t, int64(11), qs.Size(), "Incorrect number of quads in store")

	it := NewIterator(qs.(*QuadStore), quadTableName, quad.Subject, qs.ValueOf(quad.String("D")))

	expectIteratedQuads(it, []quad.Quad{
		quad.Make("D", "follows", "B", nil),
		quad.Make("D", "follows", "G", nil),
		quad.Make("D", "status", "cool", "status_graph"),
	})
}*/

func TestLinkToIterator(t *testing.T) {
	/*qs, opts, closer := makeRethinkDB(t)
	defer closer()

	expectIteratedQuads := func(it graph.Iterator, exp []quad.Quad) {
		graphtest.ExpectIteratedQuads(t, qs, it, exp, false)
	}

	graphtest.MakeWriter(t, qs, opts, makeQuadSet()...)
	require.Equal(t, int64(11), qs.Size(), "Incorrect number of quads in store")

	primaryIt := NewIterator(qs.(*QuadStore), quadTableName, quad.Subject, qs.ValueOf(quad.String("D")))

	ltoIt := NewLinksTo(qs.(*QuadStore), primaryIt, quadTableName, quad.Subject, graph.Linkage{
		Dir:   quad.Predicate,
		Value: qs.ValueOf(quad.String("follows")).(NodeHash),
	})

	expectIteratedQuads(ltoIt, []quad.Quad{
		quad.Make("D", "follows", "B", nil),
		quad.Make("D", "follows", "G", nil),
	})*/
}
