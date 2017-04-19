// +build docker

package sql

import (
	"fmt"
	"math/rand"
	"runtime"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/graphtest"
	"github.com/cayleygraph/cayley/graph/path/pathtest"
	"github.com/cayleygraph/cayley/internal/dock"
	"github.com/cayleygraph/cayley/quad"
	"github.com/fsouza/go-dockerclient"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func makeCockroach(t testing.TB) (graph.QuadStore, graph.Options, func()) {
	var conf dock.Config // TODO

	conf.Image = "cockroachdb/cockroach:beta-20170413"
	conf.Cmd = []string{"start", "--insecure"}

	opts := graph.Options{"flavor": flavorCockroach}

	var (
		host   string
		port   int
		closer func()
	)

	if runtime.GOOS == "darwin" {
		port = func() int {
			rand.Seed(time.Now().Unix())
			return rand.Intn(1000) + 45000
		}()

		conf.PortBindings = map[docker.Port][]docker.PortBinding{
			"26257/tcp": []docker.PortBinding{
				{
					HostPort: fmt.Sprintf("%d", port),
				},
			},
		}

		host = "localhost"
		_, closer = dock.RunAndWait(t, conf, func(string) bool {
			conn, err := pq.Open(fmt.Sprintf("postgresql://root@%s:%d?sslmode=disable", "localhost", port))
			if err != nil {
				return false
			}
			conn.Close()
			return true
		})
	} else {
		port = 26257
		host, closer = dock.RunAndWait(t, conf, func(host string) bool {
			conn, err := pq.Open(fmt.Sprintf("postgresql://root@%s:%d?sslmode=disable", host, port))
			if err != nil {
				return false
			}
			conn.Close()
			return true
		})
	}

	addr := fmt.Sprintf("postgresql://root@%s:%d/cayley?sslmode=disable", host, port)

	db, err := connect(addr, flavorPostgres, nil)
	if err != nil {
		closer()
		t.Fatal(err)
	} else if _, err = db.Exec("CREATE DATABASE cayley"); err != nil {
		closer()
		t.Fatal(err)
	}
	db.Close()

	if err := createSQLTables(addr, opts); err != nil {
		closer()
		t.Fatal(err)
	}
	qs, err := newQuadStore(addr, opts)
	if err != nil {
		closer()
		t.Fatal(err)
	}
	return qs, nil, func() {
		qs.Close()
		closer()
	}
}

func TestCockroachAll(t *testing.T) {
	graphtest.TestAll(t, makeCockroach, &graphtest.Config{
		TimeInMcs:               true,
		TimeRound:               true,
		OptimizesHasAToUnique:   true,
		SkipIntHorizon:          true,
		SkipNodeDelAfterQuadDel: true,
	})
}

func TestCockroachZeroRune(t *testing.T) {
	qs, opts, closer := makeCockroach(t)
	defer closer()

	w := graphtest.MakeWriter(t, qs, opts)

	obj := quad.String("AB\u0000CD")
	if !utf8.ValidString(string(obj)) {
		t.Fatal("invalid utf8")
	}

	err := w.AddQuad(quad.Quad{
		Subject:   quad.IRI("bob"),
		Predicate: quad.IRI("pred"),
		Object:    obj,
	})
	require.NoError(t, err)
	require.Equal(t, obj, qs.NameOf(qs.ValueOf(quad.Raw(obj.String()))))
}

func TestCockroachPaths(t *testing.T) {
	pathtest.RunTestMorphisms(t, makeCockroach)
}
