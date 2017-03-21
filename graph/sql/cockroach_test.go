// +build docker

package sql

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/graphtest"
	"github.com/cayleygraph/cayley/graph/path/pathtest"
	"github.com/cayleygraph/cayley/internal/dock"
	"github.com/cayleygraph/cayley/quad"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
	"testing"
	"unicode/utf8"
)

func makeCockroach(t testing.TB) (graph.QuadStore, graph.Options, func()) {
	var conf dock.Config // TODO

	conf.Image = "cockroachdb/cockroach:beta-20170112"
	conf.Cmd = []string{"start", "--insecure"}

	opts := graph.Options{"flavor": flavorCockroach}

	addr, closer := dock.RunAndWait(t, conf, func(addr string) bool {
		conn, err := pq.Open(`postgresql://root@` + addr + `:26257?sslmode=disable`)
		if err != nil {
			return false
		}
		conn.Close()
		return true
	})
	db, err := connect(`postgresql://root@`+addr+`:26257?sslmode=disable`, flavorPostgres, nil)
	if err != nil {
		closer()
		t.Fatal(err)
	} else if _, err = db.Exec("CREATE DATABASE cayley"); err != nil {
		closer()
		t.Fatal(err)
	}
	db.Close()
	addr = `postgresql://root@` + addr + `:26257/cayley?sslmode=disable`
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
	require.Nil(t, err)
	require.Equal(t, obj, qs.NameOf(qs.ValueOf(quad.Raw(obj.String()))))
}

func TestCockroachPaths(t *testing.T) {
	pathtest.RunTestMorphisms(t, makeCockroach)
}
