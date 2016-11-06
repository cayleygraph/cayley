// +build docker

package sql

import (
	"testing"
	"unicode/utf8"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/graphtest"
	"github.com/cayleygraph/cayley/graph/path/pathtest"
	"github.com/cayleygraph/cayley/internal/dock"
	"github.com/cayleygraph/cayley/quad"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func makePostgres(t testing.TB) (graph.QuadStore, graph.Options, func()) {
	var conf dock.Config

	conf.Image = "postgres:9.5"
	conf.OpenStdin = true
	conf.Tty = true
	conf.Env = []string{`POSTGRES_PASSWORD=postgres`}

	opts := graph.Options{"flavor": flavorPostgres}

	addr, closer := dock.RunAndWait(t, conf, func(addr string) bool {
		conn, err := pq.Open(`postgres://postgres:postgres@` + addr + `/postgres?sslmode=disable`)
		if err != nil {
			return false
		}
		conn.Close()
		return true
	})
	addr = `postgres://postgres:postgres@` + addr + `/postgres?sslmode=disable`
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

func TestPostgresAll(t *testing.T) {
	graphtest.TestAll(t, makePostgres, &graphtest.Config{
		TimeInMcs:               true,
		TimeRound:               true,
		SkipNodeDelAfterQuadDel: true,
	})
}

func TestPostgresPaths(t *testing.T) {
	pathtest.RunTestMorphisms(t, makePostgres)
}

func TestPostgresZeroRune(t *testing.T) {
	qs, opts, closer := makePostgres(t)
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
