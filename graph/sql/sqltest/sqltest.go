package sqltest

import (
	"testing"
	"unicode/utf8"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/graphtest"
	"github.com/cayleygraph/cayley/graph/graphtest/testutil"
	"github.com/cayleygraph/cayley/graph/sql"
	"github.com/cayleygraph/cayley/quad"
	"github.com/stretchr/testify/require"
)

type Config struct {
	TimeRound      bool
	SkipIntHorizon bool
}

func TestAll(t *testing.T, typ string, fnc DatabaseFunc, c *Config) {
	if c == nil {
		c = &Config{}
	}
	create := makeDatabaseFunc(typ, fnc)
	t.Run("graph", func(t *testing.T) {
		t.Parallel()
		graphtest.TestAll(t, create, &graphtest.Config{
			NoPrimitives:            true,
			TimeInMcs:               true,
			TimeRound:               c.TimeRound,
			OptimizesComparison:     true,
			SkipNodeDelAfterQuadDel: true,
			SkipIntHorizon:          c.SkipIntHorizon,
		})
	})
	t.Run("zero rune", func(t *testing.T) {
		t.Parallel()
		testZeroRune(t, create)
	})
}

type DatabaseFunc func(t testing.TB) (string, graph.Options, func())

func makeDatabaseFunc(typ string, create DatabaseFunc) testutil.DatabaseFunc {
	return func(t testing.TB) (graph.QuadStore, graph.Options, func()) {
		addr, opts, closer := create(t)
		if err := sql.Init(typ, addr, opts); err != nil {
			closer()
			t.Fatal(err)
		}
		qs, err := sql.New(typ, addr, opts)
		if err != nil {
			closer()
			t.Fatal(err)
		}
		return qs, nil, func() {
			qs.Close()
			closer()
		}
	}
}

func testZeroRune(t testing.TB, create testutil.DatabaseFunc) {
	qs, opts, closer := create(t)
	defer closer()

	w := testutil.MakeWriter(t, qs, opts)

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
