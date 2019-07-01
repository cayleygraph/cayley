package nosqltest

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/graphtest"
	gnosql "github.com/cayleygraph/cayley/graph/nosql"
	"github.com/hidal-go/hidalgo/legacy/nosql"
	"github.com/hidal-go/hidalgo/legacy/nosql/nosqltest"
)

func toConfig(c nosql.Traits) graphtest.Config {
	return graphtest.Config{
		NoPrimitives:             true,
		TimeInMs:                 c.TimeInMs,
		OptimizesComparison:      true,
		SkipDeletedFromIterator:  true,
		SkipSizeCheckAfterDelete: true,
	}
}

func NewQuadStore(t testing.TB, gen nosqltest.Database) (graph.QuadStore, graph.Options, func()) {
	db, closer := gen.Run(t)
	err := gnosql.Init(db, nil)
	if err != nil {
		db.Close()
		closer()
		require.Fail(t, "init failed", "%v", err)
	}
	tr := gen.Traits
	kdb, err := gnosql.NewQuadStore(db, &tr, nil)
	if err != nil {
		db.Close()
		closer()
		require.Fail(t, "create failed", "%v", err)
	}
	return kdb, nil, func() {
		kdb.Close()
		closer()
	}
}

func TestAll(t *testing.T, gen nosqltest.Database) {
	c := toConfig(gen.Traits)
	graphtest.TestAll(t, func(t testing.TB) (graph.QuadStore, graph.Options, func()) {
		return NewQuadStore(t, gen)
	}, &c)
}

func BenchmarkAll(t *testing.B, gen nosqltest.Database) {
	c := toConfig(gen.Traits)
	graphtest.BenchmarkAll(t, func(t testing.TB) (graph.QuadStore, graph.Options, func()) {
		return NewQuadStore(t, gen)
	}, &c)
}
