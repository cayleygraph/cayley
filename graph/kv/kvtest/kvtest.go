package kvtest

import (
	"context"
	"reflect"
	"testing"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/graphtest"
	"github.com/cayleygraph/cayley/graph/graphtest/testutil"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/graph/kv"
	"github.com/cayleygraph/cayley/quad"
	"github.com/stretchr/testify/require"
)

type DatabaseFunc func(t testing.TB) (kv.BucketKV, graph.Options, func())

type Config struct {
	AlwaysRunIntegration bool
}

func (c Config) quadStore() *graphtest.Config {
	return &graphtest.Config{
		NoPrimitives:         true,
		AlwaysRunIntegration: c.AlwaysRunIntegration,
	}
}

func NewQuadStoreFunc(gen DatabaseFunc) testutil.DatabaseFunc {
	return func(t testing.TB) (graph.QuadStore, graph.Options, func()) {
		return NewQuadStore(t, gen)
	}
}

func NewQuadStore(t testing.TB, gen DatabaseFunc) (graph.QuadStore, graph.Options, func()) {
	db, opt, closer := gen(t)
	err := kv.Init(db, opt)
	if err != nil {
		db.Close()
		closer()
		require.Fail(t, "init failed", "%v", err)
	}
	kdb, err := kv.New(db, opt)
	if err != nil {
		db.Close()
		closer()
		require.Fail(t, "create failed", "%v", err)
	}
	return kdb, opt, func() {
		kdb.Close()
		closer()
	}
}

func TestAll(t *testing.T, gen DatabaseFunc, conf *Config) {
	if conf == nil {
		conf = &Config{}
	}
	qsgen := NewQuadStoreFunc(gen)
	t.Run("qs", func(t *testing.T) {
		graphtest.TestAll(t, qsgen, conf.quadStore())
	})
	t.Run("optimize", func(t *testing.T) {
		testOptimize(t, gen, conf)
	})
}

func testOptimize(t *testing.T, gen DatabaseFunc, _ *Config) {
	ctx := context.TODO()
	qs, opts, closer := NewQuadStore(t, gen)
	defer closer()

	testutil.MakeWriter(t, qs, opts, graphtest.MakeQuadSet()...)

	// With an linksto-fixed pair
	fixed := iterator.NewFixed()
	fixed.Add(qs.ValueOf(quad.Raw("F")))
	fixed.Tagger().Add("internal")
	lto := iterator.NewLinksTo(qs, fixed, quad.Object)

	oldIt := lto.Clone()
	newIt, ok := lto.Optimize()
	if !ok {
		t.Errorf("Failed to optimize iterator")
	}
	if _, ok := newIt.(*kv.QuadIterator); !ok {
		t.Errorf("Optimized iterator type does not match original, got:%T", newIt)
	}

	newQuads := graphtest.IteratedQuads(t, qs, newIt)
	oldQuads := graphtest.IteratedQuads(t, qs, oldIt)
	if !reflect.DeepEqual(newQuads, oldQuads) {
		t.Errorf("Optimized iteration does not match original")
	}

	oldIt.Next(ctx)
	oldResults := make(map[string]graph.Value)
	oldIt.TagResults(oldResults)
	newIt.Next(ctx)
	newResults := make(map[string]graph.Value)
	newIt.TagResults(newResults)
	if !reflect.DeepEqual(newResults, oldResults) {
		t.Errorf("Discordant tag results, new:%v old:%v", newResults, oldResults)
	}
}

func BenchmarkAll(t *testing.B, gen DatabaseFunc, conf *Config) {
	if conf == nil {
		conf = &Config{}
	}
	qsgen := NewQuadStoreFunc(gen)
	t.Run("qs", func(t *testing.B) {
		graphtest.BenchmarkAll(t, qsgen, conf.quadStore())
	})
}
