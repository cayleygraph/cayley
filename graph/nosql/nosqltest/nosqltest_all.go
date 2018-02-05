package nosqltest

import (
	"bytes"
	"context"
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/graphtest"
	"github.com/cayleygraph/cayley/graph/graphtest/testutil"
	"github.com/cayleygraph/cayley/graph/nosql"
	"github.com/cayleygraph/cayley/quad"
)

type DatabaseFunc func(t testing.TB) (nosql.Database, *nosql.Options, graph.Options, func())

type Config struct {
	FloatToInt bool // database silently converts all float values to ints, if possible
	IntToFloat bool // database always converts all int values to floats
	TimeInMs   bool
	Recreate   bool // tests should re-create database instance from scratch on each run
	PageSize   int  // result page size for pagination (large iterator) tests
}

func (c Config) quadStore() *graphtest.Config {
	return &graphtest.Config{
		NoPrimitives:             true,
		TimeInMs:                 c.TimeInMs,
		OptimizesComparison:      true,
		SkipDeletedFromIterator:  true,
		SkipSizeCheckAfterDelete: true,
	}
}

func NewQuadStore(t testing.TB, gen DatabaseFunc) (graph.QuadStore, graph.Options, func()) {
	db, nopt, opt, closer := gen(t)
	err := nosql.Init(db, opt)
	if err != nil {
		db.Close()
		closer()
		require.Fail(t, "init failed", "%v", err)
	}
	kdb, err := nosql.NewQuadStore(db, nopt, opt)
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

const parallel = false

func TestAll(t *testing.T, gen DatabaseFunc, conf *Config) {
	if conf == nil {
		conf = &Config{}
	}
	t.Run("nosql", func(t *testing.T) {
		TestNoSQL(t, gen, conf)
	})
	t.Run("qs", func(t *testing.T) {
		if parallel {
			t.Parallel()
		}
		graphtest.TestAll(t, func(t testing.TB) (graph.QuadStore, graph.Options, func()) {
			return NewQuadStore(t, gen)
		}, conf.quadStore())
	})
	t.Run("concurrent", func(t *testing.T) {
		if testing.Short() {
			t.SkipNow()
		}
		t.SkipNow()
		if parallel {
			t.Parallel()
		}
		testConcurrent(t, gen)
	})
}

func BenchmarkAll(t *testing.B, gen DatabaseFunc, conf *Config) {
	t.Run("qs", func(t *testing.B) {
		graphtest.BenchmarkAll(t, func(t testing.TB) (graph.QuadStore, graph.Options, func()) {
			return NewQuadStore(t, gen)
		}, conf.quadStore())
	})
}

func randString() string {
	const n = 60
	b := bytes.NewBuffer(nil)
	b.Grow(n)
	for i := 0; i < n; i++ {
		b.WriteByte(byte('a' + rand.Intn(26)))
	}
	return b.String()
}

func testConcurrent(t testing.TB, gen DatabaseFunc) {
	ctx := context.TODO()
	qs, opts, closer := NewQuadStore(t, gen)
	defer closer()
	if opts == nil {
		opts = make(graph.Options)
	}
	opts["ignore_duplicate"] = true
	qw := testutil.MakeWriter(t, qs, opts)

	const n = 1000
	subjects := make([]string, 0, n/4)
	for i := 0; i < cap(subjects); i++ {
		subjects = append(subjects, randString())
	}
	var wg sync.WaitGroup
	wg.Add(2)
	done := make(chan struct{})
	go func() {
		defer wg.Done()
		defer close(done)
		for i := 0; i < n; i++ {
			n1 := subjects[rand.Intn(len(subjects))]
			n2 := subjects[rand.Intn(len(subjects))]
			t := graph.NewTransaction()
			t.AddQuad(quad.Make(n1, "link", n2, nil))
			t.AddQuad(quad.Make(n2, "link", n1, nil))
			if err := qw.ApplyTransaction(t); err != nil {
				panic(err)
			}
		}
	}()

	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
			}
			n1 := subjects[rand.Intn(len(subjects))]
			it := qs.QuadIterator(quad.Subject, qs.ValueOf(quad.String(n1)))
			for it.Next(ctx) {
				q := qs.Quad(it.Result())
				_ = q.Subject.Native()
				_ = q.Predicate.Native()
				_ = q.Object.Native()
			}
			if err := it.Close(); err != nil {
				panic(err)
			}
		}
	}()
	wg.Wait()
}
