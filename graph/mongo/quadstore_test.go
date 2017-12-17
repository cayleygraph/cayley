// +build docker

package mongo

import (
	"bytes"
	"math/rand"
	"sync"
	"testing"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/graphtest"
	"github.com/cayleygraph/cayley/graph/path/pathtest"
	"github.com/cayleygraph/cayley/internal/dock"
	"github.com/cayleygraph/cayley/quad"
)

func makeMongo(t testing.TB) (graph.QuadStore, graph.Options, func()) {
	var conf dock.Config

	conf.Image = "mongo:3"
	conf.OpenStdin = true
	conf.Tty = true

	addr, closer := dock.Run(t, conf)
	addr = addr + ":27017"
	if err := createNewMongoGraph(addr, nil); err != nil {
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

func TestMongoAll(t *testing.T) {
	t.Parallel()
	graphtest.TestAll(t, makeMongo, &graphtest.Config{
		NoPrimitives:             true,
		TimeInMs:                 true,
		SkipDeletedFromIterator:  true,
		SkipSizeCheckAfterDelete: true,
	})
}

func TestMongoPaths(t *testing.T) {
	t.Parallel()
	pathtest.RunTestMorphisms(t, makeMongo)
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

func TestMongoConcurrent(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	qs, opts, closer := makeMongo(t)
	defer closer()
	if opts == nil {
		opts = make(graph.Options)
	}
	opts["ignore_duplicate"] = true
	qw := graphtest.MakeWriter(t, qs, opts)

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
			for it.Next() {
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
