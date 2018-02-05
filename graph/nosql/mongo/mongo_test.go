// +build docker

package mongo

import (
	"testing"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/nosql"
	"github.com/cayleygraph/cayley/graph/nosql/nosqltest"
	"github.com/cayleygraph/cayley/internal/dock"
)

func makeMongo(t testing.TB) (nosql.Database, *nosql.Options, graph.Options, func()) {
	var conf dock.Config

	conf.Image = "mongo:3"
	conf.OpenStdin = true
	conf.Tty = true

	addr, closer := dock.RunAndWait(t, conf, "27017", nil)

	qs, err := dialDB(addr, nil)
	if err != nil {
		closer()
		t.Fatal(err)
	}
	return qs, nil, nil, func() {
		qs.Close()
		closer()
	}
}

var conf = &nosqltest.Config{
	TimeInMs: true,
}

func TestMongo(t *testing.T) {
	nosqltest.TestAll(t, makeMongo, conf)
}

func BenchmarkMongo(t *testing.B) {
	nosqltest.BenchmarkAll(t, makeMongo, conf)
}
