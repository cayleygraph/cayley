// +build docker

package elastic

import (
	"testing"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/nosql"
	"github.com/cayleygraph/cayley/graph/nosql/nosqltest"
	"github.com/cayleygraph/cayley/internal/dock"
)

func makeElastic(t testing.TB) (nosql.Database, *nosql.Options, graph.Options, func()) {
	var conf dock.Config

	conf.Image = "elasticsearch"
	conf.OpenStdin = true
	conf.Tty = true

	addr, closer := dock.RunAndWait(t, conf, dock.WaitPort("9200"))
	addr = "http://" + addr + ":9200"

	db, err := dialDB(addr, nil)
	if err != nil {
		closer()
		t.Fatal(err)
	}
	return db, nil, nil, func() {
		db.Close()
		closer()
	}
}

var conf = &nosqltest.Config{
	FloatToInt: true,
}

func TestElastic(t *testing.T) {
	nosqltest.TestAll(t, makeElastic, conf)
}

func BenchmarkElastic(t *testing.B) {
	nosqltest.BenchmarkAll(t, makeElastic, conf)
}
