// +build docker

package elastic

import (
	"testing"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/nosql"
	"github.com/cayleygraph/cayley/graph/nosql/nosqltest"
	"github.com/cayleygraph/cayley/internal/dock"
)

func makeElastic(t testing.TB) (nosql.Database, graph.Options, func()) {
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
	return db, nil, func() {
		db.Close()
		closer()
	}
}

func TestElastic(t *testing.T) {
	nosqltest.TestAll(t, makeElastic, &nosqltest.Config{
		FloatToInt: true,
	})
}
