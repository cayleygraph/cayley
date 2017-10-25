// +build docker

package mongo

import (
	"testing"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/nosql"
	"github.com/cayleygraph/cayley/graph/nosql/nosqltest"
	"github.com/cayleygraph/cayley/internal/dock"
)

func makeMongo(t testing.TB) (nosql.Database, graph.Options, func()) {
	var conf dock.Config

	conf.Image = "mongo:3"
	conf.OpenStdin = true
	conf.Tty = true

	addr, closer := dock.Run(t, conf)

	addr = addr + ":27017"
	qs, err := dialDB(addr, nil)
	if err != nil {
		closer()
		t.Fatal(err)
	}
	return qs, nil, func() {
		qs.Close()
		closer()
	}
}

func TestMongo(t *testing.T) {
	nosqltest.TestAll(t, makeMongo, &nosqltest.Config{
		TimeInMs: true,
	})
}
