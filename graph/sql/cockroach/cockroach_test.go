// +build docker

package cockroach

import (
	"database/sql"
	"testing"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/sql/sqltest"
	"github.com/cayleygraph/cayley/internal/dock"
	"github.com/lib/pq"
)

func makeCockroach(t testing.TB) (string, graph.Options, func()) {
	var conf dock.Config

	conf.Image = "cockroachdb/cockroach:v1.1.2"
	conf.Cmd = []string{"start", "--insecure"}

	addr, closer := dock.RunAndWait(t, conf, func(addr string) bool {
		conn, err := pq.Open(`postgresql://root@` + addr + `:26257?sslmode=disable`)
		if err != nil {
			return false
		}
		conn.Close()
		return true
	})
	addr = `postgresql://root@` + addr + `:26257`
	db, err := sql.Open(driverName, addr+`?sslmode=disable`)
	if err != nil {
		closer()
		t.Fatal(err)
	}
	defer db.Close()
	const dbName = "cayley"
	if _, err = db.Exec("CREATE DATABASE " + dbName); err != nil {
		closer()
		t.Fatal(err)
	}
	addr = addr + `/` + dbName + `?sslmode=disable`
	return addr, nil, func() {
		closer()
	}
}

var conf = &sqltest.Config{
	TimeRound: true,
}

func TestCockroach(t *testing.T) {
	sqltest.TestAll(t, Type, makeCockroach, conf)
}

func BenchmarkCockroach(t *testing.B) {
	sqltest.BenchmarkAll(t, Type, makeCockroach, conf)
}
