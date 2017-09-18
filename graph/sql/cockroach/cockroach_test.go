// +build docker

package cockroach

import (
	"testing"
	"database/sql"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/internal/dock"
	"github.com/cayleygraph/cayley/graph/sql/sqltest"
	"github.com/lib/pq"
)

func makeCockroach(t testing.TB) (string, graph.Options, func()) {
	var conf dock.Config

	conf.Image = "cockroachdb/cockroach:v1.0.5"
	conf.Cmd = []string{"start", "--insecure"}

	addr, closer := dock.RunAndWait(t, conf, func(addr string) bool {
		conn, err := pq.Open(`postgresql://root@` + addr + `:26257?sslmode=disable`)
		if err != nil {
			return false
		}
		conn.Close()
		return true
	})
	addr = `postgresql://root@`+addr+`:26257`
	db, err := sql.Open(driverName,addr+`?sslmode=disable`)
	if err != nil {
		closer()
		t.Fatal(err)
	}
	defer db.Close()
	const dbName = "cayley"
	if _, err = db.Exec("CREATE DATABASE "+dbName); err != nil {
		closer()
		t.Fatal(err)
	}
	addr = addr + `/`+dbName+`?sslmode=disable`
	return addr, nil, func() {
		closer()
	}
}

func TestCockroach(t *testing.T) {
	sqltest.TestAll(t, Type, makeCockroach, &sqltest.Config{
		TimeRound:               true,
		SkipIntHorizon:          true,
	})
}
