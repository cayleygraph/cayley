// +build docker

package cockroach

import (
	"database/sql"
	"net"
	"strconv"
	"testing"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/sql/sqltest"
	"github.com/cayleygraph/cayley/internal/dock"
	"github.com/jackc/pgx"
	_ "github.com/jackc/pgx/stdlib"
)

func makeCockroach(t testing.TB) (string, graph.Options, func()) {
	var conf dock.Config

	conf.Image = "cockroachdb/cockroach:v1.1.5"
	conf.Cmd = []string{"start", "--insecure"}

	addr, closer := dock.RunAndWait(t, conf, "26257", func(addr string) bool {
		host, portStr, err := net.SplitHostPort(addr)
		if err != nil {
			return false
		}
		port, err := strconv.Atoi(portStr)
		if err != nil {
			return false
		}
		conn, err := pgx.Connect(pgx.ConnConfig{
			Host: host,
			Port: uint16(port),
			User: "root",
		})
		if err != nil {
			return false
		}
		conn.Close()
		return true
	})
	addr = `postgresql://root@` + addr
	db, err := sql.Open("pgx", addr+`?sslmode=disable`)
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
	TimeInMcs: true,
}

func TestCockroach(t *testing.T) {
	sqltest.TestAll(t, Type, makeCockroach, conf)
}

func BenchmarkCockroach(t *testing.B) {
	sqltest.BenchmarkAll(t, Type, makeCockroach, conf)
}
