//go:build docker
// +build docker

package cockroach

import (
	"context"
	"database/sql"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/sql/sqltest"
	"github.com/cayleygraph/cayley/internal/dock"
)

func makeCockroach(t testing.TB) (string, graph.Options) {
	var conf dock.Config

	conf.Image = "cockroachdb/cockroach:latest-v24.1"
	conf.Cmd = []string{"start-single-node", "--insecure"}

	addr := dock.RunAndWait(t, conf, "26257", func(addr string) bool {
		host, portStr, err := net.SplitHostPort(addr)
		if err != nil {
			return false
		}
		port, err := strconv.Atoi(portStr)
		if err != nil {
			return false
		}

		cconf, err := pgx.ParseConfig("")
		if err != nil {
			return false
		}
		cconf.Host = host
		cconf.Port = uint16(port)
		cconf.User = "root"

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		conn, err := pgx.ConnectConfig(ctx, cconf)
		if err != nil {
			return false
		}
		conn.Close(ctx)
		return true
	})
	addr = `postgresql://root@` + addr
	db, err := sql.Open("pgx", addr+`?sslmode=disable`)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	const dbName = "cayley"
	if _, err = db.Exec("CREATE DATABASE " + dbName); err != nil {
		t.Fatal(err)
	}
	addr = addr + `/` + dbName + `?sslmode=disable`
	return addr, nil
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
