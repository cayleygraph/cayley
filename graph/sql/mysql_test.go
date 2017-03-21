// +build docker

package sql

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/graphtest"
	"github.com/cayleygraph/cayley/graph/path/pathtest"
	"github.com/cayleygraph/cayley/internal/dock"
	_ "github.com/go-sql-driver/mysql"
	"net"
	"testing"
	"time"
)

func makeMysql(t testing.TB) (graph.QuadStore, graph.Options, func()) {
	var conf dock.Config

	conf.Image = "mysql:5.7"
	conf.Tty = true
	conf.Env = []string{
		`MYSQL_ROOT_PASSWORD=root`,
		`MYSQL_DATABASE=testdb`,
	}

	opts := graph.Options{"flavor": flavorMysql}

	const wait = time.Second * 5
	addr, closer := dock.RunAndWait(t, conf, func(addr string) bool {
		start := time.Now()
		c, err := net.DialTimeout("tcp", addr+":3306", wait)
		if err == nil {
			c.Close()
		} else if dt := time.Since(start); dt < wait {
			time.Sleep(wait - dt)
		}
		return err == nil
	})
	addr = `root:root@tcp(` + addr + `:3306)/testdb`
	if err := createSQLTables(addr, opts); err != nil {
		closer()
		t.Fatal(err)
	}
	qs, err := newQuadStore(addr, opts)
	if err != nil {
		closer()
		t.Fatal(err)
	}
	return qs, nil, func() {
		qs.Close()
		closer()
	}
}

func TestMysqlAll(t *testing.T) {
	graphtest.TestAll(t, makeMysql, &graphtest.Config{
		TimeInMcs:               true,
		OptimizesHasAToUnique:   true,
		SkipNodeDelAfterQuadDel: true,
	})
}

func TestMysqlPaths(t *testing.T) {
	pathtest.RunTestMorphisms(t, makeMysql)
}
