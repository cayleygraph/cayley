// +build docker

package mysql

import (
	"net"
	"testing"
	"time"

	"github.com/cayleygraph/cayley/graph/sql/sqltest"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/internal/dock"
)

func makeMysql(t testing.TB) (string, graph.Options, func()) {
	var conf dock.Config

	conf.Image = "mysql:5.7"
	conf.Tty = true
	conf.Env = []string{
		`MYSQL_ROOT_PASSWORD=root`,
		`MYSQL_DATABASE=testdb`,
	}

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
	return addr, nil, func() {
		closer()
	}
}

func TestMysql(t *testing.T) {
	sqltest.TestAll(t, Type, makeMysql, nil)
}
