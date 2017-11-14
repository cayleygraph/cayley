// +build docker

package mysql

import (
	"testing"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/sql/sqltest"
	"github.com/cayleygraph/cayley/internal/dock"
)

func makeMysqlVersion(image string) sqltest.DatabaseFunc {
	return func(t testing.TB) (string, graph.Options, func()) {
		var conf dock.Config

		conf.Image = image
		conf.Tty = true
		conf.Env = []string{
			`MYSQL_ROOT_PASSWORD=root`,
			`MYSQL_DATABASE=testdb`,
		}

		addr, closer := dock.RunAndWait(t, conf, dock.WaitPort("3306"))
		addr = `root:root@tcp(` + addr + `:3306)/testdb`
		return addr, nil, func() {
			closer()
		}
	}
}

func TestMysql(t *testing.T) {
	sqltest.TestAll(t, Type, makeMysqlVersion("mysql:5.7"), nil)
}

func TestMariaDB(t *testing.T) {
	sqltest.TestAll(t, Type, makeMysqlVersion("mariadb:10"), nil)
}
