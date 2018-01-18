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

		addr, closer := dock.RunAndWait(t, conf, "3306", nil)
		addr = `root:root@tcp(` + addr + `)/testdb`
		return addr, nil, func() {
			closer()
		}
	}
}

const (
	mysqlImage   = "mysql:5.7"
	mariadbImage = "mariadb:10"
)

func TestMysql(t *testing.T) {
	sqltest.TestAll(t, Type, makeMysqlVersion(mysqlImage), nil)
}

func TestMariaDB(t *testing.T) {
	sqltest.TestAll(t, Type, makeMysqlVersion(mariadbImage), nil)
}

func BenchmarkMysql(t *testing.B) {
	sqltest.BenchmarkAll(t, Type, makeMysqlVersion(mysqlImage), nil)
}

func BenchmarkMariadb(t *testing.B) {
	sqltest.BenchmarkAll(t, Type, makeMysqlVersion(mariadbImage), nil)
}
