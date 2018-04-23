// +build docker

package elastic

import (
	"testing"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/nosql"
	"github.com/cayleygraph/cayley/graph/nosql/nosqltest"
	"github.com/cayleygraph/cayley/internal/dock"
)

var versions = []struct {
	Vers   string
	Legacy bool
}{
	{Vers: "6.2.4"},
	{Vers: "5.6.9", Legacy: true},
}

func makeElasticVersion(vers string, legacy bool) nosqltest.DatabaseFunc {
	return func(t testing.TB) (nosql.Database, *nosql.Options, graph.Options, func()) {
		var conf dock.Config

		name := "docker.elastic.co/elasticsearch/elasticsearch-oss"
		if legacy {
			name = "elasticsearch"
		}
		conf.Image = name + ":" + vers
		conf.OpenStdin = true
		conf.Tty = true

		// Running this command might be necessary on the host:
		// sysctl -w vm.max_map_count=262144

		addr, closer := dock.RunAndWait(t, conf, "9200", nil)
		addr = "http://" + addr

		db, err := dialDB(addr, nil)
		if err != nil {
			closer()
			t.Fatal(addr, err)
		}
		return db, nil, nil, func() {
			db.Close()
			closer()
		}
	}
}

var conf = &nosqltest.Config{
	FloatToInt: true,
}

func TestElastic(t *testing.T) {
	for _, v := range versions {
		v := v
		t.Run(v.Vers, func(t *testing.T) {
			nosqltest.TestAll(t, makeElasticVersion(v.Vers, v.Legacy), conf)
		})
	}
}

func BenchmarkElastic(t *testing.B) {
	for _, v := range versions {
		v := v
		t.Run(v.Vers, func(t *testing.B) {
			nosqltest.BenchmarkAll(t, makeElasticVersion(v.Vers, v.Legacy), conf)
		})
	}
}
