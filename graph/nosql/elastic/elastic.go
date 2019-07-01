package elastic

import (
	"github.com/cayleygraph/cayley/graph"
	gnosql "github.com/cayleygraph/cayley/graph/nosql"
	"github.com/hidal-go/hidalgo/legacy/nosql"
	"github.com/hidal-go/hidalgo/legacy/nosql/elastic"
)

const Type = elastic.Name

func Create(addr string, opt graph.Options) (nosql.Database, error) {
	return elastic.Dial(addr, gnosql.DefaultDBName, nosql.Options(opt))
}

func Open(addr string, opt graph.Options) (nosql.Database, error) {
	return elastic.Dial(addr, gnosql.DefaultDBName, nosql.Options(opt))
}
