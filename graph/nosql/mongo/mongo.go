package mongo

import (
	"github.com/cayleygraph/cayley/graph"
	gnosql "github.com/cayleygraph/cayley/graph/nosql"
	"github.com/hidal-go/hidalgo/legacy/nosql"
	"github.com/hidal-go/hidalgo/legacy/nosql/mongo"
)

const Type = mongo.Name

func Create(addr string, opt graph.Options) (nosql.Database, error) {
	return mongo.Dial(addr, gnosql.DefaultDBName, nosql.Options(opt))
}

func Open(addr string, opt graph.Options) (nosql.Database, error) {
	return mongo.Dial(addr, gnosql.DefaultDBName, nosql.Options(opt))
}
