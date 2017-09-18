package sql

import (
	"github.com/cayleygraph/cayley/graph"
	"database/sql"
)

var types = make(map[string]Registration)

func Register(name string, f Registration) {
	if f.Driver == "" {
		panic("no sql driver in type definition")
	}
	types[name] = f

	registerQuadStore(name, name)
}

type Registration struct {
	Driver              string // sql driver to use on dial
	NodesTable          string // table definition for nodes
	QuadsTable          string // table definition for quads
	FieldQuote          rune // rune to use as field quote
	NumericPlaceholders bool // use numeric placeholders ($1, $2) instead of '?' placeholders
	Placeholder         func(int) string
	Indexes             func(graph.Options) []string // statements to build indexes on all tables
	Error               func(error) error // error conversion function
	Estimated           func(table string) string // query that string that returns an estimated number of rows in table
	RunTx               func(tx *sql.Tx, in []graph.Delta, opts graph.IgnoreOpts) error
	NoSchemaChangesInTx bool
}