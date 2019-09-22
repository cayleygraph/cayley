package sql

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/log"
	"github.com/cayleygraph/quad"
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
	Driver             string // sql driver to use on dial
	HashType           string // type for hash fields
	BytesType          string // type for binary fields
	TimeType           string // type for datetime fields
	HorizonType        string // type for horizon counter
	NodesTableExtra    string // extra SQL to append to nodes table definition
	ConditionalIndexes bool   // database supports conditional indexes
	FillFactor         bool   // database supports fill percent on indexes
	NoForeignKeys      bool   // database has no support for FKs

	QueryDialect
	NoOffsetWithoutLimit bool // SELECT ... OFFSET can be used only with LIMIT

	Error               func(error) error         // error conversion function
	Estimated           func(table string) string // query that string that returns an estimated number of rows in table
	RunTx               func(tx *sql.Tx, nodes []graphlog.NodeUpdate, quads []graphlog.QuadUpdate, opts graph.IgnoreOpts) error
	TxRetry             func(tx *sql.Tx, stmts func() error) error
	NoSchemaChangesInTx bool
}

func (r Registration) nodesTable() string {
	htyp := r.HashType
	if htyp == "" {
		htyp = "BYTEA"
	}
	btyp := r.BytesType
	if btyp == "" {
		btyp = "BYTEA"
	}
	ttyp := r.TimeType
	if ttyp == "" {
		ttyp = "timestamp with time zone"
	}
	end := "\n);"
	if r.NodesTableExtra != "" {
		end = ",\n" + r.NodesTableExtra + end
	}
	return `CREATE TABLE nodes (
	hash ` + htyp + ` PRIMARY KEY,
	refs INT NOT NULL,
	value ` + btyp + `,
	value_string TEXT,
	datatype TEXT,
	language TEXT,
	iri BOOLEAN,
	bnode BOOLEAN,
	value_int BIGINT,
	value_bool BOOLEAN,
	value_float double precision,
	value_time ` + ttyp +
		end
}

func (r Registration) quadsTable() string {
	htyp := r.HashType
	if htyp == "" {
		htyp = "BYTEA"
	}
	hztyp := r.HorizonType
	if hztyp == "" {
		hztyp = "SERIAL"
	}
	return `CREATE TABLE quads (
	horizon ` + hztyp + ` PRIMARY KEY,
	subject_hash ` + htyp + ` NOT NULL,
	predicate_hash ` + htyp + ` NOT NULL,
	object_hash ` + htyp + ` NOT NULL,
	label_hash ` + htyp + `,
	ts timestamp
);`
}

func (r Registration) quadIndexes(options graph.Options) []string {
	indexes := make([]string, 0, 10)
	if r.ConditionalIndexes {
		indexes = append(indexes,
			`CREATE UNIQUE INDEX spo_unique ON quads (subject_hash, predicate_hash, object_hash) WHERE label_hash IS NULL;`,
			`CREATE UNIQUE INDEX spol_unique ON quads (subject_hash, predicate_hash, object_hash, label_hash) WHERE label_hash IS NOT NULL;`,
		)
	} else {
		indexes = append(indexes,
			`CREATE UNIQUE INDEX spo_unique ON quads (subject_hash, predicate_hash, object_hash);`,
			`CREATE UNIQUE INDEX spol_unique ON quads (subject_hash, predicate_hash, object_hash, label_hash);`,
		)
	}
	if !r.NoForeignKeys {
		indexes = append(indexes,
			`ALTER TABLE quads ADD CONSTRAINT subject_hash_fk FOREIGN KEY (subject_hash) REFERENCES nodes (hash);`,
			`ALTER TABLE quads ADD CONSTRAINT predicate_hash_fk FOREIGN KEY (predicate_hash) REFERENCES nodes (hash);`,
			`ALTER TABLE quads ADD CONSTRAINT object_hash_fk FOREIGN KEY (object_hash) REFERENCES nodes (hash);`,
			`ALTER TABLE quads ADD CONSTRAINT label_hash_fk FOREIGN KEY (label_hash) REFERENCES nodes (hash);`,
		)
	}
	quadIndexes := [][3]quad.Direction{
		{quad.Subject, quad.Predicate, quad.Object},
		{quad.Object, quad.Predicate, quad.Subject},
		{quad.Predicate, quad.Object, quad.Subject},
		{quad.Object, quad.Subject, quad.Predicate},
	}
	factor, _ := options.IntKey("db_fill_factor", 50)
	for _, ind := range quadIndexes {
		var (
			name string
			cols []string
		)
		for _, d := range ind {
			name += string(d.Prefix())
			cols = append(cols, d.String()+"_hash")
		}
		q := fmt.Sprintf(`CREATE INDEX %s_index ON quads (%s)`,
			name, strings.Join(cols, ", "))
		if r.FillFactor {
			q += fmt.Sprintf(" WITH (FILLFACTOR = %d)", factor)
		}
		indexes = append(indexes, q+";")
	}
	return indexes
}
