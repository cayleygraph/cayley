package sql

import (
	"database/sql"
	"fmt"

	"github.com/cayleygraph/cayley/graph"
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
	RunTx               func(tx *sql.Tx, in []graph.Delta, opts graph.IgnoreOpts) error
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
	id BIGINT,
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
	if r.FillFactor {
		const defaultFillFactor = 50
		factor, ok, _ := options.IntKey("db_fill_factor")
		if !ok {
			factor = defaultFillFactor
		}
		indexes = append(indexes,
			fmt.Sprintf(`CREATE INDEX spo_index ON quads (subject_hash) WITH (FILLFACTOR = %d);`, factor),
			fmt.Sprintf(`CREATE INDEX pos_index ON quads (predicate_hash) WITH (FILLFACTOR = %d);`, factor),
			fmt.Sprintf(`CREATE INDEX osp_index ON quads (object_hash) WITH (FILLFACTOR = %d);`, factor),
		)
	} else {
		indexes = append(indexes,
			`CREATE INDEX spo_index ON quads (subject_hash);`,
			`CREATE INDEX pos_index ON quads (predicate_hash);`,
			`CREATE INDEX osp_index ON quads (object_hash);`,
		)
	}
	return indexes
}
