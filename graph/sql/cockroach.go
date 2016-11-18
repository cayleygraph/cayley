package sql

import (
	"database/sql"
	"fmt"
	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/quad"
	"github.com/lib/pq"
	"strconv"
	"strings"
)

const flavorCockroach = "cockroach"

func init() {
	RegisterFlavor(Flavor{
		Name:   flavorCockroach,
		Driver: flavorPostgres,
		NodesTable: `CREATE TABLE nodes (
	hash BYTEA PRIMARY KEY,
	value BYTEA,
	value_string TEXT,
	datatype TEXT,
	language TEXT,
	iri BOOLEAN,
	bnode BOOLEAN,
	value_int BIGINT,
	value_bool BOOLEAN,
	value_float double precision,
	value_time timestamp with time zone,
	FAMILY fhash (hash),
	FAMILY fvalue (value, value_string, datatype, language, iri, bnode,
		value_int, value_bool, value_float, value_time)
);`,
		QuadsTable: `CREATE TABLE quads (
	horizon BIGSERIAL PRIMARY KEY,
	subject_hash BYTEA NOT NULL,
	predicate_hash BYTEA NOT NULL,
	object_hash BYTEA NOT NULL,
	label_hash BYTEA,
	id BIGINT,
	ts timestamp
);`,
		FieldQuote:  '"',
		Placeholder: func(n int) string { return fmt.Sprintf("$%d", n) },
		Indexes: func(options graph.Options) []string {
			return []string{
				`CREATE UNIQUE INDEX spol_unique ON quads (subject_hash, predicate_hash, object_hash, label_hash);`,
				`CREATE UNIQUE INDEX spo_unique ON quads (subject_hash, predicate_hash, object_hash);`,
				`CREATE INDEX spo_index ON quads (subject_hash);`,
				`CREATE INDEX pos_index ON quads (predicate_hash);`,
				`CREATE INDEX osp_index ON quads (object_hash);`,
				//`ALTER TABLE quads ADD CONSTRAINT subject_hash_fk FOREIGN KEY (subject_hash) REFERENCES nodes (hash);`,
				//`ALTER TABLE quads ADD CONSTRAINT predicate_hash_fk FOREIGN KEY (predicate_hash) REFERENCES nodes (hash);`,
				//`ALTER TABLE quads ADD CONSTRAINT object_hash_fk FOREIGN KEY (object_hash) REFERENCES nodes (hash);`,
				//`ALTER TABLE quads ADD CONSTRAINT label_hash_fk FOREIGN KEY (label_hash) REFERENCES nodes (hash);`,
			}
		},
		Error: func(err error) error {
			e, ok := err.(*pq.Error)
			if !ok {
				return err
			}
			switch e.Code {
			case "42P07":
				return graph.ErrDatabaseExists
			}
			return err
		},
		//Estimated: func(table string) string{
		//	return "SELECT reltuples::BIGINT AS estimate FROM pg_class WHERE relname='"+table+"';"
		//},
		RunTx: runTxCockroach,
	})
}

func runTxCockroach(tx *sql.Tx, in []graph.Delta, opts graph.IgnoreOpts) error {
	//allAdds := true
	//for _, d := range in {
	//	if d.Action != graph.Add {
	//		allAdds = false
	//	}
	//}
	//if allAdds && !opts.IgnoreDup {
	//	return qs.copyFrom(tx, in, opts)
	//}

	end := ";"
	if true || opts.IgnoreDup {
		end = " ON CONFLICT (subject_hash, predicate_hash, object_hash) DO NOTHING;"
	}

	var (
		insertQuad  *sql.Stmt
		insertValue map[int]*sql.Stmt     // prepared statements for each value type
		inserted    map[NodeHash]struct{} // tracks already inserted values

		deleteQuad   *sql.Stmt
		deleteTriple *sql.Stmt
	)

	var err error
	for _, d := range in {
		switch d.Action {
		case graph.Add:
			if insertQuad == nil {
				insertQuad, err = tx.Prepare(`INSERT INTO quads(subject_hash, predicate_hash, object_hash, label_hash, id, ts) VALUES ($1, $2, $3, $4, $5, $6)` + end)
				if err != nil {
					return err
				}
				insertValue = make(map[int]*sql.Stmt)
				inserted = make(map[NodeHash]struct{}, len(in))
			}
			var hs, hp, ho, hl NodeHash
			for _, dir := range quad.Directions {
				v := d.Quad.Get(dir)
				if v == nil {
					continue
				}
				h := hashOf(v)
				switch dir {
				case quad.Subject:
					hs = h
				case quad.Predicate:
					hp = h
				case quad.Object:
					ho = h
				case quad.Label:
					hl = h
				}
				if !h.Valid() {
					continue
				} else if _, ok := inserted[h]; ok {
					continue
				}
				nodeKey, values, err := nodeValues(h, v)
				if err != nil {
					return err
				}
				stmt, ok := insertValue[nodeKey]
				if !ok {
					var ph = make([]string, len(values)-1)
					for i := range ph {
						ph[i] = "$" + strconv.FormatInt(int64(i)+2, 10)
					}
					stmt, err = tx.Prepare(`INSERT INTO nodes(hash, ` +
						strings.Join(nodeInsertColumns[nodeKey], ", ") +
						`) VALUES ($1, ` +
						strings.Join(ph, ", ") +
						`) ON CONFLICT (hash) DO NOTHING;`)
					if err != nil {
						return err
					}
					insertValue[nodeKey] = stmt
				}
				_, err = stmt.Exec(values...)
				if err != nil {
					clog.Errorf("couldn't exec INSERT statement: %v", err)
					return err
				}
				inserted[h] = struct{}{}
			}
			_, err := insertQuad.Exec(
				hs.toSQL(), hp.toSQL(), ho.toSQL(), hl.toSQL(),
				d.ID.Int(),
				d.Timestamp,
			)
			if err != nil {
				clog.Errorf("couldn't exec INSERT statement: %v", err)
				return err
			}
		case graph.Delete:
			if deleteQuad == nil {
				deleteQuad, err = tx.Prepare(`DELETE FROM quads WHERE subject_hash=$1 and predicate_hash=$2 and object_hash=$3 and label_hash=$4;`)
				if err != nil {
					return err
				}
				deleteTriple, err = tx.Prepare(`DELETE FROM quads WHERE subject_hash=$1 and predicate_hash=$2 and object_hash=$3 and label_hash is null;`)
				if err != nil {
					return err
				}
			}
			var result sql.Result
			if d.Quad.Label == nil {
				result, err = deleteTriple.Exec(hashOf(d.Quad.Subject).toSQL(), hashOf(d.Quad.Predicate).toSQL(), hashOf(d.Quad.Object).toSQL())
			} else {
				result, err = deleteQuad.Exec(hashOf(d.Quad.Subject).toSQL(), hashOf(d.Quad.Predicate).toSQL(), hashOf(d.Quad.Object).toSQL(), hashOf(d.Quad.Label).toSQL())
			}
			if err != nil {
				clog.Errorf("couldn't exec DELETE statement: %v", err)
				return err
			}
			affected, err := result.RowsAffected()
			if err != nil {
				clog.Errorf("couldn't get DELETE RowsAffected: %v", err)
				return err
			}
			if affected != 1 && !opts.IgnoreMissing {
				return graph.ErrQuadNotExist
			}
		default:
			panic("unknown action")
		}
	}
	return nil
}
