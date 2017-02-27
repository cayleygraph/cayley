package sql

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/codelingo/cayley/clog"
	"github.com/codelingo/cayley/graph"
	"github.com/codelingo/cayley/quad"
	_ "github.com/go-sql-driver/mysql"
)

const flavorMysql = "mysql"

func init() {
	hs := fmt.Sprint(quad.HashSize)
	RegisterFlavor(Flavor{
		Name: flavorMysql,
		NodesTable: `CREATE TABLE nodes (
	hash BINARY(` + hs + `) PRIMARY KEY,
	value BLOB,
	value_string TEXT,
	datatype TEXT,
	language TEXT,
	iri BOOLEAN,
	bnode BOOLEAN,
	value_int BIGINT,
	value_bool BOOLEAN,
	value_float double precision,
	value_time DATETIME(6)
);`,
		QuadsTable: `CREATE TABLE quads (
	horizon SERIAL PRIMARY KEY,
	subject_hash BINARY(` + hs + `) NOT NULL,
	predicate_hash BINARY(` + hs + `) NOT NULL,
	object_hash BINARY(` + hs + `) NOT NULL,
	label_hash BINARY(` + hs + `),
	id BIGINT,
	ts timestamp
);`,
		FieldQuote:  '`',
		Placeholder: func(n int) string { return "?" },
		Indexes: func(options graph.Options) []string {
			return []string{
				`CREATE UNIQUE INDEX spo_unique ON quads (subject_hash, predicate_hash, object_hash);`,
				`CREATE UNIQUE INDEX spol_unique ON quads (subject_hash, predicate_hash, object_hash, label_hash);`,
				`CREATE INDEX spo_index ON quads (subject_hash);`,
				`CREATE INDEX pos_index ON quads (predicate_hash);`,
				`CREATE INDEX osp_index ON quads (object_hash);`,
				`ALTER TABLE quads ADD CONSTRAINT subject_hash_fk FOREIGN KEY (subject_hash) REFERENCES nodes (hash);`,
				`ALTER TABLE quads ADD CONSTRAINT predicate_hash_fk FOREIGN KEY (predicate_hash) REFERENCES nodes (hash);`,
				`ALTER TABLE quads ADD CONSTRAINT object_hash_fk FOREIGN KEY (object_hash) REFERENCES nodes (hash);`,
				`ALTER TABLE quads ADD CONSTRAINT label_hash_fk FOREIGN KEY (label_hash) REFERENCES nodes (hash);`,
			}
		},
		Error: func(err error) error {
			return err
		},
		Estimated: nil,
		RunTx:     runTxMysql,
	})
}

func runTxMysql(tx *sql.Tx, in []graph.Delta, opts graph.IgnoreOpts) error {
	ignore := ""
	if opts.IgnoreDup {
		ignore = " IGNORE"
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
				insertQuad, err = tx.Prepare(`INSERT` + ignore + ` INTO quads(subject_hash, predicate_hash, object_hash, label_hash, id, ts) VALUES (?, ?, ?, ?, ?, ?);`)
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
						ph[i] = "?"
					}
					stmt, err = tx.Prepare(`INSERT IGNORE INTO nodes(hash, ` +
						strings.Join(nodeInsertColumns[nodeKey], ", ") +
						`) VALUES (?, ` +
						strings.Join(ph, ", ") +
						`);`)
					if err != nil {
						return err
					}
					insertValue[nodeKey] = stmt
				}
				_, err = stmt.Exec(values...)
				err = convInsertErrorMySql(err)
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
			err = convInsertErrorMySql(err)
			if err != nil {
				clog.Errorf("couldn't exec INSERT statement: %v", err)
				return err
			}
		case graph.Delete:
			if deleteQuad == nil {
				deleteQuad, err = tx.Prepare(`DELETE FROM quads WHERE subject_hash=? and predicate_hash=? and object_hash=? and label_hash=?;`)
				if err != nil {
					return err
				}
				deleteTriple, err = tx.Prepare(`DELETE FROM quads WHERE subject_hash=? and predicate_hash=? and object_hash=? and label_hash is null;`)
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

func convInsertErrorMySql(err error) error {
	return err // TODO
}
