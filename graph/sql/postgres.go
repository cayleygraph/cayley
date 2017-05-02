package sql

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/codelingo/cayley/clog"
	"github.com/codelingo/cayley/graph"
	"github.com/codelingo/cayley/quad"
	"github.com/lib/pq"
)

const flavorPostgres = "postgres"

const defaultFillFactor = 50

func init() {
	RegisterFlavor(Flavor{
		Name: flavorPostgres,
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
	value_time timestamp with time zone
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
			factor, factorOk, _ := options.IntKey("db_fill_factor")
			if !factorOk {
				factor = defaultFillFactor
			}
			return []string{
				`CREATE UNIQUE INDEX spol_unique ON quads (subject_hash, predicate_hash, object_hash, label_hash) WHERE label_hash IS NOT NULL;`,
				`CREATE UNIQUE INDEX spo_unique ON quads (subject_hash, predicate_hash, object_hash) WHERE label_hash IS NULL;`,
				`ALTER TABLE quads ADD CONSTRAINT subject_hash_fk FOREIGN KEY (subject_hash) REFERENCES nodes (hash);`,
				`ALTER TABLE quads ADD CONSTRAINT predicate_hash_fk FOREIGN KEY (predicate_hash) REFERENCES nodes (hash);`,
				`ALTER TABLE quads ADD CONSTRAINT object_hash_fk FOREIGN KEY (object_hash) REFERENCES nodes (hash);`,
				`ALTER TABLE quads ADD CONSTRAINT label_hash_fk FOREIGN KEY (label_hash) REFERENCES nodes (hash);`,
				fmt.Sprintf(`CREATE INDEX spo_index ON quads (subject_hash) WITH (FILLFACTOR = %d);`, factor),
				fmt.Sprintf(`CREATE INDEX pos_index ON quads (predicate_hash) WITH (FILLFACTOR = %d);`, factor),
				fmt.Sprintf(`CREATE INDEX osp_index ON quads (object_hash) WITH (FILLFACTOR = %d);`, factor),
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
		Estimated: func(table string) string {
			return "SELECT reltuples::BIGINT AS estimate FROM pg_class WHERE relname='" + table + "';"
		},
		RunTx:     runTxPostgres,
		RunChanTx: runChanTxPostgres,
	})
}

func runChanTxPostgres(tx *sql.Tx, tx2 *sql.Tx, in <-chan graph.Delta, opts graph.IgnoreOpts) error {

	type quadValue struct {
		hs, hp, ho, hl NodeHash
		id             graph.PrimaryKey
		timestamp      time.Time
	}

	quads := make(chan quadValue)
	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		_, err := tx2.Exec("CREATE TEMP TABLE IF NOT EXISTS quads_copy (LIKE quads INCLUDING ALL);")
		if err != nil {
			panic(err)
		}

		quadStmt, err := tx2.Prepare(pq.CopyIn("quads_copy", "subject_hash", "predicate_hash", "object_hash", "label_hash", "id", "ts"))
		if err != nil {
			panic(err)
			clog.Errorf("couldn't prepare COPY statement: %v", err)
		}

		for quValue := range quads {
			_, err = quadStmt.Exec(
				quValue.hs.toSQL(),
				quValue.hp.toSQL(),
				quValue.ho.toSQL(),
				quValue.hl.toSQL(),
				quValue.id.Int(),
				quValue.timestamp,
			)
			err = convInsertErrorPG(err)
			if err != nil {
				panic(err)
				clog.Errorf("couldn't exec COPY statement: %v", err)
			}
		}

		// flush
		_, err = quadStmt.Exec()
		if err != nil {
			err = convInsertErrorPG(err)
			panic(err)
		}
		_ = quadStmt.Close() // COPY will be closed on last Exec, this will return non-nil error in all cases

		wg.Done()
	}()

	go func() {
		_, err := tx.Exec("CREATE TEMP TABLE IF NOT EXISTS nodes_copy (LIKE nodes INCLUDING ALL);")
		if err != nil {
			panic(err)
		}

		inserted := make(map[NodeHash]struct{}) // tracks already inserted values
		nodeStmt, err := tx.Prepare(pq.CopyIn("nodes_copy", nodesColumns...))
		if err != nil {
			panic(err)
			clog.Errorf("couldn't prepare COPY statement: %v", err)
		}

		for d := range in {
			var hs, hp, ho, hl NodeHash
			// var quadExists bool
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
					panic(err)
				}

				// create an array of values to insert, only set the values of the array
				// indexes that align with the columns we wish to populate.
				insertValues := make([]interface{}, len(nodesColumns))

				// hash is not included in nodeInsertColumns, but is always first
				insertValues[0] = values[0]

				// populate other values from array map
				for i, valCol := range nodeInsertColumns[nodeKey] {
					for nodeNo, nodeCol := range nodesColumns {
						if valCol == nodeCol {
							// nodeInsertColumns does not have hash, so need to add one to
							// index to skip the first entry.
							insertValues[nodeNo] = values[i+1]
							break
						}
					}
				}

				_, err = nodeStmt.Exec(insertValues...)
				err = convInsertErrorPG(err)
				if err != nil {
					if !strings.Contains(err.Error(), "quad exists") {
						panic(err)
						clog.Errorf("couldn't exec COPY statement: %v", err)
					}
					// quadExists = true
				}

				inserted[h] = struct{}{}
			}
			quads <- quadValue{hs, hp, ho, hl, d.ID, d.Timestamp}
		}
		close(quads)

		_, err = nodeStmt.Exec()
		if err != nil {
			panic(err)
			err = convInsertErrorPG(err)
		}
		_ = nodeStmt.Close() // COPY will be closed on last Exec, this will return non-nil error in all cases

		var doNothing string
		if opts.IgnoreDup {
			doNothing = " ON CONFLICT (hash) DO NOTHING"
		}

		// sync copy tables back to nodes table
		_, err = tx.Exec("INSERT INTO nodes SELECT * FROM nodes_copy" + doNothing + ";")
		if err != nil {
			panic(err)
		}
		if err := tx.Commit(); err != nil {
			panic(err)
			err = convInsertErrorPG(err)
		}

		wg.Done()
	}()

	// nodes tb needs to be set up before quads
	wg.Wait()

	_, err := tx2.Exec("INSERT INTO quads SELECT * FROM quads_copy;")
	if err != nil {
		panic(err)
	}

	return tx2.Commit()
}

func convInsertErrorPG(err error) error {
	if err == nil {
		return err
	}
	if pe, ok := err.(*pq.Error); ok {
		if pe.Code == "23505" {
			return graph.ErrQuadExists
		}
	}
	return err
}

func runTxPostgres(tx *sql.Tx, in []graph.Delta, opts graph.IgnoreOpts) error {
	end := ";"
	if opts.IgnoreDup {
		end = " ON CONFLICT DO NOTHING;"
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
						`) ON CONFLICT DO NOTHING;`)
					if err != nil {
						return err
					}
					insertValue[nodeKey] = stmt
				}
				_, err = stmt.Exec(values...)
				err = convInsertErrorPG(err)
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
			err = convInsertErrorPG(err)
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
