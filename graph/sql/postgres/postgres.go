package postgres

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	csql "github.com/cayleygraph/cayley/graph/sql"
	"github.com/cayleygraph/cayley/quad"
	"github.com/lib/pq"
)

const Type = "postgres"

var QueryDialect = csql.QueryDialect{
	FieldQuote: pq.QuoteIdentifier,
	Placeholder: func(n int) string {
		return fmt.Sprintf("$%d", n)
	},
}

func init() {
	csql.Register(Type, csql.Registration{
		Driver:             "postgres",
		HashType:           `BYTEA`,
		BytesType:          `BYTEA`,
		HorizonType:        `BIGSERIAL`,
		TimeType:           `timestamp with time zone`,
		QueryDialect:       QueryDialect,
		ConditionalIndexes: true,
		FillFactor:         true,
		Error:              ConvError,
		Estimated: func(table string) string {
			return "SELECT reltuples::BIGINT AS estimate FROM pg_class WHERE relname='" + table + "';"
		},
		RunTx: RunTxPostgres,
	})
}

func ConvError(err error) error {
	e, ok := err.(*pq.Error)
	if !ok {
		return err
	}
	switch e.Code {
	case "42P07":
		return graph.ErrDatabaseExists
	}
	return err
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

//func copyFromPG(tx *sql.Tx, in []graph.Delta, opts graph.IgnoreOpts) error {
//	panic("broken")
//	stmt, err := tx.Prepare(pq.CopyIn("quads", "subject", "predicate", "object", "label", "id", "ts", "subject_hash", "predicate_hash", "object_hash", "label_hash"))
//	if err != nil {
//		clog.Errorf("couldn't prepare COPY statement: %v", err)
//		return err
//	}
//	for _, d := range in {
//		s, p, o, l, err := marshalQuadDirections(d.Quad)
//		if err != nil {
//			clog.Errorf("couldn't marshal quads: %v", err)
//			return err
//		}
//		_, err = stmt.Exec(
//			s,
//			p,
//			o,
//			l,
//			d.ID.Int(),
//			d.Timestamp,
//			hashOf(d.Quad.Subject),
//			hashOf(d.Quad.Predicate),
//			hashOf(d.Quad.Object),
//			hashOf(d.Quad.Label),
//		)
//		if err != nil {
//			err = convInsertErrorPG(err)
//			clog.Errorf("couldn't execute COPY statement: %v", err)
//			return err
//		}
//	}
//	_, err = stmt.Exec()
//	if err != nil {
//		err = convInsertErrorPG(err)
//		return err
//	}
//	_ = stmt.Close() // COPY will be closed on last Exec, this will return non-nil error in all cases
//	return nil
//}

func RunTxPostgres(tx *sql.Tx, in []graph.Delta, opts graph.IgnoreOpts) error {
	return RunTx(tx, in, opts, "")
}

func RunTx(tx *sql.Tx, in []graph.Delta, opts graph.IgnoreOpts, onConflict string) error {
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
	if opts.IgnoreDup {
		end = ` ON CONFLICT ` + onConflict + ` DO NOTHING;`
	}

	var (
		insertQuad  *sql.Stmt
		insertValue map[csql.ValueType]*sql.Stmt // prepared statements for each value type
		inserted    map[csql.NodeHash]struct{}   // tracks already inserted values

		deleteQuad   *sql.Stmt
		deleteTriple *sql.Stmt
	)

	var err error
	for _, d := range in {
		switch d.Action {
		case graph.Add:
			if insertQuad == nil {
				insertQuad, err = tx.Prepare(`INSERT INTO quads(subject_hash, predicate_hash, object_hash, label_hash, ts) VALUES ($1, $2, $3, $4, now())` + end)
				if err != nil {
					return err
				}
				insertValue = make(map[csql.ValueType]*sql.Stmt)
				inserted = make(map[csql.NodeHash]struct{}, len(in))
			}
			var hs, hp, ho, hl csql.NodeHash
			for _, dir := range quad.Directions {
				v := d.Quad.Get(dir)
				if v == nil {
					continue
				}
				h := csql.HashOf(v)
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
				nodeKey, values, err := csql.NodeValues(h, v)
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
						strings.Join(nodeKey.Columns(), ", ") +
						`) VALUES ($1, ` +
						strings.Join(ph, ", ") +
						`) ON CONFLICT (hash) DO NOTHING;`)
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
				hs.SQLValue(), hp.SQLValue(), ho.SQLValue(), hl.SQLValue(),
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
				result, err = deleteTriple.Exec(
					csql.HashOf(d.Quad.Subject).SQLValue(),
					csql.HashOf(d.Quad.Predicate).SQLValue(),
					csql.HashOf(d.Quad.Object).SQLValue(),
				)
			} else {
				result, err = deleteQuad.Exec(
					csql.HashOf(d.Quad.Subject).SQLValue(),
					csql.HashOf(d.Quad.Predicate).SQLValue(),
					csql.HashOf(d.Quad.Object).SQLValue(),
					csql.HashOf(d.Quad.Label).SQLValue(),
				)
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
