package mysql

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	csql "github.com/cayleygraph/cayley/graph/sql"
	"github.com/cayleygraph/cayley/quad"
	_ "github.com/go-sql-driver/mysql"
)

const Type = "mysql"

var QueryDialect = csql.QueryDialect{
	FieldQuote: func(name string) string {
		return "`" + name + "`"
	},
	Placeholder: func(n int) string { return "?" },
}

func init() {
	csql.Register(Type, csql.Registration{
		Driver:               "mysql",
		HashType:             fmt.Sprintf(`BINARY(%d)`, quad.HashSize),
		BytesType:            `BLOB`,
		HorizonType:          `SERIAL`,
		TimeType:             `DATETIME(6)`,
		QueryDialect:         QueryDialect,
		NoOffsetWithoutLimit: true,
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
				insertQuad, err = tx.Prepare(`INSERT` + ignore + ` INTO quads(subject_hash, predicate_hash, object_hash, label_hash, ts) VALUES (?, ?, ?, ?, now());`)
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
						ph[i] = "?"
					}
					stmt, err = tx.Prepare(`INSERT IGNORE INTO nodes(hash, ` +
						strings.Join(nodeKey.Columns(), ", ") +
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
				hs.SQLValue(), hp.SQLValue(), ho.SQLValue(), hl.SQLValue(),
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

func convInsertErrorMySql(err error) error {
	return err // TODO
}
