package mysql

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/log"
	csql "github.com/cayleygraph/cayley/graph/sql"
	"github.com/cayleygraph/quad"
	"github.com/go-sql-driver/mysql"
)

const Type = "mysql"

var QueryDialect = csql.QueryDialect{
	RegexpOp: "REGEXP",
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

func runTxMysql(tx *sql.Tx, nodes []graphlog.NodeUpdate, quads []graphlog.QuadUpdate, opts graph.IgnoreOpts) error {
	// update node ref counts and insert nodes
	var (
		// prepared statements for each value type
		insertValue = make(map[csql.ValueType]*sql.Stmt)
		updateValue *sql.Stmt
	)
	for _, n := range nodes {
		if n.RefInc >= 0 {
			nodeKey, values, err := csql.NodeValues(csql.NodeHash{n.Hash}, n.Val)
			if err != nil {
				return err
			}
			values = append([]interface{}{n.RefInc}, values...)
			values = append(values, n.RefInc) // one more time for UPDATE
			stmt, ok := insertValue[nodeKey]
			if !ok {
				var ph = make([]string, len(values)-1) // excluding last increment
				for i := range ph {
					ph[i] = "?"
				}
				stmt, err = tx.Prepare(`INSERT INTO nodes(refs, hash, ` +
					strings.Join(nodeKey.Columns(), ", ") +
					`) VALUES (` + strings.Join(ph, ", ") +
					`) ON DUPLICATE KEY UPDATE refs = refs + ?;`)
				if err != nil {
					return err
				}
				insertValue[nodeKey] = stmt
			}
			_, err = stmt.Exec(values...)
			err = convInsertError(err)
			if err != nil {
				clog.Errorf("couldn't exec INSERT statement: %v", err)
				return err
			}
		} else {
			panic("unexpected node update")
		}
	}
	for _, s := range insertValue {
		s.Close()
	}
	if s := updateValue; s != nil {
		s.Close()
	}
	insertValue = nil
	updateValue = nil

	// now we can deal with quads
	ignore := ""
	if opts.IgnoreDup {
		ignore = " IGNORE"
	}

	var (
		insertQuad *sql.Stmt
		err        error
	)
	for _, d := range quads {
		dirs := make([]interface{}, 0, len(quad.Directions))
		for _, h := range d.Quad.Dirs() {
			dirs = append(dirs, csql.NodeHash{h}.SQLValue())
		}
		if !d.Del {
			if insertQuad == nil {
				insertQuad, err = tx.Prepare(`INSERT` + ignore + ` INTO quads(subject_hash, predicate_hash, object_hash, label_hash, ts) VALUES (?, ?, ?, ?, now());`)
				if err != nil {
					return err
				}
				insertValue = make(map[csql.ValueType]*sql.Stmt)
			}
			_, err := insertQuad.Exec(dirs...)
			err = convInsertError(err)
			if err != nil {
				if _, ok := err.(*graph.DeltaError); !ok {
					clog.Errorf("couldn't exec INSERT statement: %v", err)
				}
				return err
			}
		} else {
			panic("unexpected quad delete")
		}
	}
	return nil
}

func convInsertError(err error) error {
	if err == nil {
		return nil
	}
	if e, ok := err.(*mysql.MySQLError); ok {
		if e.Number == 1062 {
			// TODO: reference to delta
			return &graph.DeltaError{Err: graph.ErrQuadExists}
		}
	}
	return err
}
