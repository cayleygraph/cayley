package postgres

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/log"
	csql "github.com/cayleygraph/cayley/graph/sql"
	"github.com/cayleygraph/quad"
	"github.com/lib/pq"
)

const Type = "postgres"

var QueryDialect = csql.QueryDialect{
	RegexpOp:   "~",
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

func convInsertError(err error) error {
	if err == nil {
		return err
	}
	if pe, ok := err.(*pq.Error); ok {
		if pe.Code == "23505" {
			// TODO: reference to delta
			return &graph.DeltaError{Err: graph.ErrQuadExists}
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

func RunTxPostgres(tx *sql.Tx, nodes []graphlog.NodeUpdate, quads []graphlog.QuadUpdate, opts graph.IgnoreOpts) error {
	return RunTx(tx, nodes, quads, opts, "")
}

func RunTx(tx *sql.Tx, nodes []graphlog.NodeUpdate, quads []graphlog.QuadUpdate, opts graph.IgnoreOpts, onConflict string) error {
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
			stmt, ok := insertValue[nodeKey]
			if !ok {
				var ph = make([]string, len(values))
				for i := range ph {
					ph[i] = "$" + strconv.FormatInt(int64(i)+1, 10)
				}
				stmt, err = tx.Prepare(`INSERT INTO nodes(refs, hash, ` +
					strings.Join(nodeKey.Columns(), ", ") +
					`) VALUES (` + strings.Join(ph, ", ") +
					`) ON CONFLICT (hash) DO UPDATE SET refs = nodes.refs + EXCLUDED.refs;`)
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

	// TODO: copy
	//if allAdds && !opts.IgnoreDup {
	//	return qs.copyFrom(tx, in, opts)
	//}

	end := ";"
	if opts.IgnoreDup {
		end = ` ON CONFLICT ` + onConflict + ` DO NOTHING;`
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
				insertQuad, err = tx.Prepare(`INSERT INTO quads(subject_hash, predicate_hash, object_hash, label_hash, ts) VALUES ($1, $2, $3, $4, now())` + end)
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
