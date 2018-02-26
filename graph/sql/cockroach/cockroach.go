package cockroach

import (
	"bytes"
	"database/sql"
	"fmt"
	"strings"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	graphlog "github.com/cayleygraph/cayley/graph/log"
	csql "github.com/cayleygraph/cayley/graph/sql"
	"github.com/jackc/pgx"
	_ "github.com/jackc/pgx/stdlib" // registers "pgx" driver
)

const Type = "cockroach"

func init() {
	csql.Register(Type, csql.Registration{
		Driver:      "pgx",
		HashType:    `BYTEA`,
		BytesType:   `BYTEA`,
		HorizonType: `BIGSERIAL`,
		TimeType:    `timestamp with time zone`,
		NodesTableExtra: `
	FAMILY fhash (hash),
	FAMILY frefs (refs),
	FAMILY fvalue (value, value_string, datatype, language, iri, bnode,
		value_int, value_bool, value_float, value_time)
`,
		QueryDialect: csql.QueryDialect{
			RegexpOp: "~",
			FieldQuote: func(name string) string {
				return pgx.Identifier{name}.Sanitize()
			},
			Placeholder: func(n int) string {
				return fmt.Sprintf("$%d", n)
			},
		},
		NoForeignKeys: true,
		Error:         convError,
		//Estimated: func(table string) string{
		//	return "SELECT reltuples::BIGINT AS estimate FROM pg_class WHERE relname='"+table+"';"
		//},
		RunTx:               runTxCockroach,
		TxRetry:             retryTxCockroach,
		NoSchemaChangesInTx: true,
	})
}

// AmbiguousCommitError represents an error that left a transaction in an
// ambiguous state: unclear if it committed or not.
type AmbiguousCommitError struct {
	error
}

// retryTxCockroach runs the transaction and will retry in case of a retryable error.
// https://www.cockroachlabs.com/docs/transactions.html#client-side-transaction-retries
func retryTxCockroach(tx *sql.Tx, stmts func() error) error {
	// Specify that we intend to retry this txn in case of CockroachDB retryable
	// errors.
	if _, err := tx.Exec("SAVEPOINT cockroach_restart"); err != nil {
		return err
	}

	for {
		released := false

		err := stmts()

		if err == nil {
			// RELEASE acts like COMMIT in CockroachDB. We use it since it gives us an
			// opportunity to react to retryable errors, whereas tx.Commit() doesn't.
			released = true
			if _, err = tx.Exec("RELEASE SAVEPOINT cockroach_restart"); err == nil {
				return nil
			}
		}
		// We got an error; let's see if it's a retryable one and, if so, restart. We look
		// for either the standard PG errcode SerializationFailureError:40001 or the Cockroach extension
		// errcode RetriableError:CR000. The Cockroach extension has been removed server-side, but support
		// for it has been left here for now to maintain backwards compatibility.
		pgErr, ok := err.(pgx.PgError)
		if retryable := ok && (pgErr.Code == "CR000" || pgErr.Code == "40001"); !retryable {
			if released {
				err = &AmbiguousCommitError{err}
			}
			return err
		}
		if _, err = tx.Exec("ROLLBACK TO SAVEPOINT cockroach_restart"); err != nil {
			return err
		}
	}
}

func convError(err error) error {
	e, ok := err.(pgx.PgError)
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
	if pe, ok := err.(pgx.PgError); ok {
		if pe.Code == "23505" {
			// TODO: reference to delta
			return &graph.DeltaError{Err: graph.ErrQuadExists}
		}
	}
	return err
}

// runTxCockroach performs the node and quad updates in the provided transaction.
// This is based on ../postgres/postgres.go:RunTx, but focuses on doing fewer insert statements,
// since those are comparatively expensive for CockroachDB.
func runTxCockroach(tx *sql.Tx, nodes []graphlog.NodeUpdate, quads []graphlog.QuadUpdate, opts graph.IgnoreOpts) error {
	// First, compile the sets of nodes, split by csql.ValueType.
	// Each of those will require a separate INSERT statement.
	type nodeEntry struct {
		refInc int
		values []interface{} // usually two, but sometimes three elements (includes hash)
	}
	nodeEntries := make(map[csql.ValueType][]nodeEntry)
	for _, n := range nodes {
		if n.RefInc < 0 {
			panic("unexpected node update")
		}
		nodeType, values, err := csql.NodeValues(csql.NodeHash{n.Hash}, n.Val)
		if err != nil {
			return err
		}
		nodeEntries[nodeType] = append(nodeEntries[nodeType], nodeEntry{
			refInc: n.RefInc,
			values: values,
		})
	}

	// Next, build and execute the INSERT statements for each type.
	for nodeType, entries := range nodeEntries {
		var query bytes.Buffer
		var allValues []interface{}
		valCols := nodeType.Columns()
		fmt.Fprintf(&query, "INSERT INTO nodes (refs, hash, %s) VALUES ", strings.Join(valCols, ", "))
		ph := 1 // next placeholder counter
		for i, entry := range entries {
			if i > 0 {
				fmt.Fprint(&query, ", ")
			}
			fmt.Fprint(&query, "(")
			// sanity check
			if len(entry.values) != 1+len(valCols) { // +1 for hash, which is in values
				panic(fmt.Sprintf("internal error: %d entry values vs. %d value columns", len(entry.values), len(valCols)))
			}
			for j := 0; j < 1+len(entry.values); j++ { // +1 for refs
				if j > 0 {
					fmt.Fprint(&query, ", ")
				}
				fmt.Fprintf(&query, "$%d", ph)
				ph++
			}
			fmt.Fprint(&query, ")")
			allValues = append(allValues, entry.refInc)
			allValues = append(allValues, entry.values...)
		}
		fmt.Fprint(&query, " ON CONFLICT (hash) DO UPDATE SET refs = nodes.refs + EXCLUDED.refs RETURNING NOTHING;")
		_, err := tx.Exec(query.String(), allValues...)
		err = convInsertError(err)
		if err != nil {
			clog.Errorf("couldn't exec node INSERT statement [%s]: %v", query.String(), err)
			return err
		}
	}

	// Now do the same thing with quads.
	// It is simpler because there's only one composite type to insert,
	// so only one INSERT statement is required.
	if len(quads) == 0 {
		return nil
	}

	var query bytes.Buffer
	var allValues []interface{}
	fmt.Fprintf(&query, "INSERT INTO quads (subject_hash, predicate_hash, object_hash, label_hash, ts) VALUES ")
	for i, d := range quads {
		if d.Del {
			panic("unexpected quad delete")
		}
		if i > 0 {
			fmt.Fprint(&query, ", ")
		}
		fmt.Fprintf(&query, "($%d, $%d, $%d, $%d, now())", 4*i+1, 4*i+2, 4*i+3, 4*i+4)
		allValues = append(allValues,
			csql.NodeHash{d.Quad.Subject}.SQLValue(),
			csql.NodeHash{d.Quad.Predicate}.SQLValue(),
			csql.NodeHash{d.Quad.Object}.SQLValue(),
			csql.NodeHash{d.Quad.Label}.SQLValue())
	}
	if opts.IgnoreDup {
		fmt.Fprint(&query, " ON CONFLICT (subject_hash, predicate_hash, object_hash) DO NOTHING")
		// Only use RETURNING NOTHING when we're ignoring duplicates;
		// otherwise the error returned on duplicates will be different.
		fmt.Fprint(&query, " RETURNING NOTHING")
	}
	fmt.Fprint(&query, ";")
	_, err := tx.Exec(query.String(), allValues...)
	err = convInsertError(err)
	if err != nil {
		if _, ok := err.(*graph.DeltaError); !ok {
			clog.Errorf("couldn't exec quad INSERT statement [%s]: %v", query.String(), err)
		}
		return err
	}
	return nil
}
