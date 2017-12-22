package cockroach

import (
	"database/sql"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/log"
	csql "github.com/cayleygraph/cayley/graph/sql"
	"github.com/cayleygraph/cayley/graph/sql/postgres"
	"github.com/lib/pq"
)

const Type = "cockroach"

const driverName = "postgres"

func init() {
	csql.Register(Type, csql.Registration{
		Driver:      driverName,
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
		QueryDialect:  postgres.QueryDialect,
		NoForeignKeys: true,
		Error:         postgres.ConvError,
		//Estimated: func(table string) string{
		//	return "SELECT reltuples::BIGINT AS estimate FROM pg_class WHERE relname='"+table+"';"
		//},
		RunTx:               runTxCockroach,
		TxRetry:             retryTxCockroach,
		NoSchemaChangesInTx: true,
	})
}

func runTxCockroach(tx *sql.Tx, nodes []graphlog.NodeUpdate, quads []graphlog.QuadUpdate, opts graph.IgnoreOpts) error {
	// FIXME: on conflict for SPOL; blocked by CockroachDB not supporting empty ON CONFLICT statements
	return postgres.RunTx(tx, nodes, quads, opts, `(subject_hash, predicate_hash, object_hash)`)
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
		pqErr, ok := err.(*pq.Error)
		if retryable := ok && (pqErr.Code == "CR000" || pqErr.Code == "40001"); !retryable {
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
