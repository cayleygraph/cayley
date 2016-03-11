package sql

import (
	"database/sql"
	"encoding/hex"
	"fmt"

	"github.com/lib/pq"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/graph/proto"
	"github.com/cayleygraph/cayley/quad"
)

const QuadStoreType = "sql"

func init() {
	graph.RegisterQuadStore(QuadStoreType, graph.QuadStoreRegistration{
		NewFunc:           newQuadStore,
		NewForRequestFunc: nil,
		UpgradeFunc:       nil,
		InitFunc:          createSQLTables,
		IsPersistent:      true,
	})
}

type QuadStore struct {
	db           *sql.DB
	sqlFlavor    string
	size         int64
	lru          *cache
	noSizes      bool
	useEstimates bool
}

func connectSQLTables(addr string, _ graph.Options) (*sql.DB, error) {
	// TODO(barakmich): Parse options for more friendly addr, other SQLs.
	conn, err := sql.Open("postgres", addr)
	if err != nil {
		clog.Errorf("Couldn't open database at %s: %#v", addr, err)
		return nil, err
	}
	// "Open may just validate its arguments without creating a connection to the database."
	// "To verify that the data source name is valid, call Ping."
	// Source: http://golang.org/pkg/database/sql/#Open
	if err := conn.Ping(); err != nil {
		clog.Errorf("Couldn't open database at %s: %#v", addr, err)
		return nil, err
	}
	return conn, nil
}

func createSQLTables(addr string, options graph.Options) error {
	conn, err := connectSQLTables(addr, options)
	if err != nil {
		return err
	}
	defer conn.Close()
	tx, err := conn.Begin()
	if err != nil {
		clog.Errorf("Couldn't begin creation transaction: %s", err)
		return err
	}

	quadTable, err := tx.Exec(`
	CREATE TABLE quads (
		subject BYTEA NOT NULL,
		predicate BYTEA NOT NULL,
		object BYTEA NOT NULL,
		label BYTEA,
		horizon BIGSERIAL PRIMARY KEY,
		id BIGINT,
		ts timestamp,
		subject_hash TEXT NOT NULL,
		predicate_hash TEXT NOT NULL,
		object_hash TEXT NOT NULL,
		label_hash TEXT,
		UNIQUE(subject_hash, predicate_hash, object_hash, label_hash)
	);`)
	if err != nil {
		tx.Rollback()
		errd := err.(*pq.Error)
		if errd.Code == "42P07" {
			return graph.ErrDatabaseExists
		}
		clog.Errorf("Cannot create quad table: %v", quadTable)
		return err
	}
	factor, factorOk, err := options.IntKey("db_fill_factor")
	if !factorOk {
		factor = 50
	}
	var index sql.Result

	index, err = tx.Exec(fmt.Sprintf(`
	CREATE INDEX spo_index ON quads (subject_hash) WITH (FILLFACTOR = %d);
	CREATE INDEX pos_index ON quads (predicate_hash) WITH (FILLFACTOR = %d);
	CREATE INDEX osp_index ON quads (object_hash) WITH (FILLFACTOR = %d);
	`, factor, factor, factor))
	if err != nil {
		clog.Errorf("Cannot create indices: %v", index)
		tx.Rollback()
		return err
	}
	tx.Commit()
	return nil
}

func newQuadStore(addr string, options graph.Options) (graph.QuadStore, error) {
	var qs QuadStore
	conn, err := connectSQLTables(addr, options)
	if err != nil {
		return nil, err
	}
	localOpt, localOptOk, err := options.BoolKey("local_optimize")
	if err != nil {
		return nil, err
	}
	qs.db = conn
	qs.sqlFlavor = "postgres"
	qs.size = -1
	qs.lru = newCache(1024)

	// Skip size checking by default.
	qs.noSizes = true
	if localOptOk {
		if localOpt {
			qs.noSizes = false
		}
	}
	qs.useEstimates, _, err = options.BoolKey("use_estimates")
	if err != nil {
		return nil, err
	}

	return &qs, nil
}

func hashOf(s quad.Value) string {
	return hex.EncodeToString(quad.HashOf(s))
}

func convInsertError(err error) error {
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

func marshalQuadDirections(q quad.Quad) (s, p, o, l []byte, err error) {
	s, err = proto.MarshalValue(q.Subject)
	if err != nil {
		return
	}
	p, err = proto.MarshalValue(q.Predicate)
	if err != nil {
		return
	}
	o, err = proto.MarshalValue(q.Object)
	if err != nil {
		return
	}
	l, err = proto.MarshalValue(q.Label)
	if err != nil {
		return
	}
	return
}

func unmarshalQuadDirections(s, p, o, l []byte) (q quad.Quad, err error) {
	q.Subject, err = proto.UnmarshalValue(s)
	if err != nil {
		return
	}
	q.Predicate, err = proto.UnmarshalValue(p)
	if err != nil {
		return
	}
	q.Object, err = proto.UnmarshalValue(o)
	if err != nil {
		return
	}
	q.Label, err = proto.UnmarshalValue(l)
	if err != nil {
		return
	}
	return
}

func unmarshalValue(data []byte) quad.Value {
	v, err := proto.UnmarshalValue(data)
	if err != nil {
		clog.Errorf("couldn't unmarshal value: %v", err)
		return nil
	}
	return v
}

func (qs *QuadStore) copyFrom(tx *sql.Tx, in []graph.Delta, opts graph.IgnoreOpts) error {
	stmt, err := tx.Prepare(pq.CopyIn("quads", "subject", "predicate", "object", "label", "id", "ts", "subject_hash", "predicate_hash", "object_hash", "label_hash"))
	if err != nil {
		clog.Errorf("couldn't prepare COPY statement: %v", err)
		return err
	}
	for _, d := range in {
		s, p, o, l, err := marshalQuadDirections(d.Quad)
		if err != nil {
			clog.Errorf("couldn't marshal quads: %v", err)
			return err
		}
		_, err = stmt.Exec(
			s,
			p,
			o,
			l,
			d.ID.Int(),
			d.Timestamp,
			hashOf(d.Quad.Subject),
			hashOf(d.Quad.Predicate),
			hashOf(d.Quad.Object),
			hashOf(d.Quad.Label),
		)
		if err != nil {
			err = convInsertError(err)
			clog.Errorf("couldn't execute COPY statement: %v", err)
			return err
		}
	}
	_, err = stmt.Exec()
	if err != nil {
		err = convInsertError(err)
		return err
	}
	_ = stmt.Close() // COPY will be closed on last Exec, this will return non-nil error in all cases
	return nil
}

func (qs *QuadStore) runTxPostgres(tx *sql.Tx, in []graph.Delta, opts graph.IgnoreOpts) error {
	allAdds := true
	for _, d := range in {
		if d.Action != graph.Add {
			allAdds = false
		}
	}
	if allAdds && !opts.IgnoreDup {
		return qs.copyFrom(tx, in, opts)
	}

	for _, d := range in {
		switch d.Action {
		case graph.Add:
			end := ";"
			if opts.IgnoreDup {
				end = " ON CONFLICT DO NOTHING;"
			}
			s, p, o, l, err := marshalQuadDirections(d.Quad)
			if err != nil {
				clog.Errorf("couldn't marshal quads: %v", err)
				return err
			}
			_, err = tx.Exec(`INSERT INTO quads(subject, predicate, object, label, id, ts, subject_hash, predicate_hash, object_hash, label_hash) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`+end,
				s,
				p,
				o,
				l,
				d.ID.Int(),
				d.Timestamp,
				hashOf(d.Quad.Subject),
				hashOf(d.Quad.Predicate),
				hashOf(d.Quad.Object),
				hashOf(d.Quad.Label),
			)
			err = convInsertError(err)
			if err != nil {
				clog.Errorf("couldn't exec INSERT statement: %v", err)
				return err
			}
		case graph.Delete:
			result, err := tx.Exec(`DELETE FROM quads WHERE subject_hash=$1 and predicate_hash=$2 and object_hash=$3 and label_hash=$4;`,
				hashOf(d.Quad.Subject), hashOf(d.Quad.Predicate), hashOf(d.Quad.Object), hashOf(d.Quad.Label))
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
	qs.size = -1 // TODO(barakmich): Sync size with writes.
	return nil
}

func (qs *QuadStore) ApplyDeltas(in []graph.Delta, opts graph.IgnoreOpts) error {
	tx, err := qs.db.Begin()
	if err != nil {
		clog.Errorf("couldn't begin write transaction: %v", err)
		return err
	}
	switch qs.sqlFlavor {
	case "postgres":
		err = qs.runTxPostgres(tx, in, opts)
		if err != nil {
			tx.Rollback()
			return err
		}
	default:
		panic("no support for flavor: " + qs.sqlFlavor)
	}
	return tx.Commit()
}

func (qs *QuadStore) Quad(val graph.Value) quad.Quad {
	return val.(quad.Quad)
}

func (qs *QuadStore) QuadIterator(d quad.Direction, val graph.Value) graph.Iterator {
	return NewSQLLinkIterator(qs, d, val.(quad.Value))
}

func (qs *QuadStore) NodesAllIterator() graph.Iterator {
	return NewAllIterator(qs, "nodes")
}

func (qs *QuadStore) QuadsAllIterator() graph.Iterator {
	return NewAllIterator(qs, "quads")
}

func (qs *QuadStore) ValueOf(s quad.Value) graph.Value {
	return s
}

func (qs *QuadStore) NameOf(v graph.Value) quad.Value {
	if v == nil {
		if clog.V(2) {
			clog.Infof("NameOf was nil")
		}
		return nil
	}
	return v.(quad.Value)
}

func (qs *QuadStore) Size() int64 {
	if qs.size != -1 {
		return qs.size
	}

	query := "SELECT COUNT(*) FROM quads;"
	if qs.useEstimates {
		switch qs.sqlFlavor {
		case "postgres":
			query = "SELECT reltuples::BIGINT AS estimate FROM pg_class WHERE relname='quads';"
		default:
			panic("no estimate support for flavor: " + qs.sqlFlavor)
		}
	}

	c := qs.db.QueryRow(query)
	err := c.Scan(&qs.size)
	if err != nil {
		clog.Errorf("Couldn't execute COUNT: %v", err)
		return 0
	}
	return qs.size
}

func (qs *QuadStore) Horizon() graph.PrimaryKey {
	var horizon int64
	err := qs.db.QueryRow("SELECT horizon FROM quads ORDER BY horizon DESC LIMIT 1;").Scan(&horizon)
	if err != nil {
		if err != sql.ErrNoRows {
			clog.Errorf("Couldn't execute horizon: %v", err)
		}
		return graph.NewSequentialKey(0)
	}
	return graph.NewSequentialKey(horizon)
}

func (qs *QuadStore) FixedIterator() graph.FixedIterator {
	return iterator.NewFixed(iterator.Identity)
}

func (qs *QuadStore) Close() {
	qs.db.Close()
}

func (qs *QuadStore) QuadDirection(in graph.Value, d quad.Direction) graph.Value {
	q := in.(quad.Quad)
	return q.Get(d)
}

func (qs *QuadStore) Type() string {
	return QuadStoreType
}

func (qs *QuadStore) sizeForIterator(isAll bool, dir quad.Direction, val quad.Value) int64 {
	var err error
	if isAll {
		return qs.Size()
	}
	if qs.noSizes {
		if dir == quad.Predicate {
			return (qs.Size() / 100) + 1
		}
		return (qs.Size() / 1000) + 1
	}
	if val, ok := qs.lru.Get(val.String() + string(dir.Prefix())); ok {
		return val
	}
	var size int64
	if clog.V(4) {
		clog.Infof("sql: getting size for select %s, %s", dir.String(), val)
	}
	err = qs.db.QueryRow(
		fmt.Sprintf("SELECT count(*) FROM quads WHERE %s_hash = $1;", dir.String()), hashOf(val)).Scan(&size)
	if err != nil {
		clog.Errorf("Error getting size from SQL database: %v", err)
		return 0
	}
	qs.lru.Put(val.String()+string(dir.Prefix()), size)
	return size
}
