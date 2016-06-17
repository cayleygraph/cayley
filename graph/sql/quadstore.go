package sql

import (
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"sync"

	"github.com/lib/pq"

	"github.com/barakmich/glog"
	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/quad"
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

var (
	hashPool = sync.Pool{
		New: func() interface{} { return sha1.New() },
	}
	hashSize = sha1.Size
)

type sqlFlavor string

const (
	postgres  sqlFlavor = "postgres"
	cockroach sqlFlavor = "cockroach"
)

var sqlFlavors = []sqlFlavor{postgres, cockroach}

type QuadStore struct {
	db           *sql.DB
	sqlFlavor    sqlFlavor
	size         int64
	lru          *cache
	noSizes      bool
	useEstimates bool
}

func connectSQLTables(addr string, _ graph.Options) (*sql.DB, error) {
	// TODO(barakmich): Parse options for more friendly addr, other SQLs.
	conn, err := sql.Open("postgres", addr)
	if err != nil {
		glog.Errorf("Couldn't open database at %s: %#v", addr, err)
		return nil, err
	}
	// "Open may just validate its arguments without creating a connection to the database."
	// "To verify that the data source name is valid, call Ping."
	// Source: http://golang.org/pkg/database/sql/#Open
	if err := conn.Ping(); err != nil {
		glog.Errorf("Couldn't open database at %s: %#v", addr, err)
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
		glog.Errorf("Couldn't begin creation transaction: %s", err)
		return err
	}

	sqlFlavor, err := getSQLFlavor(options)
	if err != nil {
		return err
	}

	quadTable, err := tx.Exec(`
	CREATE TABLE quads (
		subject TEXT NOT NULL,
		predicate TEXT NOT NULL,
		object TEXT NOT NULL,
		label TEXT,
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
		glog.Errorf("Cannot create quad table: %v", quadTable)
		return err
	}
	factor, factorOk, err := options.IntKey("db_fill_factor")
	if !factorOk {
		factor = 50
	}
	var index sql.Result

	var indexOptions string
	if sqlFlavor != cockroach {
		indexOptions = fmt.Sprintf("WITH (FILLFACTOR = %d)", factor)
	} else {
		glog.Infof("Index FILLFACTOR disabled.")
	}

	index, err = tx.Exec(fmt.Sprintf(`
	CREATE INDEX spo_index ON quads (subject_hash) %s;
	CREATE INDEX pos_index ON quads (predicate_hash) %s;
	CREATE INDEX osp_index ON quads (object_hash) %s;
	`, indexOptions, indexOptions, indexOptions))
	if err != nil {
		glog.Errorf("Cannot create indices: %v", index)
		tx.Rollback()
		return err
	}
	tx.Commit()
	return nil
}

func getSQLFlavor(options graph.Options) (sqlFlavor, error) {
	flavor, found, err := options.StringKey("db_sql_flavor")
	if err != nil {
		return "", err
	}
	if !found {
		return postgres, nil
	}
	for _, valid := range sqlFlavors {
		if flavor == string(valid) {
			return valid, nil
		}
	}
	return "", fmt.Errorf("invalid sql flavor: %q", flavor)
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
	qs.size = -1
	qs.lru = newCache(1024)

	qs.sqlFlavor, err = getSQLFlavor(options)
	if err != nil {
		return nil, err
	}
	glog.Infof("sql flavor: %s", qs.sqlFlavor)

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

func hashOf(s string) string {
	h := hashPool.Get().(hash.Hash)
	h.Reset()
	defer hashPool.Put(h)
	key := make([]byte, 0, hashSize)
	h.Write([]byte(s))
	key = h.Sum(key)
	return hex.EncodeToString(key)
}

func (qs *QuadStore) copyFrom(tx *sql.Tx, in []graph.Delta) error {
	stmt, err := tx.Prepare(pq.CopyIn("quads", "subject", "predicate", "object", "label", "id", "ts", "subject_hash", "predicate_hash", "object_hash", "label_hash"))
	if err != nil {
		return err
	}
	for _, d := range in {
		_, err := stmt.Exec(
			d.Quad.Subject,
			d.Quad.Predicate,
			d.Quad.Object,
			d.Quad.Label,
			d.ID.Int(),
			d.Timestamp,
			hashOf(d.Quad.Subject),
			hashOf(d.Quad.Predicate),
			hashOf(d.Quad.Object),
			hashOf(d.Quad.Label),
		)
		if err != nil {
			glog.Errorf("couldn't prepare COPY statement: %v", err)
			return err
		}
	}
	_, err = stmt.Exec()
	if err != nil {
		return err
	}
	return stmt.Close()
}

func (qs *QuadStore) runTxPostgres(tx *sql.Tx, in []graph.Delta, opts graph.IgnoreOpts) error {
	allAdds := true
	for _, d := range in {
		if d.Action != graph.Add {
			allAdds = false
		}
	}
	if allAdds && qs.sqlFlavor != cockroach {
		return qs.copyFrom(tx, in)
	}

	var insertQueryOptions string
	if opts.IgnoreDup {
		insertQueryOptions = `ON CONFLICT (subject_hash, predicate_hash, object_hash, label_hash) DO NOTHING`
	}
	insertQuery := fmt.Sprintf(`INSERT INTO quads(subject, predicate, object, label, id, ts, subject_hash, predicate_hash, object_hash, label_hash) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) %s;`, insertQueryOptions)

	for _, d := range in {
		switch d.Action {
		case graph.Add:
			_, err := tx.Exec(insertQuery,
				d.Quad.Subject,
				d.Quad.Predicate,
				d.Quad.Object,
				d.Quad.Label,
				d.ID.Int(),
				d.Timestamp,
				hashOf(d.Quad.Subject),
				hashOf(d.Quad.Predicate),
				hashOf(d.Quad.Object),
				hashOf(d.Quad.Label),
			)
			if err != nil {
				glog.Errorf("couldn't exec INSERT statement: %v", err)
				return err
			}
		case graph.Delete:
			result, err := tx.Exec(`DELETE FROM quads WHERE subject_hash=$1 and predicate_hash=$2 and object_hash=$3 and label_hash=$4;`,
				hashOf(d.Quad.Subject), hashOf(d.Quad.Predicate), hashOf(d.Quad.Object), hashOf(d.Quad.Label))
			if err != nil {
				glog.Errorf("couldn't exec DELETE statement: %v", err)
				return err
			}
			affected, err := result.RowsAffected()
			if err != nil {
				glog.Errorf("couldn't get DELETE RowsAffected: %v", err)
				return err
			}
			if affected != 1 && !opts.IgnoreMissing {
				return errors.New("deleting non-existent triple; rolling back")
			}
		default:
			panic("unknown action")
		}
	}
	return nil
}

func (qs *QuadStore) ApplyDeltas(in []graph.Delta, opts graph.IgnoreOpts) error {
	// TODO(barakmich): Support more ignoreOpts? "ON CONFLICT IGNORE"
	tx, err := qs.db.Begin()
	if err != nil {
		glog.Errorf("couldn't begin write transaction: %v", err)
		return err
	}
	switch qs.sqlFlavor {
	case postgres:
		err = qs.runTxPostgres(tx, in, opts)
		if err != nil {
			tx.Rollback()
			return err
		}
	case cockroach:
		for try := 0; try < 10; try++ {
			err = qs.runTxPostgres(tx, in, opts)
			if err != nil {
				tx.Rollback()
				// Retry on `40001 - restart transaction` errors from cockroach.
				if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == pq.ErrorCode("40001") {
					continue
				}
				return err
			}
			break
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
	return NewSQLLinkIterator(qs, d, val.(string))
}

func (qs *QuadStore) NodesAllIterator() graph.Iterator {
	return NewAllIterator(qs, "nodes")
}

func (qs *QuadStore) QuadsAllIterator() graph.Iterator {
	return NewAllIterator(qs, "quads")
}

func (qs *QuadStore) ValueOf(s string) graph.Value {
	return s
}

func (qs *QuadStore) NameOf(v graph.Value) string {
	if v == nil {
		glog.V(2).Info("NameOf was nil")
		return ""
	}
	return v.(string)
}

func (qs *QuadStore) Size() int64 {
	// TODO(barakmich): Sync size with writes.
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
		glog.Errorf("Couldn't execute COUNT: %v", err)
		return 0
	}
	return qs.size
}

func (qs *QuadStore) Horizon() graph.PrimaryKey {
	var horizon int64
	err := qs.db.QueryRow("SELECT horizon FROM quads ORDER BY horizon DESC LIMIT 1;").Scan(&horizon)
	if err != nil {
		if err != sql.ErrNoRows {
			glog.Errorf("Couldn't execute horizon: %v", err)
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

func (qs *QuadStore) sizeForIterator(isAll bool, dir quad.Direction, val string) int64 {
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
	if val, ok := qs.lru.Get(val + string(dir.Prefix())); ok {
		return val
	}
	var size int64
	glog.V(4).Infoln("sql: getting size for select %s, %s", dir.String(), val)
	err = qs.db.QueryRow(
		fmt.Sprintf("SELECT count(*) FROM quads WHERE %s_hash = $1;", dir.String()), hashOf(val)).Scan(&size)
	if err != nil {
		glog.Errorln("Error getting size from SQL database: %v", err)
		return 0
	}
	qs.lru.Put(val+string(dir.Prefix()), size)
	return size
}
