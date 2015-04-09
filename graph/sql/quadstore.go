package sql

import (
	"database/sql"
	"fmt"

	"github.com/lib/pq"

	"github.com/barakmich/glog"
	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/quad"
)

const QuadStoreType = "sql"

func init() {
	graph.RegisterQuadStore(QuadStoreType, true, newQuadStore, createSQLTables, nil)
}

type QuadStore struct {
	db        *sql.DB
	sqlFlavor string
	size      int64
	lru       *cache
}

func connectSQLTables(addr string, _ graph.Options) (*sql.DB, error) {
	// TODO(barakmich): Parse options for more friendly addr, other SQLs.
	conn, err := sql.Open("postgres", addr)
	if err != nil {
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
	tx, err := conn.Begin()
	if err != nil {
		glog.Errorf("Couldn't begin creation transaction: %s", err)
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
		UNIQUE(subject, predicate, object, label)
	);`)
	if err != nil {
		glog.Errorf("Cannot create quad table: %v", quadTable)
		return err
	}
	index, err := tx.Exec(`
	CREATE INDEX pos_index ON quads (predicate, object, subject) WITH (FILLFACTOR = 50);
	CREATE INDEX osp_index ON quads (object, subject, predicate) WITH (FILLFACTOR = 50);
	`)
	if err != nil {
		glog.Errorf("Cannot create indices: %v", index)
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
	qs.db = conn
	qs.sqlFlavor = "postgres"
	qs.size = -1
	qs.lru = newCache(1024)
	return &qs, nil
}

func (qs *QuadStore) copyFrom(tx *sql.Tx, in []graph.Delta) error {
	stmt, err := tx.Prepare(pq.CopyIn("quads", "subject", "predicate", "object", "label", "id", "ts"))
	if err != nil {
		return err
	}
	for _, d := range in {
		_, err := stmt.Exec(d.Quad.Subject, d.Quad.Predicate, d.Quad.Object, d.Quad.Label, d.ID.Int(), d.Timestamp)
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

func (qs *QuadStore) buildTxPostgres(tx *sql.Tx, in []graph.Delta) error {
	allAdds := true
	for _, d := range in {
		if d.Action != graph.Add {
			allAdds = false
		}
	}
	if allAdds {
		return qs.copyFrom(tx, in)
	}

	insert, err := tx.Prepare(`INSERT INTO quads(subject, predicate, object, label, id, ts) VALUES ($1, $2, $3, $4, $5, $6)`)
	if err != nil {
		glog.Errorf("Cannot prepare insert statement: %v", err)
		return err
	}
	for _, d := range in {
		switch d.Action {
		case graph.Add:
			_, err := insert.Exec(d.Quad.Subject, d.Quad.Predicate, d.Quad.Object, d.Quad.Label, d.ID.Int(), d.Timestamp)
			if err != nil {
				glog.Errorf("couldn't prepare INSERT statement: %v", err)
				return err
			}
			//for _, dir := range quad.Directions {
			//_, err := tx.Exec(`
			//WITH upsert AS (UPDATE nodes SET size=size+1 WHERE node=$1 RETURNING *)
			//INSERT INTO nodes (node, size) SELECT $1, 1 WHERE NOT EXISTS (SELECT * FROM UPSERT);
			//`, d.Quad.Get(dir))
			//if err != nil {
			//glog.Errorf("couldn't prepare upsert statement in direction %s: %v", dir, err)
			//return err
			//}
			//}
		case graph.Delete:
			_, err := tx.Exec(`DELETE FROM quads WHERE subject=$1 and predicate=$2 and object=$3 and label=$4;`,
				d.Quad.Subject, d.Quad.Predicate, d.Quad.Object, d.Quad.Label)
			if err != nil {
				glog.Errorf("couldn't prepare DELETE statement: %v", err)
			}
			//for _, dir := range quad.Directions {
			//tx.Exec(`UPDATE nodes SET size=size-1 WHERE node=$1;`, d.Quad.Get(dir))
			//}
		default:
			panic("unknown action")
		}
	}
	return nil
}

func (qs *QuadStore) ApplyDeltas(in []graph.Delta, _ graph.IgnoreOpts) error {
	// TODO(barakmich): Support ignoreOpts? "ON CONFLICT IGNORE"
	tx, err := qs.db.Begin()
	if err != nil {
		glog.Errorf("couldn't begin write transaction: %v", err)
		return err
	}
	switch qs.sqlFlavor {
	case "postgres":
		err = qs.buildTxPostgres(tx, in)
		if err != nil {
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
	return NewIterator(qs, d, val)
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
	return v.(string)
}

func (qs *QuadStore) Size() int64 {
	// TODO(barakmich): Sync size with writes.
	if qs.size != -1 {
		return qs.size
	}
	c := qs.db.QueryRow("SELECT COUNT(*) FROM quads;")
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
		glog.Errorf("Couldn't execute horizon: %v", err)
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

func (qs *QuadStore) OptimizeIterator(it graph.Iterator) (graph.Iterator, bool) {
	switch it.Type() {
	case graph.LinksTo:
		return qs.optimizeLinksTo(it.(*iterator.LinksTo))

	}
	return it, false
}

func (qs *QuadStore) optimizeLinksTo(it *iterator.LinksTo) (graph.Iterator, bool) {
	subs := it.SubIterators()
	if len(subs) != 1 {
		return it, false
	}
	primary := subs[0]
	if primary.Type() == graph.Fixed {
		size, _ := primary.Size()
		if size == 1 {
			if !graph.Next(primary) {
				panic("unexpected size during optimize")
			}
			val := primary.Result()
			newIt := qs.QuadIterator(it.Direction(), val)
			nt := newIt.Tagger()
			nt.CopyFrom(it)
			for _, tag := range primary.Tagger().Tags() {
				nt.AddFixed(tag, val)
			}
			it.Close()
			return newIt, true
		}
	}
	return it, false
}

func (qs *QuadStore) sizeForIterator(isAll bool, dir quad.Direction, val string) int64 {
	var err error
	if isAll {
		return qs.Size()
	}
	if val, ok := qs.lru.Get(val + string(dir.Prefix())); ok {
		return val
	}
	var size int64
	glog.V(4).Infoln("sql: getting size for select %s, %s", dir.String(), val)
	err = qs.db.QueryRow(
		fmt.Sprintf("SELECT count(*) FROM quads WHERE %s = $1;", dir.String()), val).Scan(&size)
	if err != nil {
		glog.Errorln("Error getting size from SQL database: %v", err)
		return 0
	}
	qs.lru.Put(val+string(dir.Prefix()), size)
	return size
}
