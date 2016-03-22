package sql

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"strconv"
	"time"

	"github.com/lib/pq"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/graph/proto"
	"github.com/cayleygraph/cayley/internal/lru"
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

type NodeHash sql.NullString

func (NodeHash) IsNode() bool { return true }

type QuadHashes [4]sql.NullString

func (QuadHashes) IsNode() bool { return false }
func (q QuadHashes) Get(d quad.Direction) sql.NullString {
	switch d {
	case quad.Subject:
		return q[0]
	case quad.Predicate:
		return q[1]
	case quad.Object:
		return q[2]
	case quad.Label:
		return q[3]
	}
	panic(fmt.Errorf("unknown direction: %v", d))
}

type QuadStore struct {
	db           *sql.DB
	sqlFlavor    string
	size         int64
	ids          *lru.Cache
	sizes        *lru.Cache
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

	table, err := tx.Exec(`
	CREATE TABLE nodes (
		hash TEXT PRIMARY KEY,
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
	);`)
	if err != nil {
		tx.Rollback()
		errd := err.(*pq.Error)
		if errd.Code == "42P07" {
			return graph.ErrDatabaseExists
		}
		clog.Errorf("Cannot create nodes table: %v", table)
		return err
	}
	table, err = tx.Exec(`
	CREATE TABLE quads (
		horizon BIGSERIAL PRIMARY KEY,
		subject_hash TEXT NOT NULL REFERENCES nodes (hash),
		predicate_hash TEXT NOT NULL REFERENCES nodes (hash),
		object_hash TEXT NOT NULL REFERENCES nodes (hash),
		label_hash TEXT REFERENCES nodes (hash),
		id BIGINT,
		ts timestamp
	);`)
	if err != nil {
		tx.Rollback()
		errd := err.(*pq.Error)
		if errd.Code == "42P07" {
			return graph.ErrDatabaseExists
		}
		clog.Errorf("Cannot create quad table: %v", table)
		return err
	}
	factor, factorOk, err := options.IntKey("db_fill_factor")
	if !factorOk {
		factor = 50
	}
	var index sql.Result

	index, err = tx.Exec(fmt.Sprintf(`
	CREATE UNIQUE INDEX spol_unique ON quads (subject_hash, predicate_hash, object_hash, label_hash) WHERE label_hash IS NOT NULL;
	CREATE UNIQUE INDEX spo_unique ON quads (subject_hash, predicate_hash, object_hash) WHERE label_hash IS NULL;
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
	qs.sizes = lru.New(1024)
	qs.ids = lru.New(1024)

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

func hashOf(s quad.Value) sql.NullString {
	if s == nil {
		return sql.NullString{Valid: false}
	}
	return sql.NullString{Valid: true, String: hex.EncodeToString(quad.HashOf(s))}
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

func (qs *QuadStore) copyFrom(tx *sql.Tx, in []graph.Delta, opts graph.IgnoreOpts) error {
	panic("broken")
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
	//allAdds := true
	//for _, d := range in {
	//	if d.Action != graph.Add {
	//		allAdds = false
	//	}
	//}
	//if allAdds && !opts.IgnoreDup {
	//	return qs.copyFrom(tx, in, opts)
	//}
	inserted := make(map[string]struct{})

	for _, d := range in {
		switch d.Action {
		case graph.Add:
			end := ";"
			if opts.IgnoreDup {
				end = " ON CONFLICT DO NOTHING;"
			}
			var hs, hp, ho, hl sql.NullString
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
				if _, ok := inserted[h.String]; ok {
					continue
				}
				var (
					names  = []string{"hash", ""}[:1]
					values = []interface{}{h, nil}[:1]
				)
				switch v := v.(type) {
				case quad.IRI:
					names = append(names, "value_string", "iri")
					values = append(values, string(v), true)
				case quad.BNode:
					names = append(names, "value_string", "bnode")
					values = append(values, string(v), true)
				case quad.String:
					names = append(names, "value_string")
					values = append(values, string(v))
				case quad.TypedString:
					names = append(names, "value_string", "datatype")
					values = append(values, string(v.Value), string(v.Type))
				case quad.LangString:
					names = append(names, "value_string", "language")
					values = append(values, string(v.Value), v.Lang)
				case quad.Int:
					names = append(names, "value_int")
					values = append(values, int64(v))
				case quad.Bool:
					names = append(names, "value_bool")
					values = append(values, bool(v))
				case quad.Float:
					names = append(names, "value_float")
					values = append(values, float64(v))
				case quad.Time:
					names = append(names, "value_time")
					values = append(values, time.Time(v))
				default:
					p, err := proto.MarshalValue(v)
					if err != nil {
						clog.Errorf("couldn't marshal value: %v", err)
						return err
					}
					names = append(names, "value")
					values = append(values, p)
				}
				var ph = make([]string, len(values))
				for i := range ph {
					ph[i] = "$" + strconv.FormatInt(int64(i)+1, 10)
				}
				_, err := tx.Exec(`INSERT INTO nodes(`+
					strings.Join(names, ", ")+
					`) VALUES (`+
					strings.Join(ph, ", ")+
					`) ON CONFLICT DO NOTHING;`,
					values...,
				)
				err = convInsertError(err)
				if err != nil {
					clog.Errorf("couldn't exec INSERT statement: %v", err)
					return err
				}
				inserted[h.String] = struct{}{}
			}
			_, err := tx.Exec(`INSERT INTO quads(subject_hash, predicate_hash, object_hash, label_hash, id, ts) VALUES ($1, $2, $3, $4, $5, $6)`+end,
				hs, hp, ho, hl,
				d.ID.Int(),
				d.Timestamp,
			)
			err = convInsertError(err)
			if err != nil {
				clog.Errorf("couldn't exec INSERT statement: %v", err)
				return err
			}
		case graph.Delete:
			var (
				result sql.Result
				err    error
			)
			if d.Quad.Label == nil {
				result, err = tx.Exec(`DELETE FROM quads WHERE subject_hash=$1 and predicate_hash=$2 and object_hash=$3 and label_hash is null;`,
					hashOf(d.Quad.Subject), hashOf(d.Quad.Predicate), hashOf(d.Quad.Object))
			} else {
				result, err = tx.Exec(`DELETE FROM quads WHERE subject_hash=$1 and predicate_hash=$2 and object_hash=$3 and label_hash=$4;`,
					hashOf(d.Quad.Subject), hashOf(d.Quad.Predicate), hashOf(d.Quad.Object), hashOf(d.Quad.Label))
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
	h := val.(QuadHashes)
	return quad.Quad{
		Subject:   qs.NameOf(NodeHash(h.Get(quad.Subject))),
		Predicate: qs.NameOf(NodeHash(h.Get(quad.Predicate))),
		Object:    qs.NameOf(NodeHash(h.Get(quad.Object))),
		Label:     qs.NameOf(NodeHash(h.Get(quad.Label))),
	}
}

func (qs *QuadStore) QuadIterator(d quad.Direction, val graph.Value) graph.Iterator {
	return newSQLLinkIterator(qs, d, val.(NodeHash))
}

func (qs *QuadStore) NodesAllIterator() graph.Iterator {
	return NewAllIterator(qs, "nodes")
}

func (qs *QuadStore) QuadsAllIterator() graph.Iterator {
	return NewAllIterator(qs, "quads")
}

func (qs *QuadStore) ValueOf(s quad.Value) graph.Value {
	return NodeHash(hashOf(s))
}

func (qs *QuadStore) NameOf(v graph.Value) quad.Value {
	if v == nil {
		if clog.V(2) {
			clog.Infof("NameOf was nil")
		}
		return nil
	}
	hash := v.(NodeHash)
	if !hash.Valid || hash.String == "" {
		if clog.V(2){
			clog.Infof("NameOf was nil")
		}
		return nil
	}
	if val, ok := qs.ids.Get(hash.String); ok {
		return val.(quad.Value)
	}
	query := `SELECT
		value,
		value_string,
		datatype,
		language,
		iri,
		bnode,
		value_int,
		value_bool,
		value_float,
		value_time
	FROM nodes WHERE hash = $1 LIMIT 1;`
	c := qs.db.QueryRow(query, sql.NullString(hash))
	var (
		data   []byte
		str    sql.NullString
		typ    sql.NullString
		lang   sql.NullString
		iri    sql.NullBool
		bnode  sql.NullBool
		vint   sql.NullInt64
		vbool  sql.NullBool
		vfloat sql.NullFloat64
		vtime  pq.NullTime
	)
	if err := c.Scan(
		&data,
		&str,
		&typ,
		&lang,
		&iri,
		&bnode,
		&vint,
		&vbool,
		&vfloat,
		&vtime,
	); err != nil {
		clog.Errorf("Couldn't execute value lookup: %v", err)
		return nil
	}
	var val quad.Value
	if str.Valid {
		if iri.Bool {
			val = quad.IRI(str.String)
		} else if bnode.Bool {
			val = quad.BNode(str.String)
		} else if lang.Valid {
			val = quad.LangString{
				Value: quad.String(str.String),
				Lang:  lang.String,
			}
		} else if typ.Valid {
			val = quad.TypedString{
				Value: quad.String(str.String),
				Type:  quad.IRI(typ.String),
			}
		} else {
			val = quad.String(str.String)
		}
	} else if vint.Valid {
		val = quad.Int(vint.Int64)
	} else if vbool.Valid {
		val = quad.Bool(vbool.Bool)
	} else if vfloat.Valid {
		val = quad.Float(vfloat.Float64)
	} else if vtime.Valid {
		val = quad.Time(vtime.Time)
	} else {
		qv, err := proto.UnmarshalValue(data)
		if err != nil {
			clog.Errorf("Couldn't unmarshal value: %v", err)
			return nil
		}
		val = qv
	}
	if val != nil {
		qs.ids.Put(hash.String, val)
	}
	return val
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
	return NodeHash(in.(QuadHashes).Get(d))
}

func (qs *QuadStore) Type() string {
	return QuadStoreType
}

func (qs *QuadStore) sizeForIterator(isAll bool, dir quad.Direction, hash sql.NullString) int64 {
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
	if val, ok := qs.sizes.Get(hash.String + string(dir.Prefix())); ok {
		return val.(int64)
	}
	var size int64
	if clog.V(4) {
		clog.Infof("sql: getting size for select %s, %v", dir.String(), val)
	}
	err = qs.db.QueryRow(
		fmt.Sprintf("SELECT count(*) FROM quads WHERE %s_hash = $1;", dir.String()), hash).Scan(&size)
	if err != nil {
		clog.Errorf("Error getting size from SQL database: %v", err)
		return 0
	}
	qs.sizes.Put(hash.String+string(dir.Prefix()), size)
	return size
}
