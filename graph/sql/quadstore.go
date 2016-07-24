package sql

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/internal/lru"
	"github.com/cayleygraph/cayley/quad"
	"github.com/cayleygraph/cayley/quad/pquads"
)

const QuadStoreType = "sql"

const defaultFillFactor = 50

func init() {
	graph.RegisterQuadStore(QuadStoreType, graph.QuadStoreRegistration{
		NewFunc:           newQuadStore,
		NewForRequestFunc: nil,
		UpgradeFunc:       nil,
		InitFunc:          createSQLTables,
		IsPersistent:      true,
	})
}

type NodeHash [quad.HashSize]byte

func (NodeHash) IsNode() bool { return true }
func (h NodeHash) Valid() bool {
	return h != NodeHash{}
}
func (h NodeHash) toSQL() interface{} {
	if !h.Valid() {
		return nil
	}
	return []byte(h[:])
}
func (h NodeHash) String() string {
	if !h.Valid() {
		return ""
	}
	return hex.EncodeToString(h[:])
}
func (h *NodeHash) Scan(src interface{}) error {
	if src == nil {
		*h = NodeHash{}
		return nil
	}
	b, ok := src.([]byte)
	if !ok {
		return fmt.Errorf("cannot scan %T to NodeHash", src)
	}
	if len(b) == 0 {
		*h = NodeHash{}
		return nil
	} else if len(b) != quad.HashSize {
		return fmt.Errorf("unexpected hash length: %d", len(b))
	}
	copy((*h)[:], b)
	return nil
}

func hashOf(s quad.Value) (out NodeHash) {
	if s == nil {
		return
	}
	quad.HashTo(s, out[:])
	return
}

type QuadHashes [4]NodeHash

func (QuadHashes) IsNode() bool { return false }
func (q QuadHashes) Get(d quad.Direction) NodeHash {
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

var nodesColumns = []string{
	"hash",
	"value",
	"value_string",
	"datatype",
	"language",
	"iri",
	"bnode",
	"value_int",
	"value_bool",
	"value_float",
	"value_time",
}

var nodeInsertColumns = [][]string{
	{"value"},
	{"value_string", "iri"},
	{"value_string", "bnode"},
	{"value_string"},
	{"value_string", "datatype"},
	{"value_string", "language"},
	{"value_int"},
	{"value_bool"},
	{"value_float"},
	{"value_time"},
}

const nodesTableStatement = `CREATE TABLE nodes (
	hash BYTEA PRIMARY KEY,
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
);`

const quadsUniqueIndex = `
	CREATE UNIQUE INDEX spol_unique ON quads (subject_hash, predicate_hash, object_hash, label_hash) WHERE label_hash IS NOT NULL;
	CREATE UNIQUE INDEX spo_unique ON quads (subject_hash, predicate_hash, object_hash) WHERE label_hash IS NULL;
	`

const quadsForeignIndex = `
	ALTER TABLE quads ADD CONSTRAINT subject_hash_fk FOREIGN KEY (subject_hash) REFERENCES nodes (hash);
	ALTER TABLE quads ADD CONSTRAINT predicate_hash_fk FOREIGN KEY (predicate_hash) REFERENCES nodes (hash);
	ALTER TABLE quads ADD CONSTRAINT object_hash_fk FOREIGN KEY (object_hash) REFERENCES nodes (hash);
	ALTER TABLE quads ADD CONSTRAINT label_hash_fk FOREIGN KEY (label_hash) REFERENCES nodes (hash);
	`

func quadsSecondaryIndexes(factor int) string {
	return fmt.Sprintf(`
	CREATE INDEX spo_index ON quads (subject_hash) WITH (FILLFACTOR = %d);
	CREATE INDEX pos_index ON quads (predicate_hash) WITH (FILLFACTOR = %d);
	CREATE INDEX osp_index ON quads (object_hash) WITH (FILLFACTOR = %d);
	`, factor, factor, factor)
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

	table, err := tx.Exec(nodesTableStatement)
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
		subject_hash BYTEA NOT NULL,
		predicate_hash BYTEA NOT NULL,
		object_hash BYTEA NOT NULL,
		label_hash BYTEA,
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
		factor = defaultFillFactor
	}
	spoIndexes := quadsSecondaryIndexes(factor)

	var index sql.Result
	index, err = tx.Exec(quadsUniqueIndex + quadsForeignIndex + spoIndexes)
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
	s, err = pquads.MarshalValue(q.Subject)
	if err != nil {
		return
	}
	p, err = pquads.MarshalValue(q.Predicate)
	if err != nil {
		return
	}
	o, err = pquads.MarshalValue(q.Object)
	if err != nil {
		return
	}
	l, err = pquads.MarshalValue(q.Label)
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

func escapeNullByte(s string) string {
	return strings.Replace(s, "\u0000", `\x00`, -1)
}
func unescapeNullByte(s string) string {
	return strings.Replace(s, `\x00`, "\u0000", -1)
}

func nodeValues(h NodeHash, v quad.Value) (int, []interface{}, error) {
	var (
		nodeKey int
		values  = []interface{}{h.toSQL(), nil, nil}[:1]
	)
	switch v := v.(type) {
	case quad.IRI:
		nodeKey = 1
		values = append(values, string(v), true)
	case quad.BNode:
		nodeKey = 2
		values = append(values, string(v), true)
	case quad.String:
		nodeKey = 3
		values = append(values, escapeNullByte(string(v)))
	case quad.TypedString:
		nodeKey = 4
		values = append(values, escapeNullByte(string(v.Value)), string(v.Type))
	case quad.LangString:
		nodeKey = 5
		values = append(values, escapeNullByte(string(v.Value)), v.Lang)
	case quad.Int:
		nodeKey = 6
		values = append(values, int64(v))
	case quad.Bool:
		nodeKey = 7
		values = append(values, bool(v))
	case quad.Float:
		nodeKey = 8
		values = append(values, float64(v))
	case quad.Time:
		nodeKey = 9
		values = append(values, time.Time(v))
	default:
		nodeKey = 0
		p, err := pquads.MarshalValue(v)
		if err != nil {
			clog.Errorf("couldn't marshal value: %v", err)
			return 0, nil, err
		}
		values = append(values, p)
	}
	return nodeKey, values, nil
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

	end := ";"
	if opts.IgnoreDup {
		end = " ON CONFLICT DO NOTHING;"
	}

	var (
		insertQuad  *sql.Stmt
		insertValue map[int]*sql.Stmt     // prepared statements for each value type
		inserted    map[NodeHash]struct{} // tracks already inserted values

		deleteQuad   *sql.Stmt
		deleteTriple *sql.Stmt
	)

	var err error
	for _, d := range in {
		switch d.Action {
		case graph.Add:
			if insertQuad == nil {
				insertQuad, err = tx.Prepare(`INSERT INTO quads(subject_hash, predicate_hash, object_hash, label_hash, id, ts) VALUES ($1, $2, $3, $4, $5, $6)` + end)
				if err != nil {
					return err
				}
				insertValue = make(map[int]*sql.Stmt)
				inserted = make(map[NodeHash]struct{}, len(in))
			}
			var hs, hp, ho, hl NodeHash
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
				if !h.Valid() {
					continue
				} else if _, ok := inserted[h]; ok {
					continue
				}
				nodeKey, values, err := nodeValues(h, v)
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
						strings.Join(nodeInsertColumns[nodeKey], ", ") +
						`) VALUES ($1, ` +
						strings.Join(ph, ", ") +
						`) ON CONFLICT DO NOTHING;`)
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
				inserted[h] = struct{}{}
			}
			_, err := insertQuad.Exec(
				hs.toSQL(), hp.toSQL(), ho.toSQL(), hl.toSQL(),
				d.ID.Int(),
				d.Timestamp,
			)
			err = convInsertError(err)
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
				result, err = deleteTriple.Exec(hashOf(d.Quad.Subject).toSQL(), hashOf(d.Quad.Predicate).toSQL(), hashOf(d.Quad.Object).toSQL())
			} else {
				result, err = deleteQuad.Exec(hashOf(d.Quad.Subject).toSQL(), hashOf(d.Quad.Predicate).toSQL(), hashOf(d.Quad.Object).toSQL(), hashOf(d.Quad.Label).toSQL())
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
		Subject:   qs.NameOf(h.Get(quad.Subject)),
		Predicate: qs.NameOf(h.Get(quad.Predicate)),
		Object:    qs.NameOf(h.Get(quad.Object)),
		Label:     qs.NameOf(h.Get(quad.Label)),
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
	} else if v, ok := v.(graph.PreFetchedValue); ok {
		return v.NameOf()
	}
	hash := v.(NodeHash)
	if !hash.Valid() {
		if clog.V(2) {
			clog.Infof("NameOf was nil")
		}
		return nil
	}
	if val, ok := qs.ids.Get(hash.String()); ok {
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
	c := qs.db.QueryRow(query, hash.toSQL())
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
				Value: quad.String(unescapeNullByte(str.String)),
				Lang:  lang.String,
			}
		} else if typ.Valid {
			val = quad.TypedString{
				Value: quad.String(unescapeNullByte(str.String)),
				Type:  quad.IRI(typ.String),
			}
		} else {
			val = quad.String(unescapeNullByte(str.String))
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
		qv, err := pquads.UnmarshalValue(data)
		if err != nil {
			clog.Errorf("Couldn't unmarshal value: %v", err)
			return nil
		}
		val = qv
	}
	if val != nil {
		qs.ids.Put(hash.String(), val)
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

func (qs *QuadStore) sizeForIterator(isAll bool, dir quad.Direction, hash NodeHash) int64 {
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
	if val, ok := qs.sizes.Get(hash.String() + string(dir.Prefix())); ok {
		return val.(int64)
	}
	var size int64
	if clog.V(4) {
		clog.Infof("sql: getting size for select %s, %v", dir.String(), hash)
	}
	err = qs.db.QueryRow(
		fmt.Sprintf("SELECT count(*) FROM quads WHERE %s_hash = $1;", dir.String()), hash.toSQL()).Scan(&size)
	if err != nil {
		clog.Errorf("Error getting size from SQL database: %v", err)
		return 0
	}
	qs.sizes.Put(hash.String()+string(dir.Prefix()), size)
	return size
}
