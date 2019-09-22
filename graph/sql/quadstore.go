package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/graph/log"
	"github.com/cayleygraph/cayley/internal/lru"
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/pquads"
)

// Type string for generic sql QuadStore.
//
// Deprecated: use specific types from sub-packages.
const QuadStoreType = "sql"

func init() {
	// Deprecated QS registration that resolves backend type via "flavor" option.
	registerQuadStore(QuadStoreType, "")
}

func registerQuadStore(name, typ string) {
	graph.RegisterQuadStore(name, graph.QuadStoreRegistration{
		NewFunc: func(addr string, options graph.Options) (graph.QuadStore, error) {
			return New(typ, addr, options)
		},
		UpgradeFunc: nil,
		InitFunc: func(addr string, options graph.Options) error {
			return Init(typ, addr, options)
		},
		IsPersistent: true,
	})
}

var _ Value = StringVal("")

type StringVal string

func (v StringVal) SQLValue() interface{} {
	return escapeNullByte(string(v))
}

type IntVal int64

func (v IntVal) SQLValue() interface{} {
	return int64(v)
}

type FloatVal float64

func (v FloatVal) SQLValue() interface{} {
	return float64(v)
}

type BoolVal bool

func (v BoolVal) SQLValue() interface{} {
	return bool(v)
}

type TimeVal time.Time

func (v TimeVal) SQLValue() interface{} {
	return time.Time(v)
}

type NodeHash struct {
	graph.ValueHash
}

func (h NodeHash) SQLValue() interface{} {
	if !h.Valid() {
		return nil
	}
	return []byte(h.ValueHash[:])
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
	copy(h.ValueHash[:], b)
	return nil
}

func HashOf(s quad.Value) NodeHash {
	return NodeHash{graph.HashOf(s)}
}

type QuadHashes struct {
	graph.QuadHash
}

type QuadStore struct {
	db      *sql.DB
	opt     *Optimizer
	flavor  Registration
	ids     *lru.Cache
	sizes   *lru.Cache
	noSizes bool

	mu    sync.RWMutex
	nodes int64
	quads int64
}

func connect(addr string, flavor string, opts graph.Options) (*sql.DB, error) {
	maxOpenConnections, err := opts.IntKey("maxopenconnections", -1)

	if err != nil {
		return nil, fmt.Errorf("could not retrieve maxopenconnections from options: %v", err)
	}

	maxIdleConnections, err := opts.IntKey("maxidleconnections", -1)

	if err != nil {
		return nil, fmt.Errorf("could not retrieve maxIdleConnections from options: %v", err)
	}

	connMaxLifetime, err := opts.StringKey("connmaxlifetime", "")

	if err != nil {
		return nil, fmt.Errorf("could not retrieve connmaxlifetime from options: %v", err)
	}

	var connDuration time.Duration
	if connMaxLifetime != "" {
		connDuration, err = time.ParseDuration(connMaxLifetime)

		if err != nil {
			return nil, fmt.Errorf("couldn't parse connmaxlifetime string: %v", err)
		}
	}

	// TODO(barakmich): Parse options for more friendly addr
	conn, err := sql.Open(flavor, addr)
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

	if maxOpenConnections != -1 {
		conn.SetMaxOpenConns(maxOpenConnections)
	}
	if maxIdleConnections != -1 {
		conn.SetMaxIdleConns(maxIdleConnections)
	}
	if connDuration != 0 {
		conn.SetConnMaxLifetime(connDuration)
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

func typeFromOpts(opts graph.Options) string {
	flavor, _ := opts.StringKey("flavor", "postgres")
	return flavor
}

func Init(typ string, addr string, options graph.Options) error {
	if typ == "" {
		typ = typeFromOpts(options)
	}
	fl, ok := types[typ]
	if !ok {
		return fmt.Errorf("unsupported sql database: %s", typ)
	}
	conn, err := connect(addr, fl.Driver, options)
	if err != nil {
		return err
	}
	defer conn.Close()

	nodesSql := fl.nodesTable()
	quadsSql := fl.quadsTable()
	indexes := fl.quadIndexes(options)

	if fl.NoSchemaChangesInTx {
		_, err = conn.Exec(nodesSql)
		if err != nil {
			err = fl.Error(err)
			clog.Errorf("Cannot create nodes table: %v", err)
			return err
		}
		_, err = conn.Exec(quadsSql)
		if err != nil {
			err = fl.Error(err)
			clog.Errorf("Cannot create quad table: %v", err)
			return err
		}
		for _, index := range indexes {
			if _, err = conn.Exec(index); err != nil {
				clog.Errorf("Cannot create index: %v", err)
				return err
			}
		}
	} else {
		tx, err := conn.Begin()
		if err != nil {
			clog.Errorf("Couldn't begin creation transaction: %s", err)
			return err
		}

		_, err = tx.Exec(nodesSql)
		if err != nil {
			tx.Rollback()
			err = fl.Error(err)
			clog.Errorf("Cannot create nodes table: %v", err)
			return err
		}
		_, err = tx.Exec(quadsSql)
		if err != nil {
			tx.Rollback()
			err = fl.Error(err)
			clog.Errorf("Cannot create quad table: %v", err)
			return err
		}
		for _, index := range indexes {
			if _, err = tx.Exec(index); err != nil {
				clog.Errorf("Cannot create index: %v", err)
				tx.Rollback()
				return err
			}
		}
		tx.Commit()
	}
	return nil
}

func New(typ string, addr string, options graph.Options) (graph.QuadStore, error) {
	if typ == "" {
		typ = typeFromOpts(options)
	}
	fl, ok := types[typ]
	if !ok {
		return nil, fmt.Errorf("unsupported sql database: %s", typ)
	}
	conn, err := connect(addr, fl.Driver, options)
	if err != nil {
		return nil, err
	}
	qs := &QuadStore{
		db:      conn,
		opt:     NewOptimizer(),
		flavor:  fl,
		quads:   -1,
		nodes:   -1,
		sizes:   lru.New(1024),
		ids:     lru.New(1024),
		noSizes: true, // Skip size checking by default.
	}
	qs.opt.SetRegexpOp(qs.flavor.RegexpOp)
	if qs.flavor.NoOffsetWithoutLimit {
		qs.opt.NoOffsetWithoutLimit()
	}

	if local, err := options.BoolKey("local_optimize", false); err != nil {
		return nil, err
	} else if ok && local {
		qs.noSizes = false
	}
	return qs, nil
}

func escapeNullByte(s string) string {
	return strings.Replace(s, "\u0000", `\x00`, -1)
}
func unescapeNullByte(s string) string {
	return strings.Replace(s, `\x00`, "\u0000", -1)
}

type ValueType int

func (t ValueType) Columns() []string {
	return nodeInsertColumns[t]
}

func NodeValues(h NodeHash, v quad.Value) (ValueType, []interface{}, error) {
	var (
		nodeKey ValueType
		values  = []interface{}{h.SQLValue(), nil, nil}[:1]
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

func (qs *QuadStore) NewQuadWriter() (quad.WriteCloser, error) {
	return &quadWriter{qs: qs}, nil
}

type quadWriter struct {
	qs     *QuadStore
	deltas []graph.Delta
}

func (w *quadWriter) WriteQuad(q quad.Quad) error {
	_, err := w.WriteQuads([]quad.Quad{q})
	return err
}

func (w *quadWriter) WriteQuads(buf []quad.Quad) (int, error) {
	// TODO(dennwc): write an optimized implementation
	w.deltas = w.deltas[:0]
	if cap(w.deltas) < len(buf) {
		w.deltas = make([]graph.Delta, 0, len(buf))
	}
	for _, q := range buf {
		w.deltas = append(w.deltas, graph.Delta{
			Quad: q, Action: graph.Add,
		})
	}
	err := w.qs.ApplyDeltas(w.deltas, graph.IgnoreOpts{
		IgnoreDup: true,
	})
	w.deltas = w.deltas[:0]
	if err != nil {
		return 0, err
	}
	return len(buf), nil
}

func (w *quadWriter) Close() error {
	w.deltas = nil
	return nil
}

func (qs *QuadStore) ApplyDeltas(in []graph.Delta, opts graph.IgnoreOpts) error {
	// first calculate values ref deltas
	deltas := graphlog.SplitDeltas(in)

	tx, err := qs.db.Begin()
	if err != nil {
		clog.Errorf("couldn't begin write transaction: %v", err)
		return err
	}

	retry := qs.flavor.TxRetry
	if retry == nil {
		retry = func(tx *sql.Tx, stmts func() error) error {
			return stmts()
		}
	}
	p := make([]string, 4)
	for i := range p {
		p[i] = qs.flavor.Placeholder(i + 1)
	}

	err = retry(tx, func() error {
		err = qs.flavor.RunTx(tx, deltas.IncNode, deltas.QuadAdd, opts)
		if err != nil {
			return err
		}
		// quad delete is also generic, execute here
		var (
			deleteQuad   *sql.Stmt
			deleteTriple *sql.Stmt
		)
		fixNodes := make(map[graph.ValueHash]int)
		for _, d := range deltas.QuadDel {
			dirs := make([]interface{}, 0, len(quad.Directions))
			for _, h := range d.Quad.Dirs() {
				dirs = append(dirs, NodeHash{h}.SQLValue())
			}
			if deleteQuad == nil {
				deleteQuad, err = tx.Prepare(`DELETE FROM quads WHERE subject_hash=` + p[0] + ` and predicate_hash=` + p[1] + ` and object_hash=` + p[2] + ` and label_hash=` + p[3] + `;`)
				if err != nil {
					return err
				}
				deleteTriple, err = tx.Prepare(`DELETE FROM quads WHERE subject_hash=` + p[0] + ` and predicate_hash=` + p[1] + ` and object_hash=` + p[2] + ` and label_hash is null;`)
				if err != nil {
					return err
				}
			}
			stmt := deleteQuad
			if i := len(dirs) - 1; dirs[i] == nil {
				stmt = deleteTriple
				dirs = dirs[:i]
			}
			result, err := stmt.Exec(dirs...)
			if err != nil {
				clog.Errorf("couldn't exec DELETE statement: %v", err)
				return err
			}
			affected, err := result.RowsAffected()
			if err != nil {
				clog.Errorf("couldn't get DELETE RowsAffected: %v", err)
				return err
			}
			if affected != 1 {
				if !opts.IgnoreMissing {
					// TODO: reference to delta
					return &graph.DeltaError{Err: graph.ErrQuadNotExist}
				}
				// revert counters for all directions of this quad
				for _, dir := range quad.Directions {
					if h := d.Quad.Get(dir); h.Valid() {
						fixNodes[h]++
					}
				}
			}
		}
		if len(deltas.DecNode) == 0 {
			return nil
		}
		// node update SQL is generic enough to run it here
		updateNode, err := tx.Prepare(`UPDATE nodes SET refs = refs + ` + p[0] + ` WHERE hash = ` + p[1] + `;`)
		if err != nil {
			return err
		}
		for _, n := range deltas.DecNode {
			n.RefInc += fixNodes[n.Hash]
			if n.RefInc == 0 {
				continue
			}
			_, err := updateNode.Exec(n.RefInc, NodeHash{n.Hash}.SQLValue())
			if err != nil {
				clog.Errorf("couldn't exec UPDATE statement: %v", err)
				return err
			}
		}
		// and remove unused nodes at last
		_, err = tx.Exec(`DELETE FROM nodes WHERE refs <= 0;`)
		if err != nil {
			clog.Errorf("couldn't exec DELETE nodes statement: %v", err)
			return err
		}
		return nil
	})
	if err != nil {
		tx.Rollback()
		return err
	}

	qs.mu.Lock()
	// TODO(barakmich): Sync size with writes.
	qs.quads = -1
	qs.nodes = -1
	qs.mu.Unlock()
	return tx.Commit()
}

func (qs *QuadStore) Quad(val graph.Ref) quad.Quad {
	h := val.(QuadHashes)
	return quad.Quad{
		Subject:   qs.NameOf(h.Get(quad.Subject)),
		Predicate: qs.NameOf(h.Get(quad.Predicate)),
		Object:    qs.NameOf(h.Get(quad.Object)),
		Label:     qs.NameOf(h.Get(quad.Label)),
	}
}

func (qs *QuadStore) QuadIterator(d quad.Direction, val graph.Ref) graph.Iterator {
	v, ok := val.(Value)
	if !ok {
		return iterator.NewNull()
	}
	sel := AllQuads("")
	sel.WhereEq("", dirField(d), v)
	return qs.NewIterator(sel)
}

func (qs *QuadStore) querySize(ctx context.Context, sel Select) (graph.Size, error) {
	sel.Fields = []Field{
		{Name: "COUNT(*)", Raw: true}, // TODO: proper support for expressions
	}
	var sz int64
	err := qs.QueryRow(ctx, sel).Scan(&sz)
	if err != nil {
		return graph.Size{}, err
	}
	return graph.Size{
		Size:  sz,
		Exact: true,
	}, nil
}

func (qs *QuadStore) QuadIteratorSize(ctx context.Context, d quad.Direction, val graph.Ref) (graph.Size, error) {
	v, ok := val.(Value)
	if !ok {
		return graph.Size{Size: 0, Exact: true}, nil
	}
	sel := AllQuads("")
	sel.WhereEq("", dirField(d), v)
	return qs.querySize(ctx, sel)
}

func (qs *QuadStore) NodesAllIterator() graph.Iterator {
	return qs.NewIterator(AllNodes())
}

func (qs *QuadStore) QuadsAllIterator() graph.Iterator {
	return qs.NewIterator(AllQuads(""))
}

func (qs *QuadStore) ValueOf(s quad.Value) graph.Ref {
	return NodeHash(HashOf(s))
}

// NullTime represents a time.Time that may be null. NullTime implements the
// sql.Scanner interface so it can be used as a scan destination, similar to
// sql.NullString.
type NullTime struct {
	Time  time.Time
	Valid bool // Valid is true if Time is not NULL
}

// Scan implements the Scanner interface.
func (nt *NullTime) Scan(value interface{}) error {
	if value == nil {
		nt.Time, nt.Valid = time.Time{}, false
		return nil
	}
	switch value := value.(type) {
	case time.Time:
		nt.Time, nt.Valid = value, true
	case []byte:
		t, err := time.Parse("2006-01-02 15:04:05.999999", string(value))
		if err != nil {
			return err
		}
		nt.Time, nt.Valid = t, true
	default:
		return fmt.Errorf("unsupported time format: %T: %v", value, value)
	}
	return nil
}

// Value implements the driver Valuer interface.
func (nt NullTime) Value() (driver.Value, error) {
	if !nt.Valid {
		return nil, nil
	}
	return nt.Time, nil
}

func (qs *QuadStore) NameOf(v graph.Ref) quad.Value {
	if v == nil {
		if clog.V(2) {
			clog.Infof("NameOf was nil")
		}
		return nil
	} else if v, ok := v.(graph.PreFetchedValue); ok {
		return v.NameOf()
	}
	var hash NodeHash
	switch h := v.(type) {
	case graph.PreFetchedValue:
		return h.NameOf()
	case NodeHash:
		hash = h
	case graph.ValueHash:
		hash = NodeHash{h}
	default:
		panic(fmt.Errorf("unexpected token: %T", v))
	}
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
	FROM nodes WHERE hash = ` + qs.flavor.Placeholder(1) + ` LIMIT 1;`
	c := qs.db.QueryRow(query, hash.SQLValue())
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
		vtime  NullTime
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
		if err != sql.ErrNoRows {
			clog.Errorf("Couldn't execute value lookup: %v", err)
		}
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

func (qs *QuadStore) Stats(ctx context.Context, exact bool) (graph.Stats, error) {
	st := graph.Stats{
		Nodes: graph.Size{Exact: true},
		Quads: graph.Size{Exact: true},
	}
	qs.mu.RLock()
	st.Quads.Size = qs.quads
	st.Nodes.Size = qs.nodes
	qs.mu.RUnlock()
	if st.Quads.Size >= 0 {
		return st, nil
	}
	query := func(table string) string {
		return "SELECT COUNT(*) FROM " + table + ";"
	}
	if !exact && qs.flavor.Estimated != nil {
		query = qs.flavor.Estimated
		st.Quads.Exact = false
		st.Nodes.Exact = false
	}
	err := qs.db.QueryRow(query("quads")).Scan(&st.Quads.Size)
	if err != nil {
		return graph.Stats{}, err
	}
	err = qs.db.QueryRow(query("nodes")).Scan(&st.Nodes.Size)
	if err != nil {
		return graph.Stats{}, err
	}
	if st.Quads.Exact {
		qs.mu.Lock()
		qs.quads = st.Quads.Size
		qs.nodes = st.Nodes.Size
		qs.mu.Unlock()
	}
	return st, nil
}

func (qs *QuadStore) Close() error {
	return qs.db.Close()
}

func (qs *QuadStore) QuadDirection(in graph.Ref, d quad.Direction) graph.Ref {
	return NodeHash{in.(QuadHashes).Get(d)}
}

func (qs *QuadStore) sizeForIterator(dir quad.Direction, hash NodeHash) int64 {
	var err error
	if qs.noSizes {
		st, _ := qs.Stats(context.TODO(), false)
		if dir == quad.Predicate {
			return (st.Quads.Size / 100) + 1
		}
		return (st.Quads.Size / 1000) + 1
	}
	if val, ok := qs.sizes.Get(hash.String() + string(dir.Prefix())); ok {
		return val.(int64)
	}
	var size int64
	if clog.V(4) {
		clog.Infof("sql: getting size for select %s, %v", dir.String(), hash)
	}
	err = qs.db.QueryRow(
		fmt.Sprintf("SELECT count(*) FROM quads WHERE %s_hash = "+qs.flavor.Placeholder(1)+";", dir.String()), hash.SQLValue()).Scan(&size)
	if err != nil {
		clog.Errorf("Error getting size from SQL database: %v", err)
		return 0
	}
	qs.sizes.Put(hash.String()+string(dir.Prefix()), size)
	return size
}
