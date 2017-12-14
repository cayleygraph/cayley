package sql

import (
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/internal/lru"
	"github.com/cayleygraph/cayley/quad"
	"github.com/cayleygraph/cayley/quad/pquads"
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

type NodeHash [quad.HashSize]byte

func (NodeHash) IsNode() bool       { return true }
func (h NodeHash) Key() interface{} { return h }
func (h NodeHash) Valid() bool {
	return h != NodeHash{}
}
func (h NodeHash) SQLValue() interface{} {
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

func HashOf(s quad.Value) (out NodeHash) {
	if s == nil {
		return
	}
	quad.HashTo(s, out[:])
	return
}

type QuadHashes [4]NodeHash

func (QuadHashes) IsNode() bool       { return false }
func (q QuadHashes) Key() interface{} { return q }
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
func (q *QuadHashes) Set(d quad.Direction, h NodeHash) {
	switch d {
	case quad.Subject:
		q[0] = h
	case quad.Predicate:
		q[1] = h
	case quad.Object:
		q[2] = h
	case quad.Label:
		q[3] = h
	default:
		panic(fmt.Errorf("unknown direction: %v", d))
	}
}

type QuadStore struct {
	db           *sql.DB
	opt          *Optimizer
	flavor       Registration
	ids          *lru.Cache
	sizes        *lru.Cache
	noSizes      bool
	useEstimates bool

	mu   sync.RWMutex
	size int64
}

func connect(addr string, flavor string, opts graph.Options) (*sql.DB, error) {
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

const defaultFlavor = "postgres"

func typeFromOpts(opts graph.Options) string {
	flavor, _, _ := opts.StringKey("flavor")
	if flavor == "" {
		flavor = defaultFlavor
	}
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
		size:    -1,
		sizes:   lru.New(1024),
		ids:     lru.New(1024),
		noSizes: true, // Skip size checking by default.
	}
	if qs.flavor.NoOffsetWithoutLimit {
		qs.opt.NoOffsetWithoutLimit()
	}

	if local, ok, err := options.BoolKey("local_optimize"); err != nil {
		return nil, err
	} else if ok && local {
		qs.noSizes = false
	}
	if qs.useEstimates, _, err = options.BoolKey("use_estimates"); err != nil {
		return nil, err
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

func (qs *QuadStore) ApplyDeltas(in []graph.Delta, opts graph.IgnoreOpts) error {
	tx, err := qs.db.Begin()
	if err != nil {
		clog.Errorf("couldn't begin write transaction: %v", err)
		return err
	}
	err = qs.flavor.RunTx(tx, in, opts)
	if err != nil {
		tx.Rollback()
		return err
	}
	qs.mu.Lock()
	qs.size = -1 // TODO(barakmich): Sync size with writes.
	qs.mu.Unlock()
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
	v := val.(Value)
	sel := AllQuads("")
	sel.WhereEq("", dirField(d), v)
	return qs.NewIterator(sel)
}

func (qs *QuadStore) NodesAllIterator() graph.Iterator {
	return qs.NewIterator(AllNodes())
}

func (qs *QuadStore) QuadsAllIterator() graph.Iterator {
	return qs.NewIterator(AllQuads(""))
}

func (qs *QuadStore) ValueOf(s quad.Value) graph.Value {
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

func (qs *QuadStore) Size() int64 {
	qs.mu.RLock()
	sz := qs.size
	qs.mu.RUnlock()
	if sz >= 0 {
		return sz
	}

	query := "SELECT COUNT(*) FROM quads;"
	if qs.useEstimates && qs.flavor.Estimated != nil {
		query = qs.flavor.Estimated("quads")
	}

	err := qs.db.QueryRow(query).Scan(&sz)
	if err != nil {
		clog.Errorf("Couldn't execute COUNT: %v", err)
		return 0
	}
	qs.mu.Lock()
	qs.size = sz
	qs.mu.Unlock()
	return sz
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

func (qs *QuadStore) Close() error {
	return qs.db.Close()
}

func (qs *QuadStore) QuadDirection(in graph.Value, d quad.Direction) graph.Value {
	return NodeHash(in.(QuadHashes).Get(d))
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
		fmt.Sprintf("SELECT count(*) FROM quads WHERE %s_hash = "+qs.flavor.Placeholder(1)+";", dir.String()), hash.SQLValue()).Scan(&size)
	if err != nil {
		clog.Errorf("Error getting size from SQL database: %v", err)
		return 0
	}
	qs.sizes.Put(hash.String()+string(dir.Prefix()), size)
	return size
}
