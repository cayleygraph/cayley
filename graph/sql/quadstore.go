package sql

import (
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/internal/lru"
	"github.com/cayleygraph/cayley/quad"
	"github.com/cayleygraph/cayley/quad/pquads"
)

const QuadStoreType = "sql"

func init() {
	graph.RegisterQuadStore(QuadStoreType, graph.QuadStoreRegistration{
		NewFunc:      newQuadStore,
		UpgradeFunc:  nil,
		InitFunc:     createSQLTables,
		IsPersistent: true,
	})
}

type NodeHash [quad.HashSize]byte

func (NodeHash) IsNode() bool       { return true }
func (h NodeHash) Key() interface{} { return h }
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

type QuadStore struct {
	db           *sql.DB
	flavor       Flavor
	size         int64
	ids          *lru.Cache
	sizes        *lru.Cache
	noSizes      bool
	useEstimates bool
}

type Flavor struct {
	Name                string
	Driver              string
	NodesTable          string
	QuadsTable          string
	FieldQuote          rune
	Placeholder         func(int) string
	Indexes             func(graph.Options) []string
	Error               func(error) error
	Estimated           func(table string) string
	RunTx               func(tx *sql.Tx, in []graph.Delta, opts graph.IgnoreOpts) error
	NoSchemaChangesInTx bool
}

var flavors = make(map[string]Flavor)

func RegisterFlavor(f Flavor) {
	flavors[f.Name] = f
}

const defaultFlavor = "postgres"

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

func createSQLTables(addr string, options graph.Options) error {
	flavor, _, _ := options.StringKey("flavor")
	if flavor == "" {
		flavor = defaultFlavor
	}
	fl, ok := flavors[flavor]
	if !ok {
		return fmt.Errorf("unsupported sql flavor: %s", flavor)
	}
	dr := fl.Driver
	if dr == "" {
		dr = fl.Name
	}
	conn, err := connect(addr, dr, options)
	if err != nil {
		return err
	}
	defer conn.Close()

	if fl.NoSchemaChangesInTx {
		_, err = conn.Exec(fl.NodesTable)
		if err != nil {
			err = fl.Error(err)
			clog.Errorf("Cannot create nodes table: %v", err)
			return err
		}
		_, err = conn.Exec(fl.QuadsTable)
		if err != nil {
			err = fl.Error(err)
			clog.Errorf("Cannot create quad table: %v", err)
			return err
		}
		for _, index := range fl.Indexes(options) {
			if _, err = conn.Exec(index); err != nil {
				clog.Errorf("Cannot create index: %v", err)
				return err
			}
		}
		return nil
	}

	tx, err := conn.Begin()
	if err != nil {
		clog.Errorf("Couldn't begin creation transaction: %s", err)
		return err
	}

	_, err = tx.Exec(fl.NodesTable)
	if err != nil {
		tx.Rollback()
		err = fl.Error(err)
		clog.Errorf("Cannot create nodes table: %v", err)
		return err
	}
	_, err = tx.Exec(fl.QuadsTable)
	if err != nil {
		tx.Rollback()
		err = fl.Error(err)
		clog.Errorf("Cannot create quad table: %v", err)
		return err
	}
	for _, index := range fl.Indexes(options) {
		if _, err = tx.Exec(index); err != nil {
			clog.Errorf("Cannot create index: %v", err)
			tx.Rollback()
			return err
		}
	}
	tx.Commit()
	return nil
}

func newQuadStore(addr string, options graph.Options) (graph.QuadStore, error) {
	flavor, _, _ := options.StringKey("flavor")
	if flavor == "" {
		flavor = defaultFlavor
	}
	fl, ok := flavors[flavor]
	if !ok {
		return nil, fmt.Errorf("unsupported sql flavor: %s", flavor)
	}
	dr := fl.Driver
	if dr == "" {
		dr = fl.Name
	}
	var qs QuadStore
	conn, err := connect(addr, dr, options)
	if err != nil {
		return nil, err
	}
	localOpt, localOptOk, err := options.BoolKey("local_optimize")
	if err != nil {
		return nil, err
	}
	qs.db = conn
	qs.flavor = fl
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
	qs.size = -1 // TODO(barakmich): Sync size with writes.
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
	if qs.useEstimates && qs.flavor.Estimated != nil {
		query = qs.flavor.Estimated("quads")
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
		fmt.Sprintf("SELECT count(*) FROM quads WHERE %s_hash = "+qs.flavor.Placeholder(1)+";", dir.String()), hash.toSQL()).Scan(&size)
	if err != nil {
		clog.Errorf("Error getting size from SQL database: %v", err)
		return 0
	}
	qs.sizes.Put(hash.String()+string(dir.Prefix()), size)
	return size
}
