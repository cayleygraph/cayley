package mongo

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/nosql"
)

const Type = "mongo"

var (
	_ nosql.BatchInserter = (*DB)(nil)
)

func init() {
	nosql.Register(Type, nosql.Registration{
		NewFunc:      Open,
		InitFunc:     Create,
		IsPersistent: true,
	})
}

func dialMongo(addr string, options graph.Options) (*mgo.Session, error) {
	if connVal, ok := options["session"]; ok {
		if conn, ok := connVal.(*mgo.Session); ok {
			return conn, nil
		}
	}
	if strings.HasPrefix(addr, "mongodb://") || strings.ContainsAny(addr, `@/\`) {
		// full mongodb url
		return mgo.Dial(addr)
	}
	var dialInfo mgo.DialInfo
	dialInfo.Addrs = strings.Split(addr, ",")
	user, ok, err := options.StringKey("username")
	if err != nil {
		return nil, err
	}
	if ok {
		dialInfo.Username = user
		password, ok, err := options.StringKey("password")
		if err != nil {
			return nil, err
		}
		if ok {
			dialInfo.Password = password
		}
	}
	dbName := nosql.DefaultDBName
	val, ok, err := options.StringKey("database_name")
	if err != nil {
		return nil, err
	}
	if ok {
		dbName = val
	}
	dialInfo.Database = dbName
	return mgo.DialWithInfo(&dialInfo)
}

func dialDB(addr string, opt graph.Options) (*DB, error) {
	sess, err := dialMongo(addr, opt)
	if err != nil {
		return nil, err
	}
	return &DB{
		sess: sess, db: sess.DB(""),
		colls: make(map[string]collection),
	}, nil
}

func Create(addr string, opt graph.Options) (nosql.Database, error) {
	return dialDB(addr, opt)
}

func Open(addr string, opt graph.Options) (nosql.Database, error) {
	return dialDB(addr, opt)
}

type collection struct {
	c         *mgo.Collection
	compPK    bool // compose PK from existing keys; if false, use _id instead of target field
	primary   nosql.Index
	secondary []nosql.Index
}

type DB struct {
	sess  *mgo.Session
	db    *mgo.Database
	colls map[string]collection
}

func (db *DB) Close() error {
	db.sess.Close()
	return nil
}
func (db *DB) EnsureIndex(ctx context.Context, col string, primary nosql.Index, secondary []nosql.Index) error {
	if primary.Type != nosql.StringExact {
		return fmt.Errorf("unsupported type of primary index: %v", primary.Type)
	}
	c := db.db.C(col)
	compPK := len(primary.Fields) != 1
	if compPK {
		err := c.EnsureIndex(mgo.Index{
			Key:    []string(primary.Fields),
			Unique: true,
		})
		if err != nil {
			return err
		}
	}
	for _, ind := range secondary {
		err := c.EnsureIndex(mgo.Index{
			Key:        []string(ind.Fields),
			Unique:     false,
			Background: true,
			Sparse:     true,
		})
		if err != nil {
			return err
		}
	}
	db.colls[col] = collection{
		c:         c,
		compPK:    compPK,
		primary:   primary,
		secondary: secondary,
	}
	return nil
}
func toBsonValue(v nosql.Value) interface{} {
	switch v := v.(type) {
	case nil:
		return nil
	case nosql.Document:
		return toBsonDoc(v)
	case nosql.Strings:
		return []string(v)
	case nosql.String:
		return string(v)
	case nosql.Int:
		return int64(v)
	case nosql.Float:
		return float64(v)
	case nosql.Bool:
		return bool(v)
	case nosql.Time:
		return time.Time(v)
	case nosql.Bytes:
		return []byte(v)
	default:
		panic(fmt.Errorf("unsupported type: %T", v))
	}
}
func fromBsonValue(v interface{}) nosql.Value {
	switch v := v.(type) {
	case nil:
		return nil
	case bson.M:
		return fromBsonDoc(v)
	case []interface{}:
		arr := make(nosql.Strings, 0, len(v))
		for _, s := range v {
			sv := fromBsonValue(s)
			str, ok := sv.(nosql.String)
			if !ok {
				panic(fmt.Errorf("unsupported value in array: %T", sv))
			}
			arr = append(arr, string(str))
		}
		return arr
	case bson.ObjectId:
		return nosql.String(objidString(v))
	case string:
		return nosql.String(v)
	case int:
		return nosql.Int(v)
	case int64:
		return nosql.Int(v)
	case float64:
		return nosql.Float(v)
	case bool:
		return nosql.Bool(v)
	case time.Time:
		return nosql.Time(v)
	case []byte:
		return nosql.Bytes(v)
	default:
		panic(fmt.Errorf("unsupported type: %T", v))
	}
}
func toBsonDoc(d nosql.Document) bson.M {
	if d == nil {
		return nil
	}
	m := make(bson.M, len(d))
	for k, v := range d {
		m[k] = toBsonValue(v)
	}
	return m
}
func fromBsonDoc(d bson.M) nosql.Document {
	if d == nil {
		return nil
	}
	m := make(nosql.Document, len(d))
	for k, v := range d {
		m[k] = fromBsonValue(v)
	}
	return m
}

const idField = "_id"

func (c *collection) getKey(m bson.M) nosql.Key {
	if !c.compPK {
		// key field renamed to _id - just return it
		if v, ok := m[idField].(string); ok {
			return nosql.Key{v}
		}
		return nil
	}
	// key field computed from multiple source fields
	// get source fields from document in correct order
	key := make(nosql.Key, 0, len(c.primary.Fields))
	for _, f := range c.primary.Fields {
		s, _ := m[f].(string)
		key = append(key, s)
	}
	return key
}

func (c *collection) setKey(m bson.M, key nosql.Key) {
	if !c.compPK {
		// delete source field, since we already added it as _id
		delete(m, c.primary.Fields[0])
	} else {
		for i, f := range c.primary.Fields {
			m[f] = string(key[i])
		}
	}
}

func (c *collection) convDoc(m bson.M) nosql.Document {
	if c.compPK {
		// key field computed from multiple source fields - remove it
		delete(m, idField)
	} else {
		// key field renamed - set correct name
		if v, ok := m[idField].(string); ok {
			delete(m, idField)
			m[c.primary.Fields[0]] = string(v)
		}
	}
	return fromBsonDoc(m)
}

func getOrGenID(key nosql.Key) (nosql.Key, string) {
	var mid string
	if key == nil {
		// TODO: maybe allow to pass custom key types as nosql.Key
		oid := objidString(bson.NewObjectId())
		mid = oid
		key = nosql.Key{oid}
	} else {
		mid = compKey(key)
	}
	return key, mid
}

func (c *collection) convIns(key nosql.Key, d nosql.Document) (nosql.Key, bson.M) {
	m := toBsonDoc(d)

	var mid string
	key, mid = getOrGenID(key)
	m[idField] = mid
	c.setKey(m, key)

	return key, m
}

func objidString(id bson.ObjectId) string {
	return hex.EncodeToString([]byte(id))
}

func compKey(key nosql.Key) string {
	if len(key) == 1 {
		return key[0]
	}
	return strings.Join(key, "")
}

func (db *DB) Insert(ctx context.Context, col string, key nosql.Key, d nosql.Document) (nosql.Key, error) {
	c, ok := db.colls[col]
	if !ok {
		return nil, fmt.Errorf("collection %q not found", col)
	}
	key, m := c.convIns(key, d)
	if err := c.c.Insert(m); err != nil {
		return nil, err
	}
	return key, nil
}
func (db *DB) FindByKey(ctx context.Context, col string, key nosql.Key) (nosql.Document, error) {
	c := db.colls[col]
	var m bson.M
	err := c.c.FindId(compKey(key)).One(&m)
	if err == mgo.ErrNotFound {
		return nil, nosql.ErrNotFound
	} else if err != nil {
		return nil, err
	}
	return c.convDoc(m), nil
}
func (db *DB) Query(col string) nosql.Query {
	c := db.colls[col]
	return &Query{c: &c}
}
func (db *DB) Update(col string, key nosql.Key) nosql.Update {
	c := db.colls[col]
	return &Update{col: &c, key: key, update: make(bson.M)}
}
func (db *DB) Delete(col string) nosql.Delete {
	c := db.colls[col]
	return &Delete{col: &c}
}

func buildFilters(filters []nosql.FieldFilter) bson.M {
	m := make(bson.M, len(filters))
	for _, f := range filters {
		v := toBsonValue(f.Value)
		var mf interface{}
		switch f.Filter {
		case nosql.Equal:
			mf = v
		case nosql.NotEqual:
			mf = bson.M{"$ne": v}
		case nosql.GT:
			mf = bson.M{"$gt": v}
		case nosql.GTE:
			mf = bson.M{"$gte": v}
		case nosql.LT:
			mf = bson.M{"$lt": v}
		case nosql.LTE:
			mf = bson.M{"$lte": v}
		default:
			panic(fmt.Errorf("unsupported filter: %v", f.Filter))
		}
		m[strings.Join(f.Path, ".")] = mf
	}
	return m
}

func mergeFilters(dst, src bson.M) {
	for k, v := range src {
		dst[k] = v
	}
}

type Query struct {
	c     *collection
	limit int
	query bson.M
}

func (q *Query) WithFields(filters ...nosql.FieldFilter) nosql.Query {
	m := buildFilters(filters)
	if q.query == nil {
		q.query = m
	} else {
		mergeFilters(q.query, m)
	}
	return q
}
func (q *Query) Limit(n int) nosql.Query {
	q.limit = n
	return q
}
func (q *Query) build() *mgo.Query {
	var m interface{}
	if q.query != nil {
		m = q.query
	}
	qu := q.c.c.Find(m)
	if q.limit > 0 {
		qu = qu.Limit(q.limit)
	}
	return qu
}
func (q *Query) Count(ctx context.Context) (int64, error) {
	n, err := q.build().Count()
	return int64(n), err
}
func (q *Query) One(ctx context.Context) (nosql.Document, error) {
	var m bson.M
	err := q.build().One(&m)
	if err == mgo.ErrNotFound {
		return nil, nosql.ErrNotFound
	} else if err != nil {
		return nil, err
	}
	return q.c.convDoc(m), nil
}
func (q *Query) Iterate() nosql.DocIterator {
	it := q.build().Iter()
	return &Iterator{it: it, c: q.c}
}

type Iterator struct {
	c   *collection
	it  *mgo.Iter
	res bson.M
}

func (it *Iterator) Next(ctx context.Context) bool {
	it.res = make(bson.M)
	return it.it.Next(&it.res)
}
func (it *Iterator) Err() error {
	return it.it.Err()
}
func (it *Iterator) Close() error {
	return it.it.Close()
}
func (it *Iterator) Key() nosql.Key {
	return it.c.getKey(it.res)
}
func (it *Iterator) Doc() nosql.Document {
	return it.c.convDoc(it.res)
}

type Delete struct {
	col   *collection
	query bson.M
}

func (d *Delete) WithFields(filters ...nosql.FieldFilter) nosql.Delete {
	m := buildFilters(filters)
	if d.query == nil {
		d.query = m
	} else {
		mergeFilters(d.query, m)
	}
	return d
}
func (d *Delete) Keys(keys ...nosql.Key) nosql.Delete {
	if len(keys) == 0 {
		return d
	}
	m := make(bson.M, 1)
	if len(keys) == 1 {
		m[idField] = compKey(keys[0])
	} else {
		ids := make([]string, 0, len(keys))
		for _, k := range keys {
			ids = append(ids, compKey(k))
		}
		m[idField] = bson.M{"$in": ids}
	}
	if d.query == nil {
		d.query = m
	} else {
		mergeFilters(d.query, m)
	}
	return d
}
func (d *Delete) Do(ctx context.Context) error {
	var qu interface{}
	if d.query != nil {
		qu = d.query
	}
	_, err := d.col.c.RemoveAll(qu)
	return err
}

type Update struct {
	col    *collection
	key    nosql.Key
	upsert bson.M
	update bson.M
}

func (u *Update) Inc(field string, dn int) nosql.Update {
	inc, _ := u.update["$inc"].(bson.M)
	if inc == nil {
		inc = make(bson.M)
	}
	inc[field] = dn
	u.update["$inc"] = inc
	return u
}
func (u *Update) Push(field string, v nosql.Value) nosql.Update {
	push, _ := u.update["$push"].(bson.M)
	if push == nil {
		push = make(bson.M)
	}
	push[field] = toBsonValue(v)
	u.update["$push"] = push
	return u
}
func (u *Update) Upsert(d nosql.Document) nosql.Update {
	u.upsert = toBsonDoc(d)
	if u.upsert == nil {
		u.upsert = make(bson.M)
	}
	u.col.setKey(u.upsert, u.key)
	return u
}
func (u *Update) Do(ctx context.Context) error {
	key := compKey(u.key)
	var err error
	if u.upsert != nil {
		if len(u.upsert) != 0 {
			u.update["$setOnInsert"] = u.upsert
		}
		_, err = u.col.c.UpsertId(key, u.update)
	} else {
		err = u.col.c.UpdateId(key, u.update)
	}
	return err
}

func (db *DB) BatchInsert(col string) nosql.DocWriter {
	c := db.colls[col]
	return &inserter{col: &c}
}

const batchSize = 100

type inserter struct {
	col   *collection
	buf   []interface{}
	ikeys []nosql.Key
	keys  []nosql.Key
	err   error
}

func (w *inserter) WriteDoc(ctx context.Context, key nosql.Key, d nosql.Document) error {
	if len(w.buf) >= batchSize {
		if err := w.Flush(ctx); err != nil {
			return err
		}
	}
	key, m := w.col.convIns(key, d)
	w.buf = append(w.buf, m)
	w.ikeys = append(w.ikeys, key)
	return nil
}

func (w *inserter) Flush(ctx context.Context) error {
	if len(w.buf) == 0 {
		return w.err
	}
	if err := w.col.c.Insert(w.buf...); err != nil {
		w.err = err
		return err
	}
	w.keys = append(w.keys, w.ikeys...)
	w.ikeys = w.ikeys[:0]
	w.buf = w.buf[:0]
	return w.err
}

func (w *inserter) Keys() []nosql.Key {
	return w.keys
}

func (w *inserter) Close() error {
	w.ikeys = nil
	w.buf = nil
	return w.err
}
