package ouch

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"runtime"
	"strings"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/nosql"

	"github.com/go-kivik/kivik"
)

const Type = driverName

var nosqlOptions = nosql.Options{
	Number32: true,
}

func init() {
	nosql.Register(Type, nosql.Registration{
		NewFunc:      Open,
		InitFunc:     Create,
		IsPersistent: true,
		Options:      nosqlOptions,
	})
}

func dialDB(create bool, addr string, opt graph.Options) (*DB, error) {
	ctx := context.TODO() // TODO - replace with parameter value

	addrParsed, err := url.Parse(addr)
	if err != nil {
		return nil, err
	}

	pathParts := strings.Split(addrParsed.Path, "/")
	dbName := ""
	if len(pathParts) > 0 && addr != "" {
		dbName = pathParts[len(pathParts)-1]
	} else {
		return nil, errors.New("unable to decypher database name from: " + addr)
	}
	dsn := strings.TrimSuffix(addr, dbName)

	client, err := kivik.New(ctx, driverName, dsn)
	if err != nil {
		return nil, fmt.Errorf("cannot open driver: %v", err)
	}

	var db *kivik.DB
	if create {
		db, err = client.CreateDB(ctx, dbName)
	} else {
		db, err = client.DB(ctx, dbName)
	}
	if err != nil {
		return nil, fmt.Errorf("cannot open db %q: %v", dbName, err)
	}

	return &DB{
		db:    db,
		colls: make(map[string]collection),
	}, nil
}

func Create(addr string, opt graph.Options) (nosql.Database, error) {
	return dialDB(true, addr, opt)
}

func Open(addr string, opt graph.Options) (nosql.Database, error) {
	return dialDB(false, addr, opt)
}

type collection struct {
	primary   nosql.Index
	secondary []nosql.Index
}

type DB struct {
	db    *kivik.DB
	colls map[string]collection
}

func (db *DB) Close() error {
	// seems no way to close a kivik session, so just let the Go garbage collection do its stuff...
	return nil
}

const (
	collectionIndex   = "collection-index"
	secondaryIndexFmt = "%s-secondary-%d"
)

func (db *DB) EnsureIndex(ctx context.Context, col string, primary nosql.Index, secondary []nosql.Index) error {

	if primary.Type != nosql.StringExact {
		return fmt.Errorf("unsupported type of primary index: %v", primary.Type)
	}

	db.colls[col] = collection{
		primary:   primary,
		secondary: secondary,
	}

	if err := db.db.CreateIndex(ctx, collectionIndex, collectionIndex,
		map[string]interface{}{
			"fields": []string{collectionField}, //  collection field only, default index
		}); err != nil {
		return err
	}

	// NOTE the field of the primary index is always "_id", so need not create an index

	for k, v := range db.colls[col].secondary {
		snam := fmt.Sprintf(secondaryIndexFmt, col, k)
		sindex := map[string]interface{}{
			"fields": v.Fields,
		}
		if err := db.db.CreateIndex(ctx, snam, snam, sindex); err != nil {
			return err
		}
	}

	return nil
}

const (
	idField         = "_id"
	revField        = "_rev"
	collectionField = "Collection"
)

func compKey(key nosql.Key) string {
	return strings.Join(key, "|")
}

func (db *DB) Insert(ctx context.Context, col string, key nosql.Key, d nosql.Document) (nosql.Key, error) {
	k, _, e := db.insert(ctx, col, key, d)
	return k, e
}
func (db *DB) insert(ctx context.Context, col string, key nosql.Key, d nosql.Document) (nosql.Key, string, error) {

	if d == nil {
		return nil, "", errors.New("no document to insert")
	}

	rev := ""
	if key == nil {
		key = nosql.GenKey()
	} else {
		var err error
		var id string
		_, id, rev, err = db.findByKey(ctx, col, key)
		if err == nil {
			rev, err = db.db.Delete(ctx, id, rev) // delete it to be sure it is removed
			if err != nil {
				return nil, "", err
			}
		}
	}

	if cP, found := db.colls[col]; found {
		// go through the key list an put each in
		if len(cP.primary.Fields) == len(key) {
			for idx, nam := range cP.primary.Fields {
				d[nam] = nosql.String(key[idx]) // just replace with the given key, even if there already
			}
		}
	}

	interfaceDoc := toOuchDoc(col, compKey(key), rev, d)

	_, rev, err := db.db.CreateDoc(ctx, interfaceDoc)
	if err != nil {
		return nil, "", err
	}

	return key, rev, nil
}

func (db *DB) FindByKey(ctx context.Context, col string, key nosql.Key) (nosql.Document, error) {
	decoded, _, _, err := db.findByKey(ctx, col, key)
	return decoded, err
}

func (db *DB) findByKey(ctx context.Context, col string, key nosql.Key) (nosql.Document, string, string, error) {
	cK := compKey(key)
	return db.findByOuchKey(ctx, cK)
}

func (db *DB) findByOuchKey(ctx context.Context, cK string) (nosql.Document, string, string, error) {
	row := db.db.Get(ctx, cK)
	if err := row.Err; err != nil {
		if kivik.StatusCode(err) == kivik.StatusNotFound {
			return nil, "", "", nosql.ErrNotFound
		}
		return nil, "", "", err
	}

	rowDoc := make(map[string]interface{})
	err := row.ScanDoc(&rowDoc)
	if err != nil {
		return nil, "", "", err
	}
	decoded := fromOuchDoc(rowDoc)

	return decoded, rowDoc[idField].(string), rowDoc[revField].(string), nil
}

func (db *DB) Query(col string) nosql.Query {
	qry := &Query{
		db:          db,
		col:         col,
		pathFilters: make(map[string][]nosql.FieldFilter),
		qu: map[string]interface{}{
			"selector": make(map[string]interface{}),
		},
	}
	if col != "" {
		qry.qu.putSelector(collectionField, col)
	}
	return qry
}
func (db *DB) Update(col string, key nosql.Key) nosql.Update {
	return &Update{db: db, col: col, key: key, update: nosql.Document{}}
}
func (db *DB) Delete(col string) nosql.Delete {
	return &Delete{db: db, col: col, q: db.Query(col).(*Query)}
}

type ouchQuery map[string]interface{}

func (q ouchQuery) clone() ouchQuery {
	if q == nil {
		return nil
	}
	out := make(ouchQuery, len(q))
	for k, v := range q {
		if m, ok := v.(map[string]interface{}); ok {
			v = map[string]interface{}(ouchQuery(m).clone())
		} else if m, ok := v.(ouchQuery); ok {
			v = m.clone()
		}
		out[k] = v
	}
	return out
}

func (q ouchQuery) putSelector(field string, v interface{}) {
	sel := q["selector"].(map[string]interface{})
	fs, ok := sel[field]
	if !ok {
		sel[field] = v
		return
	}
	fsel, ok := fs.(map[string]interface{})
	if !ok {
		// exact match in first filter - ignore second one
		return
	}
	fsel2, ok := v.(map[string]interface{})
	if !ok {
		// exact match - override filter
		sel[field] = v
		return
	}
	for k, v := range fsel2 {
		fsel[k] = v
	}
}

type Query struct {
	db          *DB
	col         string
	qu          ouchQuery
	pathFilters map[string][]nosql.FieldFilter
}

func (q *Query) WithFields(filters ...nosql.FieldFilter) nosql.Query {
	for _, filter := range filters {
		path := strings.Join(filter.Path, keySeparator)
		q.pathFilters[path] = append(q.pathFilters[path], filter)
	}
	return q
}

func (q *Query) buildFilters() {
	for jp, filterList := range q.pathFilters {
		term := map[string]interface{}{}
		for _, filter := range filterList {
			testValue := toOuchValue(filter.Value)
			test := ""
			switch filter.Filter {
			case nosql.Equal:
				test = "$eq"
			case nosql.NotEqual:
				if boolVal, isBool := testValue.(bool); isBool && boolVal && runtime.GOARCH != "js" {
					// Swap the logic of the test, which is required to make it work for missing values in CouchDB.
					// Sadly, this same formulation does not work for PouchDB, as that does not allow $or.
					test = "$or"
					testValue = []interface{}{
						map[string]interface{}{"$eq": false},     // it was $ne true
						map[string]interface{}{"$exists": false}, // non-existence => false
					}
				} else {
					test = "$ne"
				}
			case nosql.GT:
				test = "$gt"
			case nosql.GTE:
				test = "$gte"
			case nosql.LT:
				test = "$lt"
			case nosql.LTE:
				test = "$lte"
			case nosql.Regexp:
				test = "$regex"
			default:
				panic(fmt.Errorf("unknown nosqlFilter %v", filter.Filter))
			}
			term[test] = testValue
		}
		q.qu.putSelector(jp, term)
	}

	if len(q.pathFilters) == 0 {
		q.qu["use_index"] = collectionIndex
	} else {
		// NOTE primary is redundant, as the same as the _id
		c, haveCol := q.db.colls[q.col]
		if haveCol {
			for si, sv := range c.secondary {
				useSecondary := true
				for _, fieldName := range sv.Fields {
					if _, found := q.pathFilters[fieldName]; !found {
						useSecondary = false
					}
				}
				if useSecondary {
					q.qu["use_index"] = fmt.Sprintf(secondaryIndexFmt, q.col, si)
					break
				}
			}
		}
	}
}

func (q *Query) Limit(n int) nosql.Query {
	q.qu["limit"] = n
	return q
}

func (q *Query) Count(ctx context.Context) (int64, error) {
	// TODO it should be possible to use map/reduce logic, rather than a mango query, to speed this up, at least for some cases

	it := q.Iterate().(*Iterator)
	it.qu = it.qu.clone()
	// don't pull back any fields in the query, to reduce bandwidth
	it.qu["fields"] = []interface{}{}
	if !it.open(ctx) {
		return 0, it.Err()
	}
	defer it.Close()

	var count int64
	for it.rows.Next() { // for speed, use the native Next
		count++
	}
	return count, it.Err()
}

func (q *Query) One(ctx context.Context) (nosql.Document, error) {
	it := q.Iterate()
	defer it.Close()
	if err := it.Err(); err != nil {
		return nil, err
	}
	if it.Next(ctx) {
		return it.Doc(), it.Err()
	}
	return nil, nosql.ErrNotFound
}

func (q *Query) Iterate() nosql.DocIterator {
	q.buildFilters()

	// NOTE: to see that the query actually is using an index, uncomment the lines below
	// queryPlan, err := q.db.db.Explain(ctx, q.ouchQuery)
	// debug := fmt.Sprintf("DEBUG %v QueryPlan: %#v", err, queryPlan)
	// if strings.Contains(debug, collectionIndex) { // put the index you're interested in here
	// 	fmt.Println(debug)
	// }

	return &Iterator{db: q.db, col: q.col, qu: q.qu}
}

type Iterator struct {
	db     *DB
	col    string
	qu     ouchQuery
	err    error
	rows   *kivik.Rows
	doc    map[string]interface{}
	prevID interface{}
	closed bool
}

func (it *Iterator) open(ctx context.Context) bool {
	it.rows, it.err = it.db.db.Find(ctx, it.qu)
	return it.err == nil
}
func (it *Iterator) next(ctx context.Context) bool {
	it.doc = nil
	haveNext := it.rows.Next()
	it.err = it.rows.Err()
	if it.err != nil {
		return false
	} else if !haveNext {
		return false
	}
	it.scanDoc()
	return it.err == nil
}
func (it *Iterator) Next(ctx context.Context) bool {
	if it.err != nil || it.closed {
		return false
	}
	if it.rows == nil && !it.open(ctx) {
		return false
	}
	next := it.next(ctx)
	if next {
		return true
	}
	if id := it.prevID; id != nil {
		it.qu = it.qu.clone()
		it.qu.putSelector(idField, map[string]interface{}{"$gt": id})
		if it.open(ctx) {
			next = it.next(ctx)
		}
	}
	if next {
		return true
	}
	it.closed = true // auto-closed at end of iteration by API
	return false
}

func (it *Iterator) Err() error {
	return it.err
}

func (it *Iterator) Close() error {
	it.closed = true
	if it.rows == nil {
		return it.err
	}
	if err := it.rows.Close(); err != nil && it.err == nil {
		it.err = err
	}
	it.rows = nil
	return it.err
}

func (it *Iterator) Key() nosql.Key {
	if it.err != nil || it.closed {
		return nil
	} else if len(it.doc) == 0 {
		return nil
	}
	var k nosql.Key
	for _, f := range it.db.colls[it.col].primary.Fields {
		k = append(k, string(fromOuchValue(it.doc[f]).(nosql.String)))
	}
	return k
}

func (it *Iterator) Doc() nosql.Document {
	if it.err != nil || it.closed {
		return nil
	}
	return fromOuchDoc(it.doc)
}

func (it *Iterator) scanDoc() {
	if it.doc == nil && it.err == nil && !it.closed {
		it.doc = map[string]interface{}{}
		it.err = it.rows.ScanDoc(&it.doc)
		it.prevID = it.doc[idField]
	}
}

type Delete struct {
	db   *DB
	col  string
	q    *Query
	keys []string
}

func (d *Delete) WithFields(filters ...nosql.FieldFilter) nosql.Delete {
	d.q.WithFields(filters...)
	return d
}
func (d *Delete) Keys(keys ...nosql.Key) nosql.Delete {
	for _, k := range keys {
		d.keys = append(d.keys, compKey(k))
	}
	return d
}
func (d *Delete) Do(ctx context.Context) error {

	deleteSet := make(map[string]string) // [_id]_rev

	switch len(d.keys) {
	case 0:
	// no keys to test against
	case 1:
		if len(d.q.pathFilters) == 0 {
			// this special case is optimised not to use the query/iterate route at all,
			// but rather to fetch the _id and _rev directly from the given key.
			_, id, rev, err := d.db.findByOuchKey(ctx, d.keys[0])
			if err != nil {
				return err
			}
			deleteSet[id] = rev
		} else {
			d.q.qu.putSelector(idField, map[string]interface{}{"$eq": d.keys[0]})
		}

	default:
		d.q.qu.putSelector(idField, map[string]interface{}{"$in": d.keys})
	}

	// NOTE even when using idField in a Mango query, it still has to base its query on an index

	if len(deleteSet) == 0 { // did not hit the special case, so must do a mango query

		// only pull back the _id & _rev fields in the query
		d.q.qu["fields"] = []interface{}{idField, revField}

		it := d.q.Iterate().(*Iterator)
		for it.Next(ctx) {
			id := it.doc[idField].(string)
			rev := it.doc[revField].(string)
			deleteSet[id] = rev
		}
		if err := it.Err(); err != nil {
			it.Close()
			return err
		}
		if err := it.Close(); err != nil {
			return err
		}
	}

	for id, rev := range deleteSet {
		_, err := d.db.db.Delete(ctx, id, rev)
		if err != nil {
			return err
		}
	}

	return nil
}

type Update struct {
	db     *DB
	col    string
	key    nosql.Key
	update nosql.Document
	upsert bool
	inc    map[string]int // increment the named numeric field by the int
}

func (u *Update) Inc(field string, dn int) nosql.Update {
	if u.inc == nil {
		u.inc = make(map[string]int)
	}
	u.inc[field] += dn
	return u
}

func (u *Update) Upsert(d nosql.Document) nosql.Update {
	u.upsert = true
	for k, v := range d {
		u.update[k] = v
	}
	return u
}
func (u *Update) Do(ctx context.Context) error {
	orig, id, rev, err := u.db.findByKey(ctx, u.col, u.key)
	if err == nosql.ErrNotFound {
		if !u.upsert {
			return err
		}
		var idKey nosql.Key
		orig = u.update
		idKey, rev, err = u.db.insert(ctx, u.col, u.key, orig)
		if err != nil {
			return err
		}
		id = compKey(idKey)
	} else {
		if err != nil {
			return err
		}
		for k, v := range u.update { // alter any changed fields
			orig[k] = v
		}
	}

	for k, v := range u.inc { // increment numerical values
		val, exists := orig[k]
		if exists {
			switch x := val.(type) {
			case nosql.Int:
				val = nosql.Int(int64(x) + int64(v))
			case nosql.Float:
				val = nosql.Float(float64(x) + float64(v))
			default:
				return errors.New("field '" + k + "' is not a number")
			}
		} else {
			val = nosql.Int(v)
		}
		orig[k] = val
	}

	_, err = u.db.db.Put(ctx, compKey(u.key), toOuchDoc(u.col, id, rev, orig))
	return err
}
