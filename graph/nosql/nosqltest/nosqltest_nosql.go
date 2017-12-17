package nosqltest

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/cayleygraph/cayley/graph/nosql"
	"github.com/stretchr/testify/require"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type keyType struct {
	Name   string
	Fields []string
	Gen    func() nosql.Key
}

func (kt keyType) SetKey(d nosql.Document, k nosql.Key) {
	for i, f := range kt.Fields {
		d[f] = nosql.String(k[i])
	}
}

var (
	mu      sync.Mutex
	lastKey int
)

func next() int {
	mu.Lock()
	lastKey++
	v := lastKey
	mu.Unlock()
	return v
}

var keyTypes = []keyType{
	{
		Name:   "single",
		Fields: []string{"id"},
		Gen: func() nosql.Key {
			v := next()
			return nosql.Key{fmt.Sprintf("k%d", v)}
		},
	},
	{
		Name:   "composite2",
		Fields: []string{"id1", "id2"},
		Gen: func() nosql.Key {
			v := next()
			return nosql.Key{
				fmt.Sprintf("i%d", v),
				fmt.Sprintf("j%d", v),
			}
		},
	},
	{
		Name:   "composite3",
		Fields: []string{"id1", "id2", "id3"},
		Gen: func() nosql.Key {
			v := next()
			return nosql.Key{
				fmt.Sprintf("i%d", v),
				fmt.Sprintf("j%d", v),
				fmt.Sprintf("k%d", v),
			}
		},
	},
}

var testsNoSQLKey = []struct {
	name string
	t    func(t *testing.T, c tableConf)
}{
	{name: "ensure", t: testEnsure},
	{name: "insert", t: testInsert},
	{name: "delete by key", t: testDeleteByKey},
	{name: "update", t: testUpdate},
	{name: "delete query", t: testDeleteQuery},
}

type tableConf struct {
	ctx  context.Context
	db   nosql.Database
	col  string
	conf *Config
	kt   keyType
}

func (c tableConf) ensurePK(t testing.TB, secondary ...nosql.Index) {
	err := c.db.EnsureIndex(c.ctx, c.col, nosql.Index{
		Fields: c.kt.Fields,
		Type:   nosql.StringExact,
	}, secondary)
	require.NoError(t, err)
}
func (c tableConf) FindByKey(key nosql.Key) (nosql.Document, error) {
	return c.db.FindByKey(c.ctx, c.col, key)
}
func (c tableConf) Insert(key nosql.Key, d nosql.Document) (nosql.Key, error) {
	return c.db.Insert(c.ctx, c.col, key, d)
}
func (c tableConf) fixDoc(k nosql.Key, d nosql.Document) {
	c.kt.SetKey(d, k)
	fixDoc(c.conf, d)
}
func (c tableConf) expectAll(t testing.TB, docs []nosql.Document) {
	iterateExpect(t, c.kt, c.db.Query(c.col), docs)
}
func (c tableConf) insertDocs(t testing.TB, n int, fnc func(i int) nosql.Document) ([]nosql.Key, []nosql.Document) {
	var (
		docs []nosql.Document
	)
	w := nosql.BatchInsert(c.db, c.col)
	defer w.Close()
	for i := 0; i < n; i++ {
		doc := fnc(i)
		var key nosql.Key
		if len(c.kt.Fields) != 1 || i%2 == 0 {
			key = c.kt.Gen()
		}
		err := w.WriteDoc(c.ctx, key, doc)
		require.NoError(t, err)
		docs = append(docs, doc)
	}
	err := w.Flush(c.ctx)
	require.NoError(t, err)

	keys := w.Keys()
	for i := range docs {
		c.fixDoc(keys[i], docs[i])
	}

	c.expectAll(t, docs)
	return keys, docs
}
func (c tableConf) Delete() nosql.Delete {
	return c.db.Delete(c.col)
}
func (c tableConf) DeleteKeys(keys ...nosql.Key) error {
	return c.db.Delete(c.col).Keys(keys...).Do(c.ctx)
}
func (c tableConf) Update(key nosql.Key) nosql.Update {
	return c.db.Update(c.col, key)
}
func (c tableConf) Query() nosql.Query {
	return c.db.Query(c.col)
}

func TestNoSQL(t *testing.T, gen DatabaseFunc, conf *Config) {
	var (
		db     nosql.Database
		closer func()
	)
	if !conf.Recreate {
		db, _, closer = gen(t)
		defer closer()
	}

	for _, kt := range keyTypes {
		t.Run(kt.Name, func(t *testing.T) {
			for _, c := range testsNoSQLKey {
				t.Run(c.name, func(t *testing.T) {
					col := fmt.Sprintf("col_%x", rand.Int())
					db := db
					if conf.Recreate {
						var closer func()
						db, _, closer = gen(t)
						defer closer()
					}
					c.t(t, tableConf{
						ctx: context.TODO(),
						db:  db, conf: conf,
						col: col, kt: kt,
					})
				})
			}
		})
	}
}

func newDoc(d nosql.Document) nosql.Document {
	d["val_key"] = nosql.Strings{"a"}
	d["val_key2"] = nosql.Strings{"a", "b"}
	d["val_str"] = nosql.String("bar")
	d["val_int"] = nosql.Int(42)
	d["val_int0"] = nosql.Int(0)
	d["val_float"] = nosql.Float(42.3)
	d["val_floati"] = nosql.Float(42)
	d["val_float0"] = nosql.Float(0)
	d["val_bool"] = nosql.Bool(true)
	d["val_boolf"] = nosql.Bool(false)
	d["val_sub"] = nosql.Document{"v": nosql.String("c")}
	// TODO: time type
	return d
}

func fixDoc(conf *Config, d nosql.Document) {
	if conf.FloatToInt {
		for k, v := range d {
			if f, ok := v.(nosql.Float); ok && nosql.Float(nosql.Int(f)) == f {
				d[k] = nosql.Int(f)
			}
		}
	}
}

type byFields []string

func (s byFields) Key(d nosql.Document) nosql.Key {
	return nosql.KeyFrom(s, d)
}
func (s byFields) Less(d1, d2 nosql.Document) bool {
	k1, k2 := s.Key(d1), s.Key(d2)
	for i := range k1 {
		s1, s2 := k1[i], k2[i]
		if s1 < s2 {
			return true
		}
	}
	return false
}

type docsAndKeys struct {
	LessFunc func(d1, d2 nosql.Document) bool
	Docs     []nosql.Document
	Keys     []nosql.Key
}

func (s docsAndKeys) Len() int {
	return len(s.Docs)
}

func (s docsAndKeys) Less(i, j int) bool {
	return s.LessFunc(s.Docs[i], s.Docs[j])
}

func (s docsAndKeys) Swap(i, j int) {
	s.Docs[i], s.Docs[j] = s.Docs[j], s.Docs[i]
	s.Keys[i], s.Keys[j] = s.Keys[j], s.Keys[i]
}

func iterateExpect(t testing.TB, kt keyType, qu nosql.Query, exp []nosql.Document) {
	ctx := context.TODO()

	it := qu.Iterate()
	defer it.Close()
	var (
		got  = make([]nosql.Document, 0, len(exp))
		keys []nosql.Key
	)
	for i := 0; i < len(exp)*2 && it.Next(ctx); i++ {
		keys = append(keys, it.Key())
		got = append(got, it.Doc())
	}
	require.NoError(t, it.Err())

	sorter := byFields(kt.Fields)
	exp = append([]nosql.Document{}, exp...)
	sort.Slice(exp, func(i, j int) bool {
		return sorter.Less(exp[i], exp[j])
	})
	var expKeys []nosql.Key
	for _, d := range exp {
		expKeys = append(expKeys, sorter.Key(d))
	}

	sort.Sort(docsAndKeys{
		LessFunc: sorter.Less,
		Docs:     got, Keys: keys,
	})
	require.Equal(t, exp, got)
	require.Equal(t, expKeys, keys)

	n, err := qu.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(len(exp)), int64(n))
}

func testEnsure(t *testing.T, c tableConf) {
	c.ensurePK(t)
	c.ensurePK(t)
	c2 := c
	c2.col += "2"
	c2.ensurePK(t)
}

func testInsert(t *testing.T, c tableConf) {
	c.ensurePK(t)

	_, err := c.FindByKey(c.kt.Gen())
	require.Equal(t, nosql.ErrNotFound, err)

	c.expectAll(t, nil)

	type insert struct {
		Key nosql.Key
		Doc nosql.Document
	}

	k1 := c.kt.Gen()
	doc1 := make(nosql.Document)
	for i, f := range c.kt.Fields {
		doc1[f] = nosql.String(k1[i])
	}
	k2 := c.kt.Gen()
	ins := []insert{
		{ // set key in doc and in insert
			Key: k1,
			Doc: newDoc(doc1),
		},
		{ // set key on insert, but not in doc
			Key: k2,
			Doc: newDoc(nosql.Document{}),
		},
	}
	if len(c.kt.Fields) == 1 {
		ins = append(ins, insert{
			// auto-generate key
			Doc: newDoc(nosql.Document{}),
		})
	}
	for i := range ins {
		in := &ins[i]
		k, err := c.Insert(in.Key, in.Doc)
		require.NoError(t, err)
		if in.Key == nil {
			require.NotNil(t, k)
			in.Key = k
		} else {
			require.Equal(t, in.Key, k)
		}
	}

	var docs []nosql.Document
	for _, in := range ins {
		doc, err := c.FindByKey(in.Key)
		require.NoError(t, err, "find %#v", in.Key)
		c.fixDoc(in.Key, in.Doc)
		require.Equal(t, in.Doc, doc, "got: %#v", doc)
		docs = append(docs, in.Doc)
	}

	_, err = c.FindByKey(c.kt.Gen())
	require.Equal(t, nosql.ErrNotFound, err)

	c.expectAll(t, docs)
}

func testDeleteByKey(t *testing.T, c tableConf) {
	c.ensurePK(t)

	keys, docs := c.insertDocs(t, 10, func(i int) nosql.Document {
		return nosql.Document{
			"data": nosql.Int(i),
		}
	})

	del := keys[:5]
	keys = keys[len(del):]
	docs = docs[len(del):]

	err := c.DeleteKeys(del[0])
	require.NoError(t, err)

	err = c.DeleteKeys(del[1:]...)
	require.NoError(t, err)

	c.expectAll(t, docs)
}

func testUpdate(t *testing.T, c tableConf) {
	ctx := context.TODO()
	c.ensurePK(t)

	docs := []nosql.Document{
		{
			"a": nosql.String("A"),
			"n": nosql.Int(1),
		},
		{
			"a": nosql.String("B"),
			"n": nosql.Int(2),
		},
	}
	var keys []nosql.Key
	for range docs {
		keys = append(keys, c.kt.Gen())
	}

	// test update on both upserted and inserted nodes
	err := c.Update(keys[0]).Upsert(docs[0]).Do(ctx)
	require.NoError(t, err)

	_, err = c.Insert(keys[1], docs[1])
	require.NoError(t, err)

	for _, k := range keys {
		err = c.Update(k).Inc("n", +2).Do(ctx)
		require.NoError(t, err)
	}

	exp := []nosql.Document{
		{
			"a": nosql.String("A"),
			"n": nosql.Int(3),
		},
		{
			"a": nosql.String("B"),
			"n": nosql.Int(4),
		},
	}
	for i, k := range keys {
		c.kt.SetKey(exp[i], k)
	}
	c.expectAll(t, exp)

	// remove one document, so next upsert will create document
	err = c.DeleteKeys(keys[0])
	require.NoError(t, err)

	// get a clean copy of data
	docs = []nosql.Document{
		{
			"a": nosql.String("C"),
		},
		{
			"a": nosql.String("B"),
			// can't specify "n" here
		},
		{
			"a": nosql.String("D"),
			// field should appear after upsert
		},
	}
	keys = append(keys, c.kt.Gen())

	for i, k := range keys {
		err = c.Update(k).Upsert(docs[i]).Inc("n", -1).Do(ctx)
		require.NoError(t, err)
	}

	exp = []nosql.Document{
		{
			"a": nosql.String("C"),
			"n": nosql.Int(-1),
		},
		{
			"a": nosql.String("B"),
			"n": nosql.Int(3),
		},
		{
			"a": nosql.String("D"),
			"n": nosql.Int(-1),
		},
	}
	for i, k := range keys {
		c.kt.SetKey(exp[i], k)
	}
	c.expectAll(t, exp)
}

func testDeleteQuery(t *testing.T, c tableConf) {
	ctx := context.TODO()
	c.ensurePK(t)

	keys, docs := c.insertDocs(t, 10+len(c.kt.Fields), func(i int) nosql.Document {
		return nosql.Document{
			"data": nosql.Int(i),
			"sub": nosql.Document{
				"n": nosql.Int(i),
			},
		}
	})

	lt := 1
	delLt := func(keys []nosql.Key, field ...string) {
		del := c.Delete()
		if keys != nil {
			del = del.Keys(keys...)
		}
		err := del.WithFields(nosql.FieldFilter{
			Path:   field,
			Filter: nosql.LT,
			Value:  nosql.Int(lt),
		}).Do(ctx)
		require.NoError(t, err)
		c.expectAll(t, docs)
	}

	// first, execute a partial delete - try to delete 3 docs, but only 1 doc should be removed
	docs = docs[1:]
	delLt(keys[:3], "data")
	keys = keys[1:]

	if len(c.kt.Fields) > 1 {
		// second, try partial delete by key prefix
		k := keys[0]
		docs = docs[1:]
		lt++
		err := c.Delete().WithFields(nosql.FieldFilter{
			Path:   []string{c.kt.Fields[0]},
			Filter: nosql.Equal,
			Value:  nosql.String(k[0]),
		}).Do(ctx)
		require.NoError(t, err)
		c.expectAll(t, docs)
	}

	const del = 3
	// delete first 3 docs
	docs = docs[del:]
	lt += del

	delLt(nil, "data")

	// delete first 3 more docs (by sub field)
	docs = docs[del:]
	lt += del

	delLt(nil, "sub", "n")

	// delete remaining docs
	err := c.Delete().Do(ctx)
	require.NoError(t, err)

	c.expectAll(t, nil)
}
