package kv_test

import (
	"bytes"
	"context"
	"encoding/binary"
	henc "encoding/hex"
	"fmt"
	"sort"
	"sync"
	"testing"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/kv"
	"github.com/cayleygraph/cayley/graph/kv/btree"
	"github.com/cayleygraph/cayley/quad"
	"github.com/cayleygraph/cayley/writer"
	"github.com/stretchr/testify/require"
)

func hex(s string) []byte {
	b, err := henc.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return b
}

func irih(s string) []byte {
	h := graph.HashOf(quad.IRI(s))
	return h[:]
}

func irib(s string) string {
	h := graph.HashOf(quad.IRI(s))
	return string([]byte{'v', h[0], h[1]})
}

func iric(s string) string {
	h := graph.HashOf(quad.IRI(s))
	return string([]byte{'n', h[0], h[1]})
}

func be(v uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(v))
	return b[:]
}
func le(v uint64) []byte {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(v))
	return b[:]
}

const (
	bMeta = "meta"
	bLog  = "log"
)

var (
	kVers = []byte("version")
	vVers = le(2)

	vAuto = []byte("auto")
)

type Ops []kvOp

func (s Ops) Len() int {
	return len(s)
}

func (s Ops) Less(i, j int) bool {
	a, b := s[i], s[j]
	if a.bucket == b.bucket {
		return bytes.Compare(a.key, b.key) < 0
	}
	return a.bucket < b.bucket
}

func (s Ops) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s Ops) String() string {
	buf := bytes.NewBuffer(nil)
	for _, op := range s {
		fmt.Fprintf(buf, "%v: %q %q = %x\n", op.typ, op.bucket, op.key, op.val)
	}
	return buf.String()
}

func TestApplyDeltas(t *testing.T) {
	kdb := btree.New()

	hook := &kvHook{db: kdb}
	expect := func(exp Ops) {
		got := hook.log()
		if len(exp) == len(got) {
			if false {
				sortByOp(exp, got)
			}
			// TODO: make node insert predictable
			for i, d := range exp {
				if bytes.Equal(d.key, vAuto) {
					exp[i].key = got[i].key
				}
				if bytes.Equal(d.val, vAuto) {
					exp[i].val = got[i].val
				}
			}
		}
		require.Equal(t, exp, got, "%d\n%v\nvs\n\n%d\n%v", len(exp), exp, len(got), got)
	}

	err := kv.Init(hook, nil)
	require.NoError(t, err)

	expect(Ops{
		{opPut, bMeta, kVers, vVers, nil},
	})

	qs, err := kv.New(hook, nil)
	require.NoError(t, err)
	defer qs.Close()

	expect(Ops{
		{opGet, bMeta, kVers, vVers, nil},
	})

	qw, err := writer.NewSingle(qs, graph.IgnoreOpts{})
	require.NoError(t, err)

	err = qw.AddQuad(quad.MakeIRI("a", "b", "c", ""))
	require.NoError(t, err)

	expect(Ops{
		{opGet, irib("a"), irih("a"), nil, nil},
		{opGet, irib("b"), irih("b"), nil, nil},
		{opGet, irib("c"), irih("c"), nil, nil},
		{opGet, bMeta, []byte("horizon"), nil, nil},
		{opPut, bMeta, []byte("horizon"), le(3), nil},

		{opPut, irib("a"), irih("a"), vAuto, nil},
		{opPut, bLog, be(1), vAuto, nil},
		{opPut, irib("b"), irih("b"), vAuto, nil},
		{opPut, bLog, be(2), vAuto, nil},
		{opPut, irib("c"), irih("c"), vAuto, nil},
		{opPut, bLog, be(3), vAuto, nil},

		{opGet, iric("a"), irih("a"), nil, nil},
		{opGet, iric("b"), irih("b"), nil, nil},
		{opGet, iric("c"), irih("c"), nil, nil},
		{opPut, iric("a"), irih("a"), hex("01"), nil},
		{opPut, iric("b"), irih("b"), hex("01"), nil},
		{opPut, iric("c"), irih("c"), hex("01"), nil},
		{opGet, bMeta, []byte("horizon"), le(3), nil},
		{opPut, bMeta, []byte("horizon"), le(4), nil},
		{opPut, bLog, be(4), vAuto, nil},
		{opGet, bMeta, []byte("size"), nil, nil},
		{opPut, bMeta, []byte("size"), le(1), nil},
		{opGet, "o", be(3), nil, nil},
		{opPut, "o", be(3), hex("04"), nil},
		{opGet, "s", be(1), nil, nil},
		{opPut, "s", be(1), hex("04"), nil},
	})

	err = qw.AddQuad(quad.MakeIRI("a", "b", "e", ""))
	require.NoError(t, err)

	expect(Ops{
		// served from IRI cache
		//{opGet, irib("a"), irih("a"), vAuto, nil},
		//{opGet, irib("b"), irih("b"), vAuto, nil},
		{opGet, irib("e"), irih("e"), nil, nil},
		{opGet, bMeta, []byte("horizon"), le(4), nil},
		{opPut, bMeta, []byte("horizon"), le(5), nil},

		{opPut, irib("e"), irih("e"), vAuto, nil},
		{opPut, bLog, be(5), vAuto, nil},

		{opGet, iric("a"), irih("a"), hex("01"), nil},
		{opGet, iric("b"), irih("b"), hex("01"), nil},
		{opGet, iric("e"), irih("e"), nil, nil},
		{opPut, iric("a"), irih("a"), hex("02"), nil},
		{opPut, iric("b"), irih("b"), hex("02"), nil},
		{opPut, iric("e"), irih("e"), hex("01"), nil},
		{opGet, bMeta, []byte("horizon"), le(5), nil},
		{opPut, bMeta, []byte("horizon"), le(6), nil},
		{opPut, bLog, be(6), vAuto, nil},
		{opGet, bMeta, []byte("size"), le(1), nil},
		{opPut, bMeta, []byte("size"), le(2), nil},
		{opGet, "o", be(5), nil, nil},
		{opPut, "o", be(5), hex("06"), nil},
		{opGet, "s", be(1), hex("04"), nil},
		{opPut, "s", be(1), hex("0406"), nil},
	})

	err = qw.RemoveQuad(quad.MakeIRI("a", "b", "c", ""))
	expect(Ops{
		{opGet, "s", be(1), hex("0406"), nil},
		{opGet, "o", be(3), hex("04"), nil},
		{opGet, bLog, be(4), vAuto, nil},
		{opPut, bLog, be(4), vAuto, nil},
		{opGet, bMeta, []byte("size"), le(2), nil},
		{opPut, bMeta, []byte("size"), le(1), nil},
		{opGet, iric("a"), irih("a"), hex("02"), nil},
		{opGet, iric("b"), irih("b"), hex("02"), nil},
		{opGet, iric("c"), irih("c"), hex("01"), nil},
		{opPut, iric("a"), irih("a"), hex("01"), nil},
		{opPut, iric("b"), irih("b"), hex("01"), nil},
		{opDel, iric("c"), irih("c"), nil, nil},
		{opDel, irib("c"), irih("c"), nil, nil},
		{opDel, bLog, be(3), nil, nil},
	})
	require.NoError(t, err)
}

func clone(b []byte) []byte {
	if b == nil {
		return nil
	}
	return append([]byte{}, b...)
}

func sortByOp(exp, got Ops) {
	// sort ops of one type
	li := -1
	typ, b := -1, ""
	check := func(i int) {
		if li < 0 || i-li <= 0 {
			return
		}
		sort.Sort(exp[li:i])
		sort.Sort(got[li:i])
		//sort.Sort(bothOps{a: exp[li:i], b: got[li:i]})
		li, typ, b = -1, -1, ""
	}
	for i, op := range exp {
		if op.typ != typ {
			check(i)
		}
		if li < 0 {
			li, typ, b = i, op.typ, op.bucket
		}
	}
	_ = b
	check(len(exp))
}

const (
	opGet = iota
	opPut
	opDel
)

type kvOp struct {
	typ    int
	bucket string
	key    []byte
	val    []byte
	err    error
}

type kvHook struct {
	db kv.BucketKV

	mu  sync.Mutex
	ops Ops
}

func (h *kvHook) log() Ops {
	h.mu.Lock()
	ops := h.ops
	h.ops = nil
	h.mu.Unlock()
	return ops
}

func (h *kvHook) addOp(op kvOp) {
	h.mu.Lock()
	h.ops = append(h.ops, op)
	h.mu.Unlock()
}

func (h *kvHook) Type() string {
	return h.db.Type()
}

func (h *kvHook) Close() error {
	return h.db.Close()
}

func (h *kvHook) Tx(update bool) (kv.BucketTx, error) {
	tx, err := h.db.Tx(update)
	if err != nil {
		return nil, err
	}
	return txHook{h: h, tx: tx}, nil
}

type txHook struct {
	h  *kvHook
	tx kv.BucketTx
}

func (h txHook) Commit(ctx context.Context) error {
	return h.tx.Commit(ctx)
}

func (h txHook) Rollback() error {
	return h.tx.Rollback()
}

func (h txHook) Bucket(name []byte) kv.Bucket {
	return bucketHook{h: h.h, name: string(name), b: h.tx.Bucket(name)}
}

func (h txHook) Get(ctx context.Context, keys []kv.BucketKey) ([][]byte, error) {
	vals, err := h.tx.Get(ctx, keys)
	if err != nil {
		return nil, err
	}
	for i, k := range keys {
		h.h.addOp(kvOp{
			bucket: string(k.Bucket),
			key:    clone(k.Key),
			val:    clone(vals[i]),
		})
	}
	return vals, nil
}

type bucketHook struct {
	h    *kvHook
	name string
	b    kv.Bucket
}

func (h bucketHook) Get(ctx context.Context, keys [][]byte) ([][]byte, error) {
	vals, err := h.b.Get(ctx, keys)
	if err != nil {
		return nil, err
	}
	for i, k := range keys {
		h.h.addOp(kvOp{
			bucket: h.name,
			key:    clone(k),
			val:    clone(vals[i]),
		})
	}
	return vals, nil
}

func (h bucketHook) Put(k, v []byte) error {
	err := h.b.Put(k, v)
	h.h.addOp(kvOp{
		typ:    opPut,
		bucket: h.name,
		key:    clone(k),
		val:    clone(v),
		err:    err,
	})
	return err
}

func (h bucketHook) Del(k []byte) error {
	err := h.b.Del(k)
	h.h.addOp(kvOp{
		typ:    opDel,
		bucket: h.name,
		key:    clone(k),
		err:    err,
	})
	return err
}

func (h bucketHook) Scan(pref []byte) kv.KVIterator {
	return h.b.Scan(pref)
}
