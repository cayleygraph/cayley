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
	"github.com/cayleygraph/cayley/writer"
	"github.com/cayleygraph/quad"
	hkv "github.com/hidal-go/hidalgo/kv"
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

func key(b string, k []byte) hkv.Key {
	return hkv.Key{[]byte(b), k}
}

func be(v ...uint64) []byte {
	b := make([]byte, 8*len(v))
	for i, vi := range v {
		binary.BigEndian.PutUint64(b[i*8:], vi)
	}
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

	kIndexes = []byte("indexes")
)

type Ops []kvOp

func (s Ops) Len() int {
	return len(s)
}

func (s Ops) Less(i, j int) bool {
	a, b := s[i], s[j]
	return a.key.Compare(b.key) < 0
}

func (s Ops) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s Ops) String() string {
	buf := bytes.NewBuffer(nil)
	for _, op := range s {
		se := ""
		if op.err != nil {
			se = " (" + op.err.Error() + ")"
		}
		fmt.Fprintf(buf, "%v: %q = %x%s\n", op.typ, op.key, op.val, se)
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
				if bytes.Equal(d.key[0], vAuto) {
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
		{opGet, key(bMeta, kVers), nil, hkv.ErrNotFound},
		{opPut, key(bMeta, []byte{}), nil, nil},
		{opPut, key(bLog, []byte{}), nil, nil},
		{opPut, key("sp", []byte{}), nil, nil},
		{opPut, key("ops", []byte{}), nil, nil},
		{opPut, key(bMeta, kVers), vVers, nil},
		{opPut, key(bMeta, kIndexes), []byte(`[{"dirs":"AQI=","unique":false},{"dirs":"AwIB","unique":false}]`), nil},
	})

	qs, err := kv.New(hook, nil)
	require.NoError(t, err)
	defer qs.Close()

	expect(Ops{
		{opGet, key(bMeta, kVers), vVers, nil},
		{opGet, key(bMeta, kIndexes), []byte(`[{"dirs":"AQI=","unique":false},{"dirs":"AwIB","unique":false}]`), nil},
		{opGet, key(bMeta, []byte("size")), nil, hkv.ErrNotFound},
	})

	qw, err := writer.NewSingle(qs, graph.IgnoreOpts{})
	require.NoError(t, err)

	err = qw.AddQuad(quad.MakeIRI("a", "b", "c", ""))
	require.NoError(t, err)

	expect(Ops{
		{opGet, key(bMeta, []byte("horizon")), nil, hkv.ErrNotFound},
		{opPut, key(bMeta, []byte("horizon")), le(3), nil},

		{opPut, key(irib("a"), irih("a")), vAuto, nil},
		{opPut, key(bLog, be(1)), vAuto, nil},
		{opPut, key(irib("b"), irih("b")), vAuto, nil},
		{opPut, key(bLog, be(2)), vAuto, nil},
		{opPut, key(irib("c"), irih("c")), vAuto, nil},
		{opPut, key(bLog, be(3)), vAuto, nil},

		{opPut, key(iric("a"), irih("a")), hex("01"), nil},
		{opPut, key(iric("b"), irih("b")), hex("01"), nil},
		{opPut, key(iric("c"), irih("c")), hex("01"), nil},
		{opGet, key(bMeta, []byte("horizon")), le(3), nil},
		{opPut, key(bMeta, []byte("horizon")), le(4), nil},
		{opPut, key(bLog, be(4)), vAuto, nil},
		{opGet, key(bMeta, []byte("size")), nil, hkv.ErrNotFound},
		{opPut, key(bMeta, []byte("size")), le(1), nil},
		{opPut, key("ops", be(3, 2, 1)), hex("04"), nil},
		{opPut, key("sp", be(1, 2)), hex("04"), nil},
	})

	err = qw.AddQuad(quad.MakeIRI("a", "b", "e", ""))
	require.NoError(t, err)

	expect(Ops{
		// served from IRI cache
		//{opGet, irib("a"), irih("a"), vAuto, nil},
		//{opGet, irib("b"), irih("b"), vAuto, nil},
		{opGet, key(bMeta, []byte("horizon")), le(4), nil},
		{opPut, key(bMeta, []byte("horizon")), le(5), nil},

		{opPut, key(irib("e"), irih("e")), vAuto, nil},
		{opPut, key(bLog, be(5)), vAuto, nil},

		{opGet, key(iric("a"), irih("a")), hex("01"), nil},
		{opGet, key(iric("b"), irih("b")), hex("01"), nil},
		{opPut, key(iric("a"), irih("a")), hex("02"), nil},
		{opPut, key(iric("b"), irih("b")), hex("02"), nil},
		{opPut, key(iric("e"), irih("e")), hex("01"), nil},
		{opGet, key(bMeta, []byte("horizon")), le(5), nil},
		{opPut, key(bMeta, []byte("horizon")), le(6), nil},
		{opPut, key(bLog, be(6)), vAuto, nil},
		{opGet, key(bMeta, []byte("size")), le(1), nil},
		{opPut, key(bMeta, []byte("size")), le(2), nil},
		{opPut, key("ops", be(5, 2, 1)), hex("06"), nil},
		{opGet, key("sp", be(1, 2)), hex("04"), nil},
		{opPut, key("sp", be(1, 2)), hex("0406"), nil},
	})

	err = qw.RemoveQuad(quad.MakeIRI("a", "b", "c", ""))
	expect(Ops{
		{opGet, key("sp", be(1, 2)), hex("0406"), nil},
		{opGet, key("ops", be(3, 2, 1)), hex("04"), nil},
		{opGet, key(bLog, be(4)), vAuto, nil},
		{opPut, key(bLog, be(4)), vAuto, nil},
		{opGet, key(bMeta, []byte("size")), le(2), nil},
		{opPut, key(bMeta, []byte("size")), le(1), nil},
		{opGet, key(iric("a"), irih("a")), hex("02"), nil},
		{opGet, key(iric("b"), irih("b")), hex("02"), nil},
		{opGet, key(iric("c"), irih("c")), hex("01"), nil},
		{opPut, key(iric("a"), irih("a")), hex("01"), nil},
		{opPut, key(iric("b"), irih("b")), hex("01"), nil},
		{opDel, key(iric("c"), irih("c")), nil, nil},
		{opDel, key(irib("c"), irih("c")), nil, nil},
		{opDel, key(bLog, be(3)), nil, nil},
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
			li, typ, b = i, op.typ, string(op.key[0])
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
	typ int
	key hkv.Key
	val hkv.Value
	err error
}

var _ hkv.KV = (*kvHook)(nil)

type kvHook struct {
	db hkv.KV

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

func (h *kvHook) Close() error {
	return h.db.Close()
}

func (h *kvHook) Tx(rw bool) (hkv.Tx, error) {
	tx, err := h.db.Tx(rw)
	if err != nil {
		return nil, err
	}
	return txHook{h: h, tx: tx}, nil
}

type txHook struct {
	h  *kvHook
	tx hkv.Tx
}

func (h txHook) Commit(ctx context.Context) error {
	return h.tx.Commit(ctx)
}

func (h txHook) Close() error {
	return h.tx.Close()
}

func (h txHook) GetBatch(ctx context.Context, keys []hkv.Key) ([]hkv.Value, error) {
	vals, err := h.tx.GetBatch(ctx, keys)
	if err != nil {
		return nil, err
	}
	for i, k := range keys {
		h.h.addOp(kvOp{
			key: k.Clone(),
			val: vals[i].Clone(),
		})
	}
	return vals, nil
}

func (h txHook) Get(ctx context.Context, k hkv.Key) (hkv.Value, error) {
	v, err := h.tx.Get(ctx, k)
	h.h.addOp(kvOp{
		key: k.Clone(),
		val: v.Clone(),
		err: err,
	})
	return v, err
}

func (h txHook) Put(k hkv.Key, v hkv.Value) error {
	err := h.tx.Put(k, v)
	h.h.addOp(kvOp{
		typ: opPut,
		key: k.Clone(),
		val: v.Clone(),
		err: err,
	})
	return err
}

func (h txHook) Del(k hkv.Key) error {
	err := h.tx.Del(k)
	h.h.addOp(kvOp{
		typ: opDel,
		key: k.Clone(),
		err: err,
	})
	return err
}

func (h txHook) Scan(pref hkv.Key) hkv.Iterator {
	return h.tx.Scan(pref)
}
