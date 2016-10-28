package etcd

import (
	"bytes"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/quad"
	"github.com/coreos/etcd/clientv3"
)

var etcdItType graph.Type

func init() {
	etcdItType = graph.RegisterIterator("etcd")
}

type QuadIterator struct {
	uid    uint64
	tags   graph.Tagger
	dir    quad.Direction
	index  [4]quad.Direction
	hash   ValueHash
	qs     *QuadStore
	kvs    *Iterator
	result graph.Value
}

func NewQuadIterator(qs *QuadStore, dir quad.Direction, val graph.Value) *QuadIterator {
	it := &QuadIterator{
		uid: iterator.NextUID(),
		qs:  qs, dir: dir, hash: val.(ValueHash),
	}
	switch dir {
	case quad.Subject:
		it.index = indSPO
	case quad.Predicate:
		it.index = indPOS
	case quad.Object:
		it.index = indOSP
	case quad.Label:
		it.index = indCPS
	}
	p, n := qs.prefQuad(it.index, quad.HashSize)
	copy(p[n:], it.hash[:])
	pref := string(p)
	it.kvs = NewIterator(qs.etc, pref, 0,
		clientv3.WithKeysOnly(),
		clientv3.WithLimit(iteratorLimit),
	)
	return it
}

func (it *QuadIterator) UID() uint64 {
	return it.uid
}

func (it *QuadIterator) Reset() {
	it.kvs.Reset()
}

func (it *QuadIterator) Tagger() *graph.Tagger {
	return &it.tags
}

func (it *QuadIterator) TagResults(dst map[string]graph.Value) {
	for _, tag := range it.tags.Tags() {
		dst[tag] = it.Result()
	}

	for tag, value := range it.tags.Fixed() {
		dst[tag] = value
	}
}

func (it *QuadIterator) Err() error {
	return it.kvs.Err()
}

func (it *QuadIterator) Result() graph.Value {
	return it.result
}

func (it *QuadIterator) NextPath() bool {
	return false
}
func (it *QuadIterator) SubIterators() []graph.Iterator {
	return nil
}

func (it *QuadIterator) Close() error {
	return nil
}

func (it *QuadIterator) Clone() graph.Iterator {
	out := NewQuadIterator(it.qs, it.dir, it.hash)
	out.tags.CopyFrom(it)
	return out
}

func (it *QuadIterator) Next() bool {
	if ok := it.kvs.Next(); !ok {
		return ok
	}
	kv := it.kvs.Result()
	var h QuadHash
	p, n := it.qs.prefQuad(it.index, 0)
	p = p[:n]

	key := bytes.TrimPrefix(kv.Key, p)
	const hs = quad.HashSize
	switch it.dir {
	case quad.Subject: // SPOL
		copy(h[:], key[:])
	case quad.Predicate: // POSL
		copy(h[0*hs:1*hs], key[hs*2:]) // S
		copy(h[1*hs:3*hs], key[:hs*2]) // PO
		copy(h[3*hs:4*hs], key[hs*3:]) // L
	case quad.Object: // OSPL
		copy(h[0*hs:2*hs], key[hs*1:]) // SP
		copy(h[2*hs:3*hs], key[:hs*1]) // O
		copy(h[3*hs:4*hs], key[hs*3:]) // L
	case quad.Label: // LPSO
		copy(h[0*hs:1*hs], key[hs*2:]) // S
		copy(h[1*hs:2*hs], key[hs*1:]) // P
		copy(h[2*hs:3*hs], key[hs*3:]) // O
		copy(h[3*hs:4*hs], key[:hs*1]) // L
	}
	it.result = h
	return true
}

func (it *QuadIterator) Contains(v graph.Value) bool {
	h := v.(QuadHash)
	if h.Get(it.dir) != it.hash {
		return false
	}
	// TODO: use it.index instead?
	key := it.qs.keyQuadHash(v.(QuadHash), indSPO)
	return it.kvs.Contains(key)
}

func (it *QuadIterator) Size() (int64, bool) {
	return it.kvs.Size()
}

func (it *QuadIterator) Describe() graph.Description {
	size, _ := it.Size()
	return graph.Description{
		UID:       it.UID(),
		Type:      it.Type(),
		Tags:      it.tags.Tags(),
		Size:      size,
		Direction: it.dir,
	}
}

func (it *QuadIterator) Type() graph.Type { return etcdItType }
func (it *QuadIterator) Sorted() bool     { return true }

func (it *QuadIterator) Optimize() (graph.Iterator, bool) {
	return it, false
}

func (it *QuadIterator) Stats() graph.IteratorStats {
	s, _ := it.Size()
	return graph.IteratorStats{
		ContainsCost: 1,
		NextCost:     2,
		Size:         s,
	}
}
