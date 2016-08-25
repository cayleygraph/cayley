package etcd

import (
	"bytes"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/coreos/etcd/clientv3"
)

const iteratorLimit = 1000

var _ graph.Iterator = (*AllIterator)(nil)

type AllIterator struct {
	uid    uint64
	nodes  bool
	tags   graph.Tagger
	qs     *QuadStore
	kvs    *Iterator
	result graph.Value
}

func NewAllIterator(qs *QuadStore, nodes bool, rev int64) *AllIterator {
	it := &AllIterator{
		uid:   iterator.NextUID(),
		nodes: nodes,
		qs:    qs,
	}
	var pref string
	if nodes {
		pref = qs.prefValue()
	} else {
		p, n := qs.prefQuad(indSPO, 0)
		pref = string(p[:n])
	}
	it.kvs = NewIterator(qs.etc, pref, rev,
		clientv3.WithKeysOnly(),
		clientv3.WithLimit(iteratorLimit),
	)
	return it
}

func (it *AllIterator) UID() uint64 {
	return it.uid
}

func (it *AllIterator) Reset() {
	it.kvs.Reset()
}

func (it *AllIterator) Tagger() *graph.Tagger {
	return &it.tags
}

func (it *AllIterator) TagResults(dst map[string]graph.Value) {
	for _, tag := range it.tags.Tags() {
		dst[tag] = it.Result()
	}

	for tag, value := range it.tags.Fixed() {
		dst[tag] = value
	}
}

func (it *AllIterator) Err() error {
	return it.kvs.Err()
}

func (it *AllIterator) Result() graph.Value {
	return it.result
}

func (it *AllIterator) NextPath() bool {
	return false
}
func (it *AllIterator) SubIterators() []graph.Iterator {
	return nil
}

func (it *AllIterator) Close() error {
	return nil
}

func (it *AllIterator) Clone() graph.Iterator {
	out := NewAllIterator(it.qs, it.nodes, it.kvs.rev)
	out.tags.CopyFrom(it)
	return out
}

func (it *AllIterator) Next() bool {
	if ok := it.kvs.Next(); !ok {
		return ok
	}
	kv := it.kvs.Result()
	if it.nodes {
		var h ValueHash
		copy(h[:], bytes.TrimPrefix(kv.Key, []byte(it.qs.prefValue())))
		it.result = h
	} else {
		var h QuadHash
		p, n := it.qs.prefQuad(indSPO, 0)
		p = p[:n]
		copy(h[:], bytes.TrimPrefix(kv.Key, p))
		it.result = h
	}
	return true
}

func (it *AllIterator) Contains(v graph.Value) bool {
	var key string
	if it.nodes {
		key = it.qs.keyValue(v.(ValueHash))
	} else {
		key = it.qs.keyQuadHash(v.(QuadHash), indSPO)
	}
	ok := it.kvs.Contains(key)
	if ok {
		it.result = v
	}
	return ok
}

func (it *AllIterator) Size() (int64, bool) {
	return it.kvs.Size()
}

func (it *AllIterator) Describe() graph.Description {
	size, _ := it.Size()
	return graph.Description{
		UID:  it.UID(),
		Type: it.Type(),
		Tags: it.tags.Tags(),
		Size: size,
	}
}

func (it *AllIterator) Type() graph.Type { return graph.All }
func (it *AllIterator) Sorted() bool     { return true }

func (it *AllIterator) Optimize() (graph.Iterator, bool) {
	return it, false
}

func (it *AllIterator) Stats() graph.IteratorStats {
	s, _ := it.Size()
	return graph.IteratorStats{
		ContainsCost: 1,
		NextCost:     2,
		Size:         s,
	}
}
