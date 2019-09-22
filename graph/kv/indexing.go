// Copyright 2016 The Cayley Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kv

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/log"
	"github.com/cayleygraph/cayley/graph/proto"
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/pquads"

	"github.com/hidal-go/hidalgo/kv"
	"github.com/prometheus/client_golang/prometheus"
	boom "github.com/tylertreat/BoomFilters"
)

var (
	metaBucket = kv.Key{[]byte("meta")}
	logIndex   = kv.Key{[]byte("log")}

	keyMetaIndexes = metaBucket.AppendBytes([]byte("indexes"))

	// List of all buckets in the current version of the database.
	buckets = []kv.Key{
		metaBucket,
		logIndex,
	}

	// legacyQuadIndexes is a set of indexes used in Cayley < 0.7.6
	legacyQuadIndexes = []QuadIndex{
		{Dirs: []quad.Direction{quad.Subject}},
		{Dirs: []quad.Direction{quad.Object}},
	}

	DefaultQuadIndexes = []QuadIndex{
		// First index optimizes forward traversals. Getting all relations for a node should
		// also be reasonably fast (prefix scan).
		{Dirs: []quad.Direction{quad.Subject, quad.Predicate}},

		// Second index helps with reverse traversals as well as full quad lookups.
		// It also prevents issues with super-nodes, since most of those are values
		// with a high in-degree.
		{Dirs: []quad.Direction{quad.Object, quad.Predicate, quad.Subject}},
	}
)

var quadKeyEnc = binary.BigEndian

type QuadIndex struct {
	Dirs   []quad.Direction `json:"dirs"`
	Unique bool             `json:"unique"`
}

func (ind QuadIndex) Key(vals []uint64) kv.Key {
	key := make([]byte, 8*len(vals))
	n := 0
	for i := range vals {
		quadKeyEnc.PutUint64(key[n:], vals[i])
		n += 8
	}
	// TODO(dennwc): split into parts?
	return ind.bucket().AppendBytes(key)
}
func (ind QuadIndex) KeyFor(p *proto.Primitive) kv.Key {
	key := make([]byte, 8*len(ind.Dirs))
	n := 0
	for _, d := range ind.Dirs {
		quadKeyEnc.PutUint64(key[n:], p.GetDirection(d))
		n += 8
	}
	// TODO(dennwc): split into parts?
	return ind.bucket().AppendBytes(key)
}
func (ind QuadIndex) bucket() kv.Key {
	buf := make([]byte, len(ind.Dirs))
	for i, d := range ind.Dirs {
		buf[i] = d.Prefix()
	}
	key := make(kv.Key, 1, 2)
	key[0] = buf
	return key
}

func bucketForVal(i, j byte) kv.Key {
	return kv.Key{[]byte{'v', i, j}}
}

func bucketForValRefs(i, j byte) kv.Key {
	return kv.Key{[]byte{'n', i, j}}
}

func (qs *QuadStore) createBuckets(ctx context.Context, upfront bool) error {
	err := kv.Update(ctx, qs.db, func(tx kv.Tx) error {
		for _, index := range buckets {
			_ = kv.CreateBucket(ctx, tx, index)
		}
		for _, ind := range qs.indexes.all {
			_ = kv.CreateBucket(ctx, tx, ind.bucket())
		}
		return nil
	})
	if err != nil {
		return err
	}
	if !upfront {
		return nil
	}
	for i := 0; i < 256; i++ {
		err := kv.Update(ctx, qs.db, func(tx kv.Tx) error {
			for j := 0; j < 256; j++ {
				_ = kv.CreateBucket(ctx, tx, bucketForVal(byte(i), byte(j)))
				_ = kv.CreateBucket(ctx, tx, bucketForValRefs(byte(i), byte(j)))
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (qs *QuadStore) incSize(ctx context.Context, tx kv.Tx, size int64) error {
	_, err := qs.incMetaInt(ctx, tx, "size", size)
	return err
}

// writeIndexesMeta writes metadata about current indexes to the KV database,
// so we can read this information back later.
func (qs *QuadStore) writeIndexesMeta(ctx context.Context) error {
	// TODO(dennwc): change to protobuf later?
	data, err := json.Marshal(qs.indexes.all)
	if err != nil {
		return err
	}
	return kv.Update(ctx, qs.db, func(tx kv.Tx) error {
		return tx.Put(keyMetaIndexes, data)
	})
}

// readIndexesMeta read metadata about current indexes from the KV database.
// If no indexes are set, it returns a list of legacy indexes to preserve backward compatibility.
func (qs *QuadStore) readIndexesMeta(ctx context.Context) ([]QuadIndex, error) {
	tx, err := qs.db.Tx(false)
	if err != nil {
		return nil, err
	}
	defer tx.Close()
	tx = wrapTx(tx)
	val, err := tx.Get(ctx, keyMetaIndexes)
	if err == kv.ErrNotFound {
		return legacyQuadIndexes, nil
	} else if err != nil {
		return nil, err
	}
	var out []QuadIndex
	if err := json.Unmarshal(val, &out); err != nil {
		return nil, fmt.Errorf("cannot decode indexes: %v", err)
	} else if len(out) == 0 {
		return legacyQuadIndexes, nil
	}
	return out, nil
}

func (qs *QuadStore) resolveValDeltas(ctx context.Context, tx kv.Tx, deltas []graphlog.NodeUpdate, fnc func(i int, id uint64)) error {
	inds := make([]int, 0, len(deltas))
	keys := make([]kv.Key, 0, len(deltas))
	for i, d := range deltas {
		if iri, ok := d.Val.(quad.IRI); ok {
			if x, ok := qs.valueLRU.Get(string(iri)); ok {
				fnc(i, x.(uint64))
				continue
			}
		} else if d.Val == nil {
			fnc(i, 0)
			continue
		}
		if qs.mapNodes != nil && !qs.mapNodes.Test(d.Hash[:]) {
			fnc(i, 0)
			continue
		}
		inds = append(inds, i)
		keys = append(keys, bucketKeyForHash(d.Hash))
	}
	if len(keys) == 0 {
		return nil
	}
	resp, err := tx.GetBatch(ctx, keys)
	if err != nil {
		return err
	}
	keys = nil
	for i, b := range resp {
		if len(b) == 0 {
			fnc(inds[i], 0)
			continue
		}
		ind := inds[i]
		id, _ := binary.Uvarint(b)
		d := &deltas[ind]
		if iri, ok := d.Val.(quad.IRI); ok && id != 0 {
			qs.valueLRU.Put(string(iri), uint64(id))
		}
		fnc(ind, uint64(id))
	}
	return nil
}

func (qs *QuadStore) getMetaIntTx(ctx context.Context, tx kv.Tx, key string) (int64, error) {
	val, err := tx.Get(ctx, metaBucket.AppendBytes([]byte(key)))
	if err == kv.ErrNotFound {
		return 0, err
	} else if err != nil {
		return 0, fmt.Errorf("cannot get horizon value: %v", err)
	}
	return int64(binary.LittleEndian.Uint64(val)), nil
}

func (qs *QuadStore) incMetaInt(ctx context.Context, tx kv.Tx, key string, n int64) (int64, error) {
	if n == 0 {
		return 0, nil
	}
	v, err := qs.getMetaIntTx(ctx, tx, key)
	if err != nil && err != kv.ErrNotFound {
		return 0, fmt.Errorf("cannot get %s: %v", key, err)
	}
	start := v
	v += n

	buf := make([]byte, 8) // bolt needs all slices available on Commit
	binary.LittleEndian.PutUint64(buf, uint64(v))

	err = tx.Put(metaBucket.AppendBytes([]byte(key)), buf)
	if err != nil {
		return 0, fmt.Errorf("cannot inc %s: %v", key, err)
	}
	return start, nil
}

func (qs *QuadStore) genIDs(ctx context.Context, tx kv.Tx, n int) (uint64, error) {
	if n == 0 {
		return 0, nil
	}
	start, err := qs.incMetaInt(ctx, tx, "horizon", int64(n))
	if err != nil {
		return 0, err
	}
	return uint64(start + 1), nil
}

type nodeUpdate struct {
	Ind int
	ID  uint64
	graphlog.NodeUpdate
}

func (qs *QuadStore) incNodesCnt(ctx context.Context, tx kv.Tx, deltas, newDeltas []nodeUpdate) ([]int, error) {
	var buf [binary.MaxVarintLen64]byte
	// increment nodes
	keys := make([]kv.Key, 0, len(deltas))
	for _, d := range deltas {
		keys = append(keys, bucketKeyForHashRefs(d.Hash))
	}
	sizes, err := tx.GetBatch(ctx, keys)
	if err != nil {
		return nil, err
	}
	var del []int
	for i, d := range deltas {
		k := keys[i]
		var sz int64
		if sizes[i] != nil {
			szu, _ := binary.Uvarint(sizes[i])
			sz = int64(szu)
			sizes[i] = nil // cannot reuse buffer since it belongs to kv
		}
		sz += int64(d.RefInc)
		if sz <= 0 {
			if err := tx.Del(k); err != nil {
				return del, err
			}
			mNodesDel.Inc()
			del = append(del, i)
			continue
		}
		n := binary.PutUvarint(buf[:], uint64(sz))
		val := append([]byte{}, buf[:n]...)
		if err := tx.Put(k, val); err != nil {
			return del, err
		}
		mNodesUpd.Inc()
	}
	// create new nodes
	for _, d := range newDeltas {
		n := binary.PutUvarint(buf[:], uint64(d.RefInc))
		val := append([]byte{}, buf[:n]...)
		if err := tx.Put(bucketKeyForHashRefs(d.Hash), val); err != nil {
			return nil, err
		}
		mNodesNew.Inc()
	}
	return del, nil
}

type resolvedNode struct {
	ID  uint64
	New bool
}

func (qs *QuadStore) incNodes(ctx context.Context, tx kv.Tx, deltas []graphlog.NodeUpdate) (map[graph.ValueHash]resolvedNode, error) {
	var (
		ins []nodeUpdate
		upd = make([]nodeUpdate, 0, len(deltas))
		ids = make(map[graph.ValueHash]resolvedNode, len(deltas))
	)
	err := qs.resolveValDeltas(ctx, tx, deltas, func(i int, id uint64) {
		if id == 0 {
			// not exists, should create
			ins = append(ins, nodeUpdate{Ind: i, NodeUpdate: deltas[i]})
		} else {
			// exists, should update
			upd = append(upd, nodeUpdate{Ind: i, ID: id, NodeUpdate: deltas[i]})
			ids[deltas[i].Hash] = resolvedNode{ID: id}
		}
	})
	if err != nil {
		return ids, err
	}
	if len(ins) != 0 {
		// preallocate IDs
		start, err := qs.genIDs(ctx, tx, len(ins))
		if err != nil {
			return ids, err
		}
		// create and index new nodes
		for i, iv := range ins {
			id := start + uint64(i)
			node, err := createNodePrimitive(iv.Val)
			if err != nil {
				return ids, err
			}

			node.ID = id
			ids[iv.Hash] = resolvedNode{ID: id, New: true}
			if err := qs.indexNode(tx, node, iv.Val); err != nil {
				return ids, err
			}
			ins[i].ID = id
		}
	}
	_, err = qs.incNodesCnt(ctx, tx, upd, ins)
	return ids, err
}
func (qs *QuadStore) decNodes(ctx context.Context, tx kv.Tx, deltas []graphlog.NodeUpdate, nodes map[graph.ValueHash]uint64) error {
	upds := make([]nodeUpdate, 0, len(deltas))
	for i, d := range deltas {
		id := nodes[d.Hash]
		if id == 0 || d.RefInc == 0 {
			continue
		}
		upds = append(upds, nodeUpdate{Ind: i, ID: id, NodeUpdate: d})
	}
	del, err := qs.incNodesCnt(ctx, tx, upds, nil)
	if err != nil {
		return err
	}
	for _, i := range del {
		d := upds[i]
		key := bucketForVal(d.Hash[0], d.Hash[1]).AppendBytes(d.Hash[:])
		if err = tx.Del(key); err != nil {
			return err
		}
		if iri, ok := d.Val.(quad.IRI); ok {
			qs.valueLRU.Del(string(iri))
		}
		if err := qs.delLog(tx, d.ID); err != nil {
			return err
		}
	}
	return nil
}

func (qs *QuadStore) NewQuadWriter() (quad.WriteCloser, error) {
	return &quadWriter{qs: qs}, nil
}

type quadWriter struct {
	qs  *QuadStore
	tx  kv.Tx
	err error
	n   int
}

func (w *quadWriter) WriteQuad(q quad.Quad) error {
	_, err := w.WriteQuads([]quad.Quad{q})
	return err
}

func (w *quadWriter) flush() error {
	w.n = 0
	ctx := context.TODO()
	if err := w.qs.flushMapBucket(ctx, w.tx); err != nil {
		w.err = err
		return err
	}
	if err := w.tx.Commit(ctx); err != nil {
		w.qs.writer.Unlock()
		w.tx = nil
		w.err = err
		return err
	}
	tx, err := w.qs.db.Tx(true)
	if err != nil {
		w.qs.writer.Unlock()
		w.err = err
		return err
	}
	w.tx = wrapTx(tx)
	return nil
}

func (w *quadWriter) WriteQuads(buf []quad.Quad) (int, error) {
	mApplyBatch.Observe(float64(len(buf)))
	defer prometheus.NewTimer(mApplySeconds).ObserveDuration()

	if w.tx == nil {
		w.qs.writer.Lock()
		tx, err := w.qs.db.Tx(true)
		if err != nil {
			w.qs.writer.Unlock()
			w.err = err
			return 0, err
		}
		w.tx = wrapTx(tx)
	}
	deltas := graphlog.InsertQuads(buf)
	if _, err := w.qs.applyAddDeltas(w.tx, nil, deltas, graph.IgnoreOpts{IgnoreDup: true}); err != nil {
		w.err = err
		return 0, err
	}
	w.n += len(buf)
	if w.n >= quad.DefaultBatch*20 {
		if err := w.flush(); err != nil {
			return 0, err
		}
	}
	return len(buf), nil
}

func (w *quadWriter) Close() error {
	if w.tx == nil {
		return w.err
	}
	defer w.qs.writer.Unlock()

	if w.err != nil {
		_ = w.tx.Close()
		w.tx = nil
		return w.err
	}

	ctx := context.TODO()
	// flush quad indexes and commit
	err := w.qs.flushMapBucket(ctx, w.tx)
	if err != nil {
		_ = w.tx.Close()
		w.tx = nil
		return err
	}
	err = w.tx.Commit(ctx)
	w.tx = nil
	return err
}

func (qs *QuadStore) applyAddDeltas(tx kv.Tx, in []graph.Delta, deltas *graphlog.Deltas, ignoreOpts graph.IgnoreOpts) (map[graph.ValueHash]resolvedNode, error) {
	ctx := context.TODO()

	// first add all new nodes
	nodes, err := qs.incNodes(ctx, tx, deltas.IncNode)
	if err != nil {
		return nil, err
	}
	deltas.IncNode = nil
	// resolve and insert all new quads
	links := make([]proto.Primitive, 0, len(deltas.QuadAdd))
	qadd := make(map[[4]uint64]struct{}, len(deltas.QuadAdd))
	for _, q := range deltas.QuadAdd {
		var link proto.Primitive
		mustBeNew := false
		var qkey [4]uint64
		for i, dir := range quad.Directions {
			n, ok := nodes[q.Quad.Get(dir)]
			if !ok {
				continue
			}
			mustBeNew = mustBeNew || n.New
			link.SetDirection(dir, n.ID)
			qkey[i] = n.ID
		}
		if _, ok := qadd[qkey]; ok {
			continue
		}
		qadd[qkey] = struct{}{}
		if !mustBeNew {
			p, err := qs.hasPrimitive(ctx, tx, &link, false)
			if err != nil {
				return nil, err
			}
			if p != nil {
				if ignoreOpts.IgnoreDup {
					continue // already exists, no need to insert
				}
				err = graph.ErrQuadExists
				if len(in) != 0 {
					return nil, &graph.DeltaError{Delta: in[q.Ind], Err: err}
				}
				return nil, err
			}
		}
		links = append(links, link)
	}
	qadd = nil
	deltas.QuadAdd = nil

	qstart, err := qs.genIDs(ctx, tx, len(links))
	if err != nil {
		return nil, err
	}
	for i := range links {
		links[i].ID = qstart + uint64(i)
		links[i].Timestamp = time.Now().UnixNano()
	}
	if err := qs.indexLinks(ctx, tx, links); err != nil {
		return nil, err
	}
	return nodes, nil
}

func (qs *QuadStore) ApplyDeltas(in []graph.Delta, ignoreOpts graph.IgnoreOpts) error {
	mApplyBatch.Observe(float64(len(in)))
	defer prometheus.NewTimer(mApplySeconds).ObserveDuration()

	ctx := context.TODO()
	qs.writer.Lock()
	defer qs.writer.Unlock()
	tx, err := qs.db.Tx(true)
	if err != nil {
		return err
	}
	defer tx.Close()
	tx = wrapTx(tx)

	deltas := graphlog.SplitDeltas(in)
	if len(deltas.QuadDel) != 0 || len(deltas.DecNode) != 0 {
		qs.mapNodes = nil
	}

	nodes, err := qs.applyAddDeltas(tx, in, deltas, ignoreOpts)
	if err != nil {
		return err
	}

	if len(deltas.QuadDel) != 0 || len(deltas.DecNode) != 0 {
		links := make([]proto.Primitive, 0, len(deltas.QuadDel))
		// resolve all nodes that will be removed
		dnodes := make(map[graph.ValueHash]uint64, len(deltas.DecNode))
		if err := qs.resolveValDeltas(ctx, tx, deltas.DecNode, func(i int, id uint64) {
			dnodes[deltas.DecNode[i].Hash] = id
		}); err != nil {
			return err
		}

		// check for existence and delete quads
		fixNodes := make(map[graph.ValueHash]int)
		for _, q := range deltas.QuadDel {
			var link proto.Primitive
			exists := true
			// resolve values of all quad directions
			// if any of the direction does not exists, the quad does not exists as well
			for _, dir := range quad.Directions {
				h := q.Quad.Get(dir)
				n, ok := nodes[h]
				if !ok {
					var id uint64
					id, ok = dnodes[h]
					n.ID = id
				}
				if !ok {
					exists = exists && !h.Valid()
					continue
				}
				link.SetDirection(dir, n.ID)
			}
			if exists {
				p, err := qs.hasPrimitive(ctx, tx, &link, true)
				if err != nil {
					return err
				} else if p == nil || p.Deleted {
					exists = false
				} else {
					link = *p
				}
			}
			if !exists {
				if !ignoreOpts.IgnoreMissing {
					return &graph.DeltaError{Delta: in[q.Ind], Err: graph.ErrQuadNotExist}
				}
				// revert counters for all directions of this quad
				for _, dir := range quad.Directions {
					if h := q.Quad.Get(dir); h.Valid() {
						fixNodes[h]++
					}
				}
				continue
			}
			links = append(links, link)
		}
		deltas.QuadDel = nil
		if err := qs.markLinksDead(ctx, tx, links); err != nil {
			return err
		}
		links = nil
		nodes = nil

		// we decremented some nodes that has non-existent quads - let's fix this
		if len(fixNodes) != 0 {
			for i, n := range deltas.DecNode {
				if dn := fixNodes[n.Hash]; dn != 0 {
					deltas.DecNode[i].RefInc += dn
				}
			}
		}

		// finally decrement and remove nodes
		if err := qs.decNodes(ctx, tx, deltas.DecNode, dnodes); err != nil {
			return err
		}
		deltas = nil
		dnodes = nil
	}
	// flush quad indexes and commit
	err = qs.flushMapBucket(ctx, tx)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (qs *QuadStore) indexNode(tx kv.Tx, p *proto.Primitive, val quad.Value) error {
	var err error
	if val == nil {
		val, err = pquads.UnmarshalValue(p.Value)
		if err != nil {
			return err
		}
	}
	hash := quad.HashOf(val)
	err = tx.Put(bucketForVal(hash[0], hash[1]).AppendBytes(hash), uint64toBytes(p.ID))
	if err != nil {
		return err
	}
	if iri, ok := val.(quad.IRI); ok {
		qs.valueLRU.Put(string(iri), p.ID)
	}
	if qs.mapNodes != nil {
		qs.mapNodes.Add(hash[:])
	}
	return qs.addToLog(tx, p)
}

func (qs *QuadStore) indexLinks(ctx context.Context, tx kv.Tx, links []proto.Primitive) error {
	for _, p := range links {
		if err := qs.indexLink(tx, &p); err != nil {
			return err
		}
	}
	return qs.incSize(ctx, tx, int64(len(links)))
}
func (qs *QuadStore) indexLink(tx kv.Tx, p *proto.Primitive) error {
	var err error
	qs.indexes.RLock()
	all := qs.indexes.all
	qs.indexes.RUnlock()
	for _, ind := range all {
		err = qs.addToMapBucket(tx, ind.KeyFor(p), p.ID)
		if err != nil {
			return err
		}
	}
	qs.bloomAdd(p)
	err = qs.indexSchema(tx, p)
	if err != nil {
		return err
	}
	return qs.addToLog(tx, p)
}

func (qs *QuadStore) markAsDead(tx kv.Tx, p *proto.Primitive) error {
	p.Deleted = true
	//TODO(barakmich): Add tombstone?
	qs.bloomRemove(p)
	return qs.addToLog(tx, p)
}

func (qs *QuadStore) delLog(tx kv.Tx, id uint64) error {
	return tx.Del(logIndex.Append(uint64KeyBytes(id)))
}

func (qs *QuadStore) markLinksDead(ctx context.Context, tx kv.Tx, links []proto.Primitive) error {
	for _, p := range links {
		if err := qs.markAsDead(tx, &p); err != nil {
			return err
		}
	}
	return qs.incSize(ctx, tx, -int64(len(links)))
}

func (qs *QuadStore) getBucketIndexes(ctx context.Context, tx kv.Tx, keys []kv.Key) ([][]uint64, error) {
	vals, err := tx.GetBatch(ctx, keys)
	if err != nil {
		return nil, err
	}
	out := make([][]uint64, len(keys))
	for i, v := range vals {
		if len(v) == 0 {
			continue
		}
		ind, err := decodeIndex(v)
		if err != nil {
			return out, err
		}
		out[i] = ind
	}
	return out, nil
}

func countIndex(b []byte) (int64, error) {
	var cnt int64
	for len(b) > 0 {
		_, n := binary.Uvarint(b)
		if n == 0 {
			return 0, io.ErrUnexpectedEOF
		} else if n < 0 {
			return 0, errors.New("varint: overflow")
		}
		cnt++
		b = b[n:]
	}
	return cnt, nil
}

func decodeIndex(b []byte) ([]uint64, error) {
	var out []uint64
	for len(b) > 0 {
		v, n := binary.Uvarint(b)
		if n == 0 {
			return out, io.ErrUnexpectedEOF
		} else if n < 0 {
			return out, errors.New("varint: overflow")
		}
		out = append(out, v)
		b = b[n:]
	}
	return out, nil
}

func appendIndex(bytelist []byte, l []uint64) []byte {
	b := make([]byte, len(bytelist)+(binary.MaxVarintLen64*len(l)))
	copy(b[:len(bytelist)], bytelist)
	off := len(bytelist)
	for _, x := range l {
		n := binary.PutUvarint(b[off:], x)
		off += n
	}
	return b[:off]
}

func (qs *QuadStore) bestUnique() ([]QuadIndex, error) {
	qs.indexes.RLock()
	ind := qs.indexes.exists
	qs.indexes.RUnlock()
	if len(ind) != 0 {
		return ind, nil
	}
	qs.indexes.Lock()
	defer qs.indexes.Unlock()
	if len(qs.indexes.exists) != 0 {
		return qs.indexes.exists, nil
	}
	for _, in := range qs.indexes.all {
		if in.Unique {
			if clog.V(2) {
				clog.Infof("using unique index: %v", in.Dirs)
			}
			qs.indexes.exists = []QuadIndex{in}
			return qs.indexes.exists, nil
		}
	}
	// TODO: find best combination of indexes
	inds := qs.indexes.all
	if len(inds) == 0 {
		return nil, fmt.Errorf("no indexes defined")
	}
	if clog.V(2) {
		clog.Infof("using index intersection: %v", inds)
	}
	qs.indexes.exists = inds
	return qs.indexes.exists, nil
}

func hasDir(dirs []quad.Direction, d quad.Direction) bool {
	for _, d2 := range dirs {
		if d == d2 {
			return true
		}
	}
	return false
}

func (qs *QuadStore) bestIndexes(dirs []quad.Direction) []QuadIndex {
	qs.indexes.RLock()
	all := qs.indexes.all
	qs.indexes.RUnlock()
	var (
		max  int // more specific index is better
		best QuadIndex
	)
	for _, ind := range all {
		if len(ind.Dirs) < len(dirs) {
			continue // TODO(dennwc): allow intersecting indexes
		}
		match := 0
		for i, d := range ind.Dirs {
			if i >= len(dirs) || !hasDir(dirs, d) {
				break
			}
			match++
		}
		if match == len(dirs) {
			// exact index match
			return []QuadIndex{ind}
		}
		if match > 0 && match > max {
			best = ind
			max = match
		}
	}
	if max == 0 {
		return nil
	}
	// TODO(dennwc): intersect with some other index
	return []QuadIndex{best}
}

func (qs *QuadStore) hasPrimitive(ctx context.Context, tx kv.Tx, p *proto.Primitive, get bool) (*proto.Primitive, error) {
	if !qs.testBloom(p) {
		mQuadsBloomHit.Inc()
		return nil, nil
	}
	mQuadsBloomMiss.Inc()
	inds, err := qs.bestUnique()
	if err != nil {
		return nil, err
	}
	unique := len(inds) != 0 && inds[0].Unique
	keys := make([]kv.Key, len(inds))
	for i, in := range inds {
		keys[i] = in.KeyFor(p)
	}
	lists, err := qs.getBucketIndexes(ctx, tx, keys)
	if err != nil {
		return nil, err
	}
	var options []uint64
	for len(lists) > 0 {
		if len(lists) == 1 {
			options = lists[0]
			break
		}
		a, b := lists[0], lists[1]
		lists = lists[1:]
		a = intersectSortedUint64(a, b)
		lists[0] = a
	}
	if !get && unique {
		return p, nil
	}
	for i := len(options) - 1; i >= 0; i-- {
		// TODO: batch
		prim, err := qs.getPrimitiveFromLog(ctx, tx, options[i])
		if err != nil {
			return nil, err
		}
		if prim.Deleted {
			continue
		}
		if prim.IsSameLink(p) {
			return prim, nil
		}
	}
	return nil, nil
}

func intersectSortedUint64(a, b []uint64) []uint64 {
	var c []uint64
	boff := 0
outer:
	for _, x := range a {
		for {
			if boff >= len(b) {
				break outer
			}
			if x > b[boff] {
				boff++
				continue
			}
			if x < b[boff] {
				break
			}
			if x == b[boff] {
				c = append(c, x)
				boff++
				break
			}
		}
	}
	return c
}

func (qs *QuadStore) addToMapBucket(tx kv.Tx, key kv.Key, value uint64) error {
	if len(key) != 2 {
		return fmt.Errorf("trying to add to map bucket with invalid key: %v", key)
	}
	b, k := key[0], key[1]
	if len(k) == 0 {
		return fmt.Errorf("trying to add to map bucket %s with key 0", b)
	}
	if qs.mapBucket == nil {
		qs.mapBucket = make(map[string]map[string][]uint64)
	}
	bucket := string(b)
	m, ok := qs.mapBucket[bucket]
	if !ok {
		m = make(map[string][]uint64)
		qs.mapBucket[bucket] = m
	}
	m[string(k)] = append(m[string(k)], value)
	mIndexWriteBufferEntries.WithLabelValues(bucket).Inc()
	return nil
}

func (qs *QuadStore) flushMapBucket(ctx context.Context, tx kv.Tx) error {
	bs := make([]string, 0, len(qs.mapBucket))
	for k := range qs.mapBucket {
		bs = append(bs, k)
	}
	sort.Strings(bs)
	for _, bucket := range bs {
		m := qs.mapBucket[bucket]
		if len(m) == 0 {
			continue
		}
		bloom := qs.mapBloom[bucket]
		mIndexWriteBufferFlushBatch.WithLabelValues(bucket).Observe(float64(len(m)))
		entryBytes := mIndexEntrySizeBytes.WithLabelValues(bucket)
		b := kv.Key{[]byte(bucket)}
		var (
			keys    []kv.Key
			keysPut []kv.Key
		)
		if qs.mapBloom == nil {
			keys = make([]kv.Key, 0, len(m))
		}
		for k := range m {
			bk := []byte(k)
			if qs.mapBloom != nil && (bloom == nil || !bloom.Test(bk)) {
				keysPut = append(keysPut, b.AppendBytes(bk))
			} else {
				keys = append(keys, b.AppendBytes(bk))
			}
		}
		sort.Sort(kv.ByKey(keysPut))
		sort.Sort(kv.ByKey(keys))
		vals, err := tx.GetBatch(ctx, keys)
		if err != nil {
			return err
		}
		if qs.mapBloom != nil && bloom == nil {
			bloom = boom.NewBloomFilter(100*1000*1000, 0.05)
			qs.mapBloom[bucket] = bloom
		}
		for _, k := range keysPut {
			l := m[string(k[1])]
			err = tx.Put(k, appendIndex(nil, l))
			if err != nil {
				return err
			}
			if bloom != nil {
				bloom.Add(k[1])
			}
		}
		for i, k := range keys {
			l := m[string(k[1])]
			buf := appendIndex(vals[i], l)
			entryBytes.Observe(float64(len(buf)))
			err = tx.Put(k, buf)
			if err != nil {
				return err
			}
			if bloom != nil {
				bloom.Add(k[1])
			}
		}
		mIndexWriteBufferEntries.WithLabelValues(bucket).Set(0)
	}
	qs.mapBucket = nil
	return nil
}

func (qs *QuadStore) indexSchema(tx kv.Tx, p *proto.Primitive) error {
	return nil
}

func (qs *QuadStore) addToLog(tx kv.Tx, p *proto.Primitive) error {
	buf, err := p.Marshal()
	if err != nil {
		return err
	}
	if err := tx.Put(logIndex.Append(uint64KeyBytes(p.ID)), buf); err != nil {
		return err
	}
	mPrimitiveAppend.Inc()
	return nil
}

func createNodePrimitive(v quad.Value) (*proto.Primitive, error) {
	p := &proto.Primitive{}
	b, err := pquads.MarshalValue(v)
	if err != nil {
		return p, err
	}
	p.Value = b
	p.Timestamp = time.Now().UnixNano()
	return p, nil
}

func (qs *QuadStore) resolveQuadValue(ctx context.Context, tx kv.Tx, v quad.Value) (uint64, error) {
	out, err := qs.resolveQuadValues(ctx, tx, []quad.Value{v})
	if err != nil {
		return 0, err
	}
	return out[0], nil
}

func bucketKeyForVal(v quad.Value) kv.Key {
	hash := graph.HashOf(v)
	return bucketKeyForHash(hash)
}

func bucketKeyForHash(h graph.ValueHash) kv.Key {
	return bucketForVal(h[0], h[1]).AppendBytes(h[:])
}

func bucketKeyForHashRefs(h graph.ValueHash) kv.Key {
	return bucketForValRefs(h[0], h[1]).AppendBytes(h[:])
}

func (qs *QuadStore) resolveQuadValues(ctx context.Context, tx kv.Tx, vals []quad.Value) ([]uint64, error) {
	out := make([]uint64, len(vals))
	inds := make([]int, 0, len(vals))
	keys := make([]kv.Key, 0, len(vals))
	for i, v := range vals {
		if iri, ok := v.(quad.IRI); ok {
			if x, ok := qs.valueLRU.Get(string(iri)); ok {
				out[i] = x.(uint64)
				continue
			}
		} else if v == nil {
			continue
		}
		inds = append(inds, i)
		keys = append(keys, bucketKeyForVal(v))
	}
	if len(keys) == 0 {
		return out, nil
	}
	resp, err := tx.GetBatch(ctx, keys)
	if err != nil {
		return out, err
	}
	for i, b := range resp {
		if len(b) == 0 {
			continue
		}
		ind := inds[i]
		out[ind], _ = binary.Uvarint(b)
		if iri, ok := vals[ind].(quad.IRI); ok && out[ind] != 0 {
			qs.valueLRU.Put(string(iri), uint64(out[ind]))
		}
	}
	return out, nil
}

func uint64toBytes(x uint64) []byte {
	b := make([]byte, binary.MaxVarintLen64)
	return uint64toBytesAt(x, b)
}

func uint64toBytesAt(x uint64, bytes []byte) []byte {
	n := binary.PutUvarint(bytes, x)
	return bytes[:n]
}

func uint64KeyBytes(x uint64) kv.Key {
	k := make([]byte, 8)
	quadKeyEnc.PutUint64(k, x)
	return kv.Key{k}
}

func (qs *QuadStore) getPrimitivesFromLog(ctx context.Context, tx kv.Tx, keys []uint64) ([]*proto.Primitive, error) {
	bkeys := make([]kv.Key, len(keys))
	for i, k := range keys {
		bkeys[i] = logIndex.Append(uint64KeyBytes(k))
	}
	vals, err := tx.GetBatch(ctx, bkeys)
	if err != nil {
		return nil, err
	}
	mPrimitiveFetch.Add(float64(len(vals)))
	out := make([]*proto.Primitive, len(keys))
	var last error
	for i, v := range vals {
		if v == nil {
			mPrimitiveFetchMiss.Inc()
			continue
		}
		var p proto.Primitive
		if err = p.Unmarshal(v); err != nil {
			last = err
		} else {
			out[i] = &p
		}
	}
	return out, last
}

func (qs *QuadStore) getPrimitiveFromLog(ctx context.Context, tx kv.Tx, k uint64) (*proto.Primitive, error) {
	out, err := qs.getPrimitivesFromLog(ctx, tx, []uint64{k})
	if err != nil {
		return nil, err
	} else if out[0] == nil {
		return nil, kv.ErrNotFound
	}
	return out[0], nil
}

func (qs *QuadStore) initBloomFilter(ctx context.Context) error {
	if qs.exists.disabled {
		return nil
	}
	qs.exists.buf = make([]byte, 3*8)
	qs.exists.DeletableBloomFilter = boom.NewDeletableBloomFilter(100*1000*1000, 120, 0.05)
	return kv.View(qs.db, func(tx kv.Tx) error {
		p := proto.Primitive{}
		it := tx.Scan(logIndex)
		defer it.Close()
		for it.Next(ctx) {
			v := it.Val()
			p = proto.Primitive{}
			err := p.Unmarshal(v)
			if err != nil {
				return err
			}
			if p.IsNode() {
				continue
			} else if p.Deleted {
				continue
			}
			writePrimToBuf(&p, qs.exists.buf)
			qs.exists.Add(qs.exists.buf)
		}
		return it.Err()
	})
}

func (qs *QuadStore) testBloom(p *proto.Primitive) bool {
	if qs.exists.disabled {
		return true // false positives are expected
	}
	qs.exists.Lock()
	defer qs.exists.Unlock()
	writePrimToBuf(p, qs.exists.buf)
	return qs.exists.Test(qs.exists.buf)
}

func (qs *QuadStore) bloomRemove(p *proto.Primitive) {
	if qs.exists.disabled {
		return
	}
	qs.exists.Lock()
	defer qs.exists.Unlock()
	writePrimToBuf(p, qs.exists.buf)
	qs.exists.TestAndRemove(qs.exists.buf)
}

func (qs *QuadStore) bloomAdd(p *proto.Primitive) {
	if qs.exists.disabled {
		return
	}
	qs.exists.Lock()
	defer qs.exists.Unlock()
	writePrimToBuf(p, qs.exists.buf)
	qs.exists.Add(qs.exists.buf)
}

func writePrimToBuf(p *proto.Primitive, buf []byte) {
	quadKeyEnc.PutUint64(buf[0:8], p.Subject)
	quadKeyEnc.PutUint64(buf[8:16], p.Predicate)
	quadKeyEnc.PutUint64(buf[16:24], p.Object)
}

type Int64Set []uint64

func (a Int64Set) Len() int           { return len(a) }
func (a Int64Set) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a Int64Set) Less(i, j int) bool { return a[i] < a[j] }
