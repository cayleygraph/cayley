// Copyright 2014 The Cayley Authors. All rights reserved.
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

package memstore

import (
	"fmt"
	"strconv"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/quad"
)

const QuadStoreType = "memstore"

func init() {
	graph.RegisterQuadStore(QuadStoreType, graph.QuadStoreRegistration{
		NewFunc: func(string, graph.Options) (graph.QuadStore, error) {
			return newQuadStore(), nil
		},
		UpgradeFunc:  nil,
		InitFunc:     nil,
		IsPersistent: false,
	})
}

type bnode int64

func (n bnode) Key() interface{} { return n }

type qprim struct {
	p *primitive
}

func (n qprim) Key() interface{} { return n.p.ID }

var _ quad.Writer = (*QuadStore)(nil)

func cmp(a, b int64) int {
	return int(a - b)
}

type QuadDirectionIndex struct {
	index [4]map[int64]*Tree
}

func NewQuadDirectionIndex() QuadDirectionIndex {
	return QuadDirectionIndex{[...]map[int64]*Tree{
		quad.Subject - 1:   make(map[int64]*Tree),
		quad.Predicate - 1: make(map[int64]*Tree),
		quad.Object - 1:    make(map[int64]*Tree),
		quad.Label - 1:     make(map[int64]*Tree),
	}}
}

func (qdi QuadDirectionIndex) Tree(d quad.Direction, id int64) *Tree {
	if d < quad.Subject || d > quad.Label {
		panic("illegal direction")
	}
	tree, ok := qdi.index[d-1][id]
	if !ok {
		tree = TreeNew(cmp)
		qdi.index[d-1][id] = tree
	}
	return tree
}

func (qdi QuadDirectionIndex) Get(d quad.Direction, id int64) (*Tree, bool) {
	if d < quad.Subject || d > quad.Label {
		panic("illegal direction")
	}
	tree, ok := qdi.index[d-1][id]
	return tree, ok
}

type primitive struct {
	ID    int64
	Quad  internalQuad
	Value quad.Value
}

type internalQuad struct {
	S, P, O, L int64
}

func (q internalQuad) Zero() bool {
	return q == (internalQuad{})
}

func (q *internalQuad) SetDir(d quad.Direction, n int64) {
	switch d {
	case quad.Subject:
		q.S = n
	case quad.Predicate:
		q.P = n
	case quad.Object:
		q.O = n
	case quad.Label:
		q.L = n
	default:
		panic(fmt.Errorf("unknown dir: %v", d))
	}
}
func (q internalQuad) Dir(d quad.Direction) int64 {
	var n int64
	switch d {
	case quad.Subject:
		n = q.S
	case quad.Predicate:
		n = q.P
	case quad.Object:
		n = q.O
	case quad.Label:
		n = q.L
	}
	return n
}

type QuadStore struct {
	last int64
	// TODO: string -> quad.Value once Raw -> typed resolution is unnecessary
	vals    map[string]int64
	quads   map[internalQuad]int64
	prim    map[int64]*primitive
	all     []*primitive // might not be sorted by id
	index   QuadDirectionIndex
	horizon int64 // used only to assign ids to tx
	// vip_index map[string]map[int64]map[string]map[int64]*b.Tree
}

// New creates a new in-memory quad store and loads provided quads.
func New(quads ...quad.Quad) *QuadStore {
	qs := newQuadStore()
	for _, q := range quads {
		qs.AddQuad(q)
	}
	return qs
}

func newQuadStore() *QuadStore {
	return &QuadStore{
		vals:  make(map[string]int64),
		quads: make(map[internalQuad]int64),
		prim:  make(map[int64]*primitive),
		index: NewQuadDirectionIndex(),
	}
}

func (qs *QuadStore) addPrimitive(p *primitive) int64 {
	qs.last++
	id := qs.last
	p.ID = id
	qs.appendPrimitive(p)
	return id
}

func (qs *QuadStore) appendPrimitive(p *primitive) {
	qs.prim[p.ID] = p
	qs.all = append(qs.all, p)
}

func (qs *QuadStore) resolveVal(v quad.Value, add bool) (int64, bool) {
	if v == nil {
		return 0, false
	}
	if n, ok := v.(quad.BNode); ok && len(n) > 1 && n[0] == 'n' {
		n = n[1:]
		id, err := strconv.ParseInt(string(n), 10, 64)
		if err == nil && id != 0 {
			_, ok := qs.prim[id]
			if ok || !add {
				return id, ok
			}
			qs.appendPrimitive(&primitive{ID: id})
			return id, true
		}
	}
	vs := v.String()
	if id, exists := qs.vals[vs]; exists || !add {
		return id, exists
	}
	id := qs.addPrimitive(&primitive{Value: v})
	qs.vals[vs] = id
	return id, true
}

func (qs *QuadStore) resolveQuad(q quad.Quad, add bool) (internalQuad, bool) {
	var p internalQuad
	for dir := quad.Subject; dir <= quad.Label; dir++ {
		v := q.Get(dir)
		if v == nil {
			continue
		}
		if vid, _ := qs.resolveVal(v, add); vid != 0 {
			p.SetDir(dir, vid)
		} else if !add {
			return internalQuad{}, false
		}
	}
	return p, true
}

func (qs *QuadStore) lookupVal(id int64) quad.Value {
	pv := qs.prim[id]
	if pv == nil || pv.Value == nil {
		return quad.BNode("n" + strconv.FormatInt(id, 10))
	}
	return pv.Value
}

func (qs *QuadStore) lookupQuadDirs(p internalQuad) quad.Quad {
	var q quad.Quad
	for dir := quad.Subject; dir <= quad.Label; dir++ {
		vid := p.Dir(dir)
		if vid == 0 {
			continue
		}
		v := qs.lookupVal(vid)
		q.Set(dir, v)
	}
	return q
}

// AddNode adds a blank node (with no value) to quad store. It returns an id of the node.
func (qs *QuadStore) AddBNode() int64 {
	return qs.addPrimitive(&primitive{})
}

// AddNode adds a value to quad store. It returns an id of the value.
// False is returned as a second parameter if value exists already.
func (qs *QuadStore) AddValue(v quad.Value) (int64, bool) {
	id, exists := qs.resolveVal(v, true)
	return id, !exists
}

func (qs *QuadStore) indexesForQuad(q internalQuad) []*Tree {
	trees := make([]*Tree, 0, 4)
	for dir := quad.Subject; dir <= quad.Label; dir++ {
		v := q.Dir(dir)
		if v == 0 {
			continue
		}
		trees = append(trees, qs.index.Tree(dir, v))
	}
	return trees
}

// AddQuad adds a quad to quad store. It returns an id of the quad.
// False is returned as a second parameter if quad exists already.
func (qs *QuadStore) AddQuad(q quad.Quad) (int64, bool) {
	p, _ := qs.resolveQuad(q, true)
	if id := qs.quads[p]; id != 0 {
		return id, false
	}
	pr := &primitive{Quad: p}
	id := qs.addPrimitive(pr)
	qs.quads[p] = id
	for _, t := range qs.indexesForQuad(p) {
		t.Set(id, pr)
	}
	// TODO(barakmich): Add VIP indexing
	return id, true
}

// WriteQuad adds a quad to quad store.
//
// Deprecated: use AddQuad instead.
func (qs *QuadStore) WriteQuad(q quad.Quad) error {
	qs.AddQuad(q)
	return nil
}

func (qs *QuadStore) Delete(id int64) bool {
	p := qs.prim[id]
	if p == nil {
		return false
	}
	// remove from value index
	if p.Value != nil {
		delete(qs.vals, p.Value.String())
	}
	// remove from quad indexes
	for _, t := range qs.indexesForQuad(p.Quad) {
		t.Delete(id)
	}
	delete(qs.quads, p.Quad)
	// remove primitive
	delete(qs.prim, id)
	for i, p2 := range qs.all {
		if p == p2 {
			qs.all = append(qs.all[:i], qs.all[i+1:]...)
			break
		}
	}
	return true
}

func (qs *QuadStore) findQuad(q quad.Quad) (int64, internalQuad, bool) {
	p, ok := qs.resolveQuad(q, false)
	if !ok {
		return 0, p, false
	}
	id := qs.quads[p]
	return id, p, id != 0
}

func (qs *QuadStore) ApplyDeltas(deltas []graph.Delta, ignoreOpts graph.IgnoreOpts) error {
	// Precheck the whole transaction (if required)
	if !ignoreOpts.IgnoreDup || !ignoreOpts.IgnoreMissing {
		for _, d := range deltas {
			switch d.Action {
			case graph.Add:
				if !ignoreOpts.IgnoreDup {
					if _, _, ok := qs.findQuad(d.Quad); ok {
						return &graph.DeltaError{Delta: d, Err: graph.ErrQuadExists}
					}
				}
			case graph.Delete:
				if !ignoreOpts.IgnoreMissing {
					if _, _, ok := qs.findQuad(d.Quad); !ok {
						return &graph.DeltaError{Delta: d, Err: graph.ErrQuadNotExist}
					}
				}
			default:
				return &graph.DeltaError{Delta: d, Err: graph.ErrInvalidAction}
			}
		}
	}

	for _, d := range deltas {
		switch d.Action {
		case graph.Add:
			qs.AddQuad(d.Quad)
		case graph.Delete:
			if id, _, ok := qs.findQuad(d.Quad); ok {
				qs.Delete(id)
			}
		default:
			// TODO: ideally we should rollback it
			return &graph.DeltaError{Delta: d, Err: graph.ErrInvalidAction}
		}
	}
	qs.horizon++
	return nil
}

func asID(v graph.Value) (int64, bool) {
	switch v := v.(type) {
	case bnode:
		return int64(v), true
	case qprim:
		return v.p.ID, true
	default:
		return 0, false
	}
}

func (qs *QuadStore) quad(v graph.Value) (q internalQuad, ok bool) {
	switch v := v.(type) {
	case bnode:
		p := qs.prim[int64(v)]
		if p == nil {
			return
		}
		q = p.Quad
	case qprim:
		q = v.p.Quad
	default:
		return internalQuad{}, false
	}
	return q, !q.Zero()
}

func (qs *QuadStore) Quad(index graph.Value) quad.Quad {
	q, ok := qs.quad(index)
	if !ok {
		return quad.Quad{}
	}
	return qs.lookupQuadDirs(q)
}

func (qs *QuadStore) QuadIterator(d quad.Direction, value graph.Value) graph.Iterator {
	id, ok := asID(value)
	if !ok {
		return iterator.NewNull()
	}
	index, ok := qs.index.Get(d, id)
	if ok && index.Len() != 0 {
		return NewIterator(index, qs, d, id)
	}
	return iterator.NewNull()
}

func (qs *QuadStore) Horizon() graph.PrimaryKey {
	return graph.NewSequentialKey(qs.horizon)
}

func (qs *QuadStore) Size() int64 {
	return int64(len(qs.prim))
}

func (qs *QuadStore) ValueOf(name quad.Value) graph.Value {
	if name == nil {
		return nil
	}
	id := qs.vals[name.String()]
	if id == 0 {
		return nil
	}
	return bnode(id)
}

func (qs *QuadStore) NameOf(v graph.Value) quad.Value {
	if v == nil {
		return nil
	} else if v, ok := v.(graph.PreFetchedValue); ok {
		return v.NameOf()
	}
	n, ok := asID(v)
	if !ok {
		return nil
	}
	if _, ok = qs.prim[n]; !ok {
		return nil
	}
	return qs.lookupVal(n)
}

func (qs *QuadStore) QuadsAllIterator() graph.Iterator {
	return newAllIterator(qs, false, qs.last)
}

func (qs *QuadStore) FixedIterator() graph.FixedIterator {
	return iterator.NewFixed(iterator.Identity)
}

func (qs *QuadStore) QuadDirection(val graph.Value, d quad.Direction) graph.Value {
	q, ok := qs.quad(val)
	if !ok {
		return nil
	}
	id := q.Dir(d)
	if id == 0 {
		return nil
	}
	return bnode(id)
}

func (qs *QuadStore) NodesAllIterator() graph.Iterator {
	return newAllIterator(qs, true, qs.last)
}

func (qs *QuadStore) Close() error { return nil }
