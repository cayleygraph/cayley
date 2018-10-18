// Copyright 2017 The Cayley Authors. All rights reserved.
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
	"errors"
	"fmt"
	"sync"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/proto"
	"github.com/cayleygraph/cayley/internal/lru"
	"github.com/cayleygraph/cayley/quad"
	"github.com/cayleygraph/cayley/quad/pquads"
	boom "github.com/tylertreat/BoomFilters"
)

type Registration struct {
	NewFunc      NewFunc
	InitFunc     InitFunc
	IsPersistent bool
}

type InitFunc func(string, graph.Options) (BucketKV, error)
type NewFunc func(string, graph.Options) (BucketKV, error)

func Register(name string, r Registration) {
	graph.RegisterQuadStore(name, graph.QuadStoreRegistration{
		InitFunc: func(addr string, opt graph.Options) error {
			if !r.IsPersistent {
				return nil
			}
			kv, err := r.InitFunc(addr, opt)
			if err != nil {
				return err
			}
			defer kv.Close()
			if err = Init(kv, opt); err != nil {
				return err
			}
			return kv.Close()
		},
		NewFunc: func(addr string, opt graph.Options) (graph.QuadStore, error) {
			kv, err := r.NewFunc(addr, opt)
			if err != nil {
				return nil, err
			}
			if !r.IsPersistent {
				if err = Init(kv, opt); err != nil {
					kv.Close()
					return nil, err
				}
			}
			return New(kv, opt)
		},
		IsPersistent: r.IsPersistent,
	})
}

const (
	latestDataVersion = 2
	nilDataVersion    = 1
)

var _ graph.BatchQuadStore = (*QuadStore)(nil)

type QuadStore struct {
	db BucketKV

	indexes struct {
		sync.RWMutex
		all []QuadIndex
		// indexes used to detect duplicate quads
		exists []QuadIndex
	}

	valueLRU *lru.Cache

	writer    sync.Mutex
	mapBucket map[string]map[string][]uint64

	exists struct {
		sync.Mutex
		buf []byte
		*boom.DeletableBloomFilter
	}
}

func newQuadStore(kv BucketKV) *QuadStore {
	qs := &QuadStore{db: kv}
	qs.indexes.all = DefaultQuadIndexes
	return qs
}

func Init(kv BucketKV, opt graph.Options) error {
	ctx := context.TODO()
	qs := newQuadStore(kv)
	if _, err := qs.getMetadata(ctx); err == nil {
		return graph.ErrDatabaseExists
	} else if err != ErrNoBucket {
		return err
	}
	upfront, err := opt.BoolKey("upfront", false)
	if err != nil {
		return err
	}
	if err := qs.createBuckets(ctx, upfront); err != nil {
		return err
	}
	if err := setVersion(ctx, qs.db, latestDataVersion); err != nil {
		return err
	}
	return nil
}

func New(kv BucketKV, _ graph.Options) (graph.QuadStore, error) {
	ctx := context.TODO()
	qs := newQuadStore(kv)
	if vers, err := qs.getMetadata(ctx); err == ErrNoBucket {
		return nil, graph.ErrNotInitialized
	} else if err != nil {
		return nil, err
	} else if vers != latestDataVersion {
		return nil, errors.New("kv: data version is out of date. Run cayleyupgrade for your config to update the data.")
	}
	qs.valueLRU = lru.New(2000)
	if err := qs.initBloomFilter(ctx); err != nil {
		return nil, err
	}
	return qs, nil
}

func setVersion(ctx context.Context, kv BucketKV, version int64) error {
	return Update(ctx, kv, func(tx BucketTx) error {
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], uint64(version))
		b := tx.Bucket(metaBucket)
		if err := b.Put([]byte("version"), buf[:]); err != nil {
			return fmt.Errorf("couldn't write version: %v", err)
		}
		return nil
	})
}

func (qs *QuadStore) getMetaInt(ctx context.Context, key string) (int64, error) {
	var v int64
	err := View(qs.db, func(tx BucketTx) error {
		b := tx.Bucket(metaBucket)
		var err error
		vals, err := b.Get(ctx, [][]byte{
			[]byte(key),
		})
		if err != nil {
			return err
		} else if vals[0] == nil {
			return ErrNoBucket
		}
		v, err = asInt64(vals[0], 0)
		if err != nil {
			return err
		}
		return nil
	})
	return v, err
}

func (qs *QuadStore) Size() int64 {
	sz, _ := qs.getMetaInt(context.TODO(), "size")
	return sz
}

func (qs *QuadStore) Close() error {
	return qs.db.Close()
}

func (qs *QuadStore) getMetadata(ctx context.Context) (int64, error) {
	var vers int64
	err := View(qs.db, func(tx BucketTx) error {
		b := tx.Bucket(metaBucket)
		var err error
		vals, err := b.Get(ctx, [][]byte{
			[]byte("version"),
		})
		if err == ErrNotFound {
			return ErrNoBucket
		} else if err != nil {
			return err
		} else if vals[0] == nil {
			return ErrNoBucket
		}
		vers, err = asInt64(vals[0], nilDataVersion)
		if err != nil {
			return err
		}
		return nil
	})
	return vers, err
}

func asInt64(b []byte, empty int64) (int64, error) {
	if len(b) == 0 {
		return empty, nil
	} else if len(b) != 8 {
		return 0, fmt.Errorf("unexpected int size: %d", len(b))
	}
	v := int64(binary.LittleEndian.Uint64(b))
	return v, nil
}

func (qs *QuadStore) horizon(ctx context.Context) int64 {
	h, _ := qs.getMetaInt(ctx, "horizon")
	return h
}

func (qs *QuadStore) ValuesOf(ctx context.Context, vals []graph.Value) ([]quad.Value, error) {
	out := make([]quad.Value, len(vals))
	var (
		inds []int
		refs []uint64
	)
	for i, v := range vals {
		if v == nil {
			continue
		} else if pv, ok := v.(graph.PreFetchedValue); ok {
			out[i] = pv.NameOf()
			continue
		}
		switch v := v.(type) {
		case Int64Value:
			if v == 0 {
				continue
			}
			inds = append(inds, i)
			refs = append(refs, uint64(v))
		default:
			return out, fmt.Errorf("unknown type of graph.Value; not meant for this quadstore. apparently a %#v", v)
		}
	}
	if len(refs) == 0 {
		return out, nil
	}
	prim, err := qs.getPrimitives(ctx, refs)
	if err != nil {
		return out, err
	}
	var last error
	for i, p := range prim {
		if !p.IsNode() {
			continue
		}
		qv, err := pquads.UnmarshalValue(p.Value)
		if err != nil {
			last = err
			continue
		}
		out[inds[i]] = qv
	}
	return out, last
}

func (qs *QuadStore) RefsOf(ctx context.Context, nodes []quad.Value) ([]graph.Value, error) {
	values := make([]graph.Value, len(nodes))
	err := View(qs.db, func(tx BucketTx) error {
		for i, node := range nodes {
			value, err := qs.resolveQuadValue(ctx, tx, node)
			if err != nil {
				return err
			}
			values[i] = Int64Value(value)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return values, nil
}

func (qs *QuadStore) NameOf(v graph.Value) quad.Value {
	ctx := context.TODO()
	vals, err := qs.ValuesOf(ctx, []graph.Value{v})
	if err != nil {
		clog.Errorf("error getting NameOf %d: %s", v, err)
		return nil
	}
	return vals[0]
}

func (qs *QuadStore) Quad(k graph.Value) quad.Quad {
	key, ok := k.(*proto.Primitive)
	if !ok {
		clog.Errorf("passed value was not a quad primitive: %T", k)
		return quad.Quad{}
	}
	ctx := context.TODO()
	var v quad.Quad
	err := View(qs.db, func(tx BucketTx) error {
		var err error
		v, err = qs.primitiveToQuad(ctx, tx, key)
		return err
	})
	if err != nil {
		if err != ErrNotFound {
			clog.Errorf("error fetching quad %#v: %s", key, err)
		}
		return quad.Quad{}
	}
	return v
}

func (qs *QuadStore) primitiveToQuad(ctx context.Context, tx BucketTx, p *proto.Primitive) (quad.Quad, error) {
	q := &quad.Quad{}
	for _, dir := range quad.Directions {
		v := p.GetDirection(dir)
		val, err := qs.getValFromLog(ctx, tx, v)
		if err != nil {
			return *q, err
		}
		q.Set(dir, val)
	}
	return *q, nil
}

func (qs *QuadStore) getValFromLog(ctx context.Context, tx BucketTx, k uint64) (quad.Value, error) {
	if k == 0 {
		return nil, nil
	}
	p, err := qs.getPrimitiveFromLog(ctx, tx, k)
	if err != nil {
		return nil, err
	}
	return pquads.UnmarshalValue(p.Value)
}

func (qs *QuadStore) ValueOf(s quad.Value) graph.Value {
	ctx := context.TODO()
	var out Int64Value
	_ = View(qs.db, func(tx BucketTx) error {
		v, err := qs.resolveQuadValue(ctx, tx, s)
		out = Int64Value(v)
		return err
	})
	if out == 0 {
		return nil
	}
	return out
}

func (qs *QuadStore) QuadDirection(val graph.Value, d quad.Direction) graph.Value {
	p, ok := val.(*proto.Primitive)
	if !ok {
		return nil
	}
	switch d {
	case quad.Subject:
		return Int64Value(p.Subject)
	case quad.Predicate:
		return Int64Value(p.Predicate)
	case quad.Object:
		return Int64Value(p.Object)
	case quad.Label:
		if p.Label == 0 {
			return nil
		}
		return Int64Value(p.Label)
	}
	return nil
}

func (qs *QuadStore) getPrimitives(ctx context.Context, vals []uint64) ([]*proto.Primitive, error) {
	tx, err := qs.db.Tx(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	return qs.getPrimitivesFromLog(ctx, tx, vals)
}

type Int64Value uint64

func (v Int64Value) Key() interface{} { return v }
