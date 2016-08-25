package etcd

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/graph/proto"
	"github.com/cayleygraph/cayley/quad"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"strings"
)

func init() {
	graph.RegisterQuadStore(Type, graph.QuadStoreRegistration{
		NewFunc: func(addr string, opts graph.Options) (graph.QuadStore, error) {
			qs, err := newQuadStore(addr, opts)
			if err != nil {
				return nil, err
			}
			if err = qs.open(); err != nil {
				qs.Close()
				return nil, err
			}
			return qs, nil
		},
		InitFunc: func(addr string, opts graph.Options) error {
			qs, err := newQuadStore(addr, opts)
			if err != nil {
				return err
			}
			err = qs.create()
			qs.Close()
			return err
		},
		NewForRequestFunc: nil,
		UpgradeFunc:       nil,
		IsPersistent:      true,
	})
}

const (
	version = 1

	Type = "etcd"
)

var _ graph.QuadStore = (*QuadStore)(nil)

var (
	indSPO = [4]quad.Direction{quad.Subject, quad.Predicate, quad.Object, quad.Label}
	indOSP = [4]quad.Direction{quad.Object, quad.Subject, quad.Predicate, quad.Label}
	indPOS = [4]quad.Direction{quad.Predicate, quad.Object, quad.Subject, quad.Label}
	indCPS = [4]quad.Direction{quad.Label, quad.Predicate, quad.Subject, quad.Object}
)

func Open(etc *clientv3.Client, prefix string) (*QuadStore, error) {
	qs := &QuadStore{etc: etc, prefix: prefix}
	if err := qs.open(); err != nil {
		return nil, err
	}
	return qs, nil
}

func Create(etc *clientv3.Client, prefix string) (*QuadStore, error) {
	qs := &QuadStore{etc: etc, prefix: prefix}
	if err := qs.create(); err != nil {
		return nil, err
	}
	return qs, nil
}

func newQuadStore(endpoints string, opts graph.Options) (*QuadStore, error) {
	etc, err := clientv3.New(clientv3.Config{
		Endpoints: strings.Split(endpoints, ","),
	})
	if err != nil {
		return nil, err
	}
	pref, _, _ := opts.StringKey("prefix")
	return &QuadStore{etc: etc, prefix: pref}, nil
}

type ValueHash [quad.HashSize]byte

func (ValueHash) IsNode() bool { return true }

type QuadHash [4 * quad.HashSize]byte

func (QuadHash) IsNode() bool { return false }

func (h QuadHash) Get(d quad.Direction) (v ValueHash) {
	off := 0
	switch d {
	case quad.Subject:
		off = 0
	case quad.Predicate:
		off = 1
	case quad.Object:
		off = 2
	case quad.Label:
		off = 3
	default:
		panic(fmt.Errorf("unknown quad direction: %v", d))
	}
	copy(v[:], h[off*quad.HashSize:])
	return
}

type QuadStore struct {
	etc    *clientv3.Client
	prefix string
}

func (qs *QuadStore) open() error {
	vers, err := qs.getDBVersion(context.TODO())
	if err != nil {
		return err
	} else if vers != version {
		return fmt.Errorf("DB version is %d, but %d only is supported; use cayleyupgrade", vers, version)
	}
	return nil
}
func (qs *QuadStore) create() error {
	return qs.setDBVersion(context.TODO())
}

func (qs *QuadStore) Type() string                { return Type }
func (qs *QuadStore) Close()                      { qs.etc.Close() }
func (qs *QuadStore) keyDBVersion() string        { return qs.prefix + "vers" }
func (qs *QuadStore) prefValue() string           { return qs.prefix + "n" }
func (qs *QuadStore) keyValue(h ValueHash) string { return qs.prefValue() + string(h[:]) }
func (qs *QuadStore) prefQuad(d [4]quad.Direction, add int) ([]byte, int) {
	key := make([]byte, len(qs.prefix)+2+add)
	n := copy(key, qs.prefix)
	key[n] = d[0].Prefix()
	n++
	if d[1] != quad.Any {
		key[n] = d[1].Prefix()
		n++
	}
	return key, n
}
func (qs *QuadStore) keyQuad(q quad.Quad, d [4]quad.Direction) string {
	key, n := qs.prefQuad(d, quad.HashSize*4)
	for i, d := range d {
		quad.HashTo(q.Get(d), key[n+quad.HashSize*i:n+quad.HashSize*(i+1)])
	}
	return string(key)
}
func (qs *QuadStore) keyQuadHash(h QuadHash, d [4]quad.Direction) string {
	key, n := qs.prefQuad(d, quad.HashSize*4)
	if d == indSPO {
		copy(key[n:], h[:])
	} else {
		panic("not implemented")
	}
	return string(key)
}
func (qs *QuadStore) getDBVersion(ctx context.Context) (int, error) {
	resp, err := qs.etc.Get(ctx, qs.keyDBVersion())
	if err != nil {
		return 0, err
	}
	if len(resp.Kvs) == 0 || resp.Kvs[0] == nil {
		return 0, errors.New("DB is not initialized")
	} else if len(resp.Kvs[0].Value) != 4 {
		return 0, errors.New("unknown DB version")
	}
	return int(binary.LittleEndian.Uint32(resp.Kvs[0].Value)), nil
}
func (qs *QuadStore) setDBVersion(ctx context.Context) error {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, version)
	resp, err := qs.etc.Txn(ctx).If(
		cmpNotExists(qs.keyDBVersion()),
	).Then(
		clientv3.OpPut(qs.keyDBVersion(), string(buf)),
	).Commit()
	if err != nil {
		return err
	} else if !resp.Succeeded {
		return graph.ErrDatabaseExists
	}
	return nil
}
func (qs *QuadStore) ValueOf(v quad.Value) graph.Value {
	if v == nil {
		return nil
	}
	var h ValueHash
	quad.HashTo(v, h[:])
	return h
}
func (qs *QuadStore) NameOf(v graph.Value) quad.Value {
	if v == nil {
		return nil
	}
	h := v.(ValueHash)
	key := qs.keyValue(h)
	resp, err := qs.etc.Get(context.TODO(), key)
	if err != nil {
		clog.Errorf("error getting node value: %v", err)
		return nil
	} else if len(resp.Kvs) == 0 || resp.Kvs[0] == nil {
		return nil // not found
	}
	var pv proto.Value
	if err = pv.Unmarshal(resp.Kvs[0].Value); err != nil {
		clog.Errorf("error decoding node value: %v", err)
		return nil
	}
	return pv.ToNative()
}
func cmpExists(key string) clientv3.Cmp {
	return clientv3.Compare(clientv3.Version(key), ">", int64(0))
}
func cmpNotExists(key string) clientv3.Cmp {
	return clientv3.Compare(clientv3.Version(key), "=", int64(0))
}
func (qs *QuadStore) Quad(v graph.Value) quad.Quad {
	if v == nil {
		return quad.Quad{}
	}
	index := indSPO
	h := v.(QuadHash)
	key := qs.keyQuadHash(h, index)
	ops := make([]clientv3.Op, 0, 4)
	for _, dir := range index {
		ops = append(ops, clientv3.OpGet(qs.keyValue(h.Get(dir))))
	}
	resp, err := qs.etc.Txn(context.TODO()).If(
		cmpExists(key),
	).Then(
		ops...,
	).Commit()
	if err != nil {
		clog.Errorf("error getting quad: %v", err)
		return quad.Quad{}
	} else if !resp.Succeeded {
		return quad.Quad{} // not found
	}
	var q quad.Quad
	for i, resp := range resp.Responses {
		r := resp.GetResponseRange()
		if len(r.Kvs) == 0 || r.Kvs[0] == nil {
			continue
		}
		kv := r.Kvs[0]
		v, err := proto.UnmarshalValue(kv.Value)
		if err != nil {
			clog.Errorf("error unmarshaling quad value: %v", err)
			return q
		}
		switch index[i] { // in sync with tx.Then
		case quad.Subject:
			q.Subject = v
		case quad.Predicate:
			q.Predicate = v
		case quad.Object:
			q.Object = v
		case quad.Label:
			q.Label = v
		}
	}
	return q
}
func (qs *QuadStore) Size() int64 {
	pref, _ := qs.prefQuad(indSPO, 0)
	resp, err := qs.etc.Get(
		context.TODO(), string(pref),
		clientv3.WithPrefix(), clientv3.WithCountOnly(),
	)
	if err != nil {
		clog.Errorf("error counting quads: %v", err)
		return 0
	}
	return resp.Count
}
func (qs *QuadStore) Horizon() graph.PrimaryKey {
	resp, err := qs.etc.Get(
		context.TODO(), qs.keyDBVersion(),
		clientv3.WithCountOnly(),
	)
	if err != nil {
		clog.Errorf("error getting horizon: %v", err)
		return graph.PrimaryKey{}
	}
	// TODO: store horizon separately?
	return graph.NewSequentialKey(resp.Header.Revision)
}
func (qs *QuadStore) QuadDirection(id graph.Value, d quad.Direction) graph.Value {
	return id.(QuadHash).Get(d)
}
func (qs *QuadStore) FixedIterator() graph.FixedIterator {
	return iterator.NewFixed(func(a, b graph.Value) bool {
		atok := a.(ValueHash)
		btok := b.(ValueHash)
		return atok == btok
	})
}
func (qs *QuadStore) ApplyDeltas(deltas []graph.Delta, ignoreOpts graph.IgnoreOpts) error {
	ctx := context.TODO()
	var (
		ifs  []clientv3.Cmp
		ops  []clientv3.Op
		seen = make(map[ValueHash]struct{})
	)
	var page = 100
	for i := 0; i < len(deltas); i += page {
		ifs = ifs[:0]
		ops = ops[:0]
		batch := deltas[i:]
		if len(batch) > page {
			batch = batch[:page]
		}
		for _, d := range batch {
			if d.Action == graph.Add {
				for _, dir := range indSPO[:] {
					if v := d.Quad.Get(dir); v != nil {
						var h ValueHash
						quad.HashTo(v, h[:])
						if _, ok := seen[h]; !ok {
							seen[h] = struct{}{}
							data, err := proto.MarshalValue(v)
							if err != nil {
								return fmt.Errorf("cannot marshal value: %v", err)
							}
							ops = append(ops,
								clientv3.OpPut(qs.keyValue(h), string(data)),
							)
						}
					}
				}
				keySPO := qs.keyQuad(d.Quad, indSPO)
				if !ignoreOpts.IgnoreDup {
					ifs = append(ifs, cmpNotExists(keySPO))
				}
				ops = append(ops,
					clientv3.OpPut(keySPO, ""),
					clientv3.OpPut(qs.keyQuad(d.Quad, indOSP), ""),
					clientv3.OpPut(qs.keyQuad(d.Quad, indPOS), ""),
					clientv3.OpPut(qs.keyQuad(d.Quad, indCPS), ""),
				)
			} else if d.Action == graph.Delete {
				// FIXME: remove values
				keySPO := qs.keyQuad(d.Quad, indSPO)
				if !ignoreOpts.IgnoreMissing {
					ifs = append(ifs, cmpExists(keySPO))
				}
				ops = append(ops,
					clientv3.OpDelete(keySPO),
					clientv3.OpDelete(qs.keyQuad(d.Quad, indOSP)),
					clientv3.OpDelete(qs.keyQuad(d.Quad, indPOS)),
					clientv3.OpDelete(qs.keyQuad(d.Quad, indCPS)),
				)
			} else {
				return &graph.DeltaError{Delta: d, Err: graph.ErrInvalidAction}
			}
		}
		resp, err := qs.etc.Txn(ctx).If(ifs...).Then(ops...).Commit()
		if err != nil {
			if e, ok := err.(rpctypes.EtcdError); ok {
				if e.Code() == codes.InvalidArgument {
					// too many operations in txn request
					if page < 100 {
						page = page * 4 / 5
					} else {
						page /= 2
					}
					if page == 0 {
						page = 1
					}
					i -= page // will be incremented by loop
					continue
				}
			}
			return err
		} else if !resp.Succeeded {
			// one of checks failed, find out which one
			ops = ops[:0]
			var ind []int
			for i, d := range batch {
				if (d.Action == graph.Add && !ignoreOpts.IgnoreDup) ||
					(d.Action == graph.Delete && !ignoreOpts.IgnoreMissing) {
					keySPO := qs.keyQuad(d.Quad, indSPO)
					ops = append(ops, clientv3.OpGet(keySPO,
						clientv3.WithRev(resp.Header.Revision),
						clientv3.WithCountOnly(),
					))
					ind = append(ind, i)
				}
			}
			if len(ops) > 0 {
				resp, err = qs.etc.Txn(ctx).Then(ops...).Commit()
				if err != nil {
					return err
				}
				for i, r := range resp.Responses {
					rng := r.GetResponseRange()
					d := batch[ind[i]]
					if d.Action == graph.Add && rng.Count != 0 {
						return &graph.DeltaError{Delta: d, Err: graph.ErrQuadExists}
					} else if d.Action == graph.Delete && rng.Count == 0 {
						return &graph.DeltaError{Delta: d, Err: graph.ErrQuadNotExist}
					}
				}
			}
			return fmt.Errorf("tx failed")
		}
	}
	return nil
}

func (qs *QuadStore) NodesAllIterator() graph.Iterator {
	return NewAllIterator(qs, true, 0)
}

func (qs *QuadStore) QuadsAllIterator() graph.Iterator {
	return NewAllIterator(qs, false, 0)
}
func (qs *QuadStore) QuadIterator(d quad.Direction, v graph.Value) graph.Iterator {
	return NewQuadIterator(qs, d, v)
}
