package kv

import (
	"bytes"
	"context"
	"errors"
	"fmt"
)

var (
	ErrNotFound     = errors.New("kv: not found")
	ErrNoBucket     = errors.New("kv: bucket is missing")
	ErrBucketExists = errors.New("kv: bucket already exists")
)

type Tx interface {
	Commit() error
	Rollback() error
}

type Bucket interface {
	Get(k []byte) ([]byte, error)
	Put(k, v []byte) error
	Del(k []byte) error
	Scan(pref []byte) KVIterator
}

type KVIterator interface {
	Next(ctx context.Context) bool
	Err() error
	Close() error
	Key() []byte
	Val() []byte
}

type Op int

const (
	OpGet = Op(iota)
	OpCreate
	OpUpsert
)

type BucketTx interface {
	Tx
	Bucket(name []byte, op Op) (Bucket, error)
}

type FlatTx interface {
	Tx
	Bucket
}

type Base interface {
	Type() string
	Close() error
}

type BucketKV interface {
	Base
	Tx(update bool) (BucketTx, error)
}

type FlatKV interface {
	Base
	Tx(update bool) (FlatTx, error)
}

func Update(kv BucketKV, update func(tx BucketTx) error) error {
	tx, err := kv.Tx(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = update(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func View(kv BucketKV, view func(tx BucketTx) error) error {
	tx, err := kv.Tx(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	err = view(tx)
	if err == nil {
		err = tx.Rollback()
	}
	return err
}

func Each(ctx context.Context, b Bucket, pref []byte, fnc func(k, v []byte) error) error {
	it := b.Scan(pref)
	defer it.Close()
	for it.Next(ctx) {
		if err := fnc(it.Key(), it.Val()); err != nil {
			return err
		}
	}
	return it.Err()
}

var _ BucketKV = (*flatKV)(nil)

func FromFlat(flat FlatKV) BucketKV {
	return &flatKV{flat: flat}
}

type flatKV struct {
	flat FlatKV
}

func (kv *flatKV) Type() string { return kv.flat.Type() }
func (kv *flatKV) Close() error { return kv.flat.Close() }
func (kv *flatKV) Tx(update bool) (BucketTx, error) {
	tx, err := kv.flat.Tx(update)
	if err != nil {
		return nil, err
	}
	return &flatTx{kv: kv.flat, tx: tx, ro: !update}, nil
}

type flatTx struct {
	kv FlatKV
	tx FlatTx
	ro bool
	// TODO: map[[]byte]*flatBucket
}

func (v *flatTx) Commit() error {
	return v.tx.Commit()
}
func (v *flatTx) Rollback() error {
	return v.tx.Rollback()
}

const bucketSep = '/'

func (v *flatTx) bucketPref(name []byte) []byte {
	pref := make([]byte, len(name)+1)
	n := copy(pref, name)
	pref[n] = bucketSep
	return pref
}
func (v *flatTx) Bucket(name []byte, op Op) (Bucket, error) {
	if v.ro && op != OpGet {
		return nil, fmt.Errorf("create bucket on ro tx")
	}
	pref := v.bucketPref(name)
	_, err := v.tx.Get(pref)
	if err == ErrNotFound {
		if op == OpGet {
			return nil, ErrNoBucket
		}
		if err := v.tx.Put(pref, []byte{0}); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	} else {
		if op == OpCreate {
			return nil, ErrBucketExists
		}
	}
	return &flatBucket{flatTx: v, pref: pref}, nil
}

type flatBucket struct {
	*flatTx
	pref []byte
}

func (b *flatBucket) key(k []byte) []byte {
	key := make([]byte, len(b.pref)+len(k))
	n := copy(key, b.pref)
	copy(key[n:], k)
	return key
}
func (b *flatBucket) Get(k []byte) ([]byte, error) {
	return b.tx.Get(b.key(k))
}
func (b *flatBucket) Put(k, v []byte) error {
	if b.ro {
		return fmt.Errorf("put in ro tx")
	}
	return b.tx.Put(b.key(k), v)
}
func (b *flatBucket) Del(k []byte) error {
	if b.ro {
		return fmt.Errorf("del in ro tx")
	}
	return b.tx.Del(b.key(k))
}

type prefIter struct {
	KVIterator
	trim []byte
}

func (it *prefIter) Key() []byte {
	return bytes.TrimPrefix(it.KVIterator.Key(), it.trim)
}
func (b *flatBucket) Scan(pref []byte) KVIterator {
	pref = b.key(pref)
	return &prefIter{KVIterator: b.tx.Scan(pref), trim: b.pref}
}
