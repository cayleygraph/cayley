package kv

import (
	"bytes"
	"errors"
	"fmt"
)

var (
	ErrNoBucket     = errors.New("kv: bucket is missing")
	ErrBucketExists = errors.New("kv: bucket already exists")
)

type Tx interface {
	Commit() error
	Rollback() error
}

type Bucket interface {
	Get(k []byte) []byte
	Put(k, v []byte) error
	ForEach(pref []byte, fnc func(k, v []byte) error) error
}

type BucketTx interface {
	Tx
	Bucket(name []byte) Bucket
	CreateBucket(name []byte, excl bool) (Bucket, error)
}

type FlatTx interface {
	Tx
	Bucket
}

type BucketKV interface {
	Type() string
	View() (BucketTx, error)
	Update() (BucketTx, error)
	Close() error
}

type FlatKV interface {
	Type() string
	View() (FlatTx, error)
	Update() (FlatTx, error)
	Close() error
}

func Update(kv BucketKV, update func(tx BucketTx) error) error {
	tx, err := kv.Update()
	if err != nil {
		return err
	}
	if err = update(tx); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

func View(kv BucketKV, view func(tx BucketTx) error) error {
	tx, err := kv.View()
	if err != nil {
		return err
	}
	err = view(tx)
	tx.Rollback()
	return err
}

var _ BucketKV = (*flatKV)(nil)

func FromFlat(flat FlatKV) BucketKV {
	return &flatKV{flat: flat}
}

type flatKV struct {
	flat FlatKV
}

func (kv *flatKV) Type() string { return kv.flat.Type() }
func (kv *flatKV) View() (BucketTx, error) {
	tx, err := kv.flat.View()
	if err != nil {
		return nil, err
	}
	return &flatTx{kv: kv.flat, tx: tx, ro: true}, nil
}
func (kv *flatKV) Update() (BucketTx, error) {
	tx, err := kv.flat.Update()
	if err != nil {
		return nil, err
	}
	return &flatTx{kv: kv.flat, tx: tx, ro: false}, nil
}
func (kv *flatKV) Close() error { return kv.flat.Close() }

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
func (v *flatTx) Bucket(name []byte) Bucket {
	pref := v.bucketPref(name)
	if v.tx.Get(pref) == nil {
		return nil
	}
	return &flatBucket{flatTx: v, pref: pref}
}
func (v *flatTx) CreateBucket(name []byte, excl bool) (Bucket, error) {
	if v.ro {
		return nil, fmt.Errorf("create bucket on ro tx")
	}
	pref := v.bucketPref(name)
	if excl && v.tx.Get(pref) != nil {
		return nil, ErrBucketExists
	}
	if err := v.tx.Put(pref, []byte{0}); err != nil {
		return nil, err
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
func (b *flatBucket) Get(k []byte) []byte {
	return b.tx.Get(b.key(k))
}
func (b *flatBucket) Put(k, v []byte) error {
	if b.ro {
		return fmt.Errorf("put in ro tx")
	}
	return b.tx.Put(b.key(k), v)
}
func (b *flatBucket) ForEach(pref []byte, fnc func(k, v []byte) error) error {
	pref = b.key(pref)
	return b.tx.ForEach(pref, func(k, v []byte) error {
		k = bytes.TrimPrefix(k, b.pref)
		return fnc(k, v)
	})
}
