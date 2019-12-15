package kv

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/registry"
	"github.com/hidal-go/hidalgo/kv"
)

const SearchStoreName = "cayley"

func init() {
	registry.RegisterKVStore(SearchStoreName, NewSearchStore)
}

func NewSearchStore(mo store.MergeOperator, config map[string]interface{}) (store.KVStore, error) {
	kv := config["kv"].(kv.KV)
	return &SearchStore{kv: kv, mo: mo}, nil
}

type SearchStore struct {
	kv kv.KV
	mo store.MergeOperator
}

func (s *SearchStore) Writer() (store.KVWriter, error) {
	return &SearchWriter{kv: s.kv}, nil
}

func (s *SearchStore) Reader() (store.KVReader, error) {
	ctx := context.TODO()
	tx, err := s.kv.Tx(false)
	if err != nil {
		return nil, err
	}
	return &SearchReader{ctx: ctx, tx: tx}, nil
}

func (s *SearchStore) Close() error {
	return s.kv.Close()
}

type SearchReader struct {
	ctx context.Context
	tx  kv.Tx
}

// Get returns the value associated with the key
// If the key does not exist, nil is returned.
// The caller owns the bytes returned.
func (r *SearchReader) Get(key []byte) ([]byte, error) {
	value, err := r.tx.Get(r.ctx, kv.Key{key})
	if err != nil {
		return nil, err
	}
	return value, nil
}

// MultiGet retrieves multiple values in one call.
func (r *SearchReader) MultiGet(keys [][]byte) ([][]byte, error) {
	var values [][]byte
	for _, key := range keys {
		value, err := r.tx.Get(r.ctx, kv.Key{key})
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

func createSearchIterator(tx kv.Tx, prefix []byte) *SearchIterator {
	return &SearchIterator{subIt: tx.Scan(kv.Key{prefix})}
}

// PrefixIterator returns a KVIterator that will
// visit all K/V pairs with the provided prefix
func (r *SearchReader) PrefixIterator(prefix []byte) store.KVIterator {
	return createSearchIterator(r.tx, prefix)
}

// RangeIterator returns a KVIterator that will
// visit all K/V pairs >= start AND < end
func (r *SearchReader) RangeIterator(start, end []byte) store.KVIterator {
	it := createSearchIterator(r.tx, start)
	return &LimitedSearchIterator{SearchIterator: it, limit: end}
}

// Close closes the iterator
func (r *SearchReader) Close() error {
	return r.tx.Close()
}

type SearchIterator struct {
	subIt kv.Iterator
	ctx   context.Context
}

// Seek will advance the iterator to the specified key
func (it *SearchIterator) Seek(key []byte) {
	for it.subIt.Next(it.ctx) {
		if bytes.Compare(it.Key(), key) == 0 {
			break
		}
	}
}

// Next will advance the iterator to the next key
func (it *SearchIterator) Next() {
	it.subIt.Next(it.ctx)
}

// Key returns the key pointed to by the iterator
// The bytes returned are **ONLY** valid until the next call to Seek/Next/Close
// Continued use after that requires that they be copied.
func (it *SearchIterator) Key() []byte {
	var key []byte
	for _, part := range it.subIt.Key() {
		key = append(key, part...)
	}
	return key
}

// Value returns the value pointed to by the iterator
// The bytes returned are **ONLY** valid until the next call to Seek/Next/Close
// Continued use after that requires that they be copied.
func (it *SearchIterator) Value() []byte {
	return it.subIt.Val()
}

// Valid returns whether or not the iterator is in a valid state
func (it *SearchIterator) Valid() bool {
	return it.subIt.Err() != nil
}

// Current returns Key(),Value(),Valid() in a single operation
func (it *SearchIterator) Current() ([]byte, []byte, bool) {
	if !it.Valid() {
		return nil, nil, false
	}
	return it.Key(), it.Value(), true
}

// Close closes the iterator
func (it *SearchIterator) Close() error {
	return it.subIt.Close()
}

type LimitedSearchIterator struct {
	*SearchIterator
	limit []byte
	done  bool
}

// Next will advance the iterator to the next key
func (it *LimitedSearchIterator) Next() {
	if it.done {
		return
	}
	it.subIt.Next(it.ctx)
	if bytes.Compare(it.Key(), it.limit) == 0 {
		it.done = true
	}
}

type SearchWriter struct {
	kv kv.KV
}

// NewBatch returns a KVBatch for performing batch operations on this kvstore
func (w *SearchWriter) NewBatch() store.KVBatch {
	panic("Not implemented")
}

// NewBatchEx returns a KVBatch and an associated byte array
// that's pre-sized based on the KVBatchOptions.  The caller can
// use the returned byte array for keys and values associated with
// the batch.  Once the batch is either executed or closed, the
// associated byte array should no longer be accessed by the
// caller.
func (w *SearchWriter) NewBatchEx(options store.KVBatchOptions) ([]byte, store.KVBatch, error) {
	panic("Not implemented")
}

// ExecuteBatch will execute the KVBatch, the provided KVBatch **MUST** have
// been created by the same KVStore (though not necessarily the same KVWriter)
// Batch execution is atomic, either all the operations or none will be performed
func (w *SearchWriter) ExecuteBatch(batch store.KVBatch) error {
	panic("Not implemented")
}

// Close closes the writer
func (w *SearchWriter) Close() error {
	panic("Not implemented")
}

type SearchBatch struct{}

// Set updates the key with the specified value
// both key and value []byte may be reused as soon as this call returns
func (b *SearchBatch) Set(key, val []byte) {
	panic("Not implemented")
}

// Delete removes the specified key
// the key []byte may be reused as soon as this call returns
func (b *SearchBatch) Delete(key []byte) {
	panic("Not implemented")
}

// Merge merges old value with the new value at the specified key
// as prescribed by the KVStores merge operator
// both key and value []byte may be reused as soon as this call returns
func (b *SearchBatch) Merge(key, val []byte) {
	panic("Not implemented")
}

// Reset frees resources for this batch and allows reuse
func (b *SearchBatch) Reset() {
	panic("Not implemented")
}

// Close frees resources
func (b *SearchBatch) Close() error {
	panic("Not implemented")
}

// KVStoreStats is an optional interface that KVStores can implement
// if they're able to report any useful stats
type SearchStoreStats struct{}

// Stats returns a JSON serializable object representing stats for this KVStore
func (s *SearchStoreStats) Stats() json.Marshaler {
	panic("Not implemented")
}

func (s *SearchStoreStats) StatsMap() map[string]interface{} {
	panic("Not implemented")
}
