package etcd

import (
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"golang.org/x/net/context"
)

func NewIterator(etc *clientv3.Client, pref string, rev int64, opts ...clientv3.OpOption) *Iterator {
	if pref == "" {
		panic("prefix is empty")
	}
	return &Iterator{
		etc: etc, pref: pref, rev: rev, opts: opts,
		size: -1,
	}
}

type Iterator struct {
	etc  *clientv3.Client
	pref string
	rev  int64
	opts []clientv3.OpOption

	resp   *clientv3.GetResponse
	offset int
	err    error
	result *mvccpb.KeyValue
	size   int64

	requests int
}

func (it *Iterator) Result() *mvccpb.KeyValue { return it.result }
func (it *Iterator) Err() error               { return it.err }
func (it *Iterator) Reset() {
	it.result = nil
	it.err = nil
	it.resp = nil
	it.offset = 0
	it.size = -1
}
func (it *Iterator) Clone() *Iterator {
	return NewIterator(it.etc, it.pref, it.rev, it.opts...)
}
func getNext(key []byte, pref bool) string {
	end := make([]byte, len(key))
	copy(end, key)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i] = end[i] + 1
			if pref {
				end = end[:i+1]
			}
			return string(end)
		}
	}
	// next prefix does not exist (e.g., 0xffff);
	// default to WithFromKey policy
	return "\x00"
}
func (it *Iterator) Next() bool {
	if it.err != nil {
		return false
	}
	if it.resp != nil && it.offset+1 < len(it.resp.Kvs) {
		it.offset++
		it.result = it.resp.Kvs[it.offset]
		return true
	}
	ctx := context.TODO()
	var (
		opts []clientv3.OpOption
		key  string
	)
	if it.resp == nil {
		// first iteration
		opts = append(opts,
			clientv3.WithPrefix(),
			clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
		)
		key = it.pref
	} else if !it.resp.More {
		return false
	} else {
		key = getNext(it.resp.Kvs[len(it.resp.Kvs)-1].Key, false)
		opts = append(opts, clientv3.WithRange(getNext([]byte(it.pref), true)))
	}
	if it.rev > 0 {
		opts = append(opts, clientv3.WithRev(it.rev))
	}
	if len(it.opts) > 0 {
		opts = append(opts, it.opts...)
	}
	it.requests++
	resp, err := it.etc.Get(ctx, key, opts...)
	if err != nil {
		it.err = err
		return false
	}
	it.resp = resp
	if it.rev <= 0 { // fix revision
		it.rev = it.resp.Header.Revision
	}
	it.offset = 0
	if len(it.resp.Kvs) == 0 {
		it.result = nil
		return false
	}
	it.result = it.resp.Kvs[it.offset]
	return true
}
func (it *Iterator) Size() (int64, bool) {
	if it.size >= 0 {
		return it.size, true
	}
	ctx := context.TODO()
	opts := []clientv3.OpOption{
		clientv3.WithPrefix(),
	}
	if it.rev > 0 {
		opts = append(opts, clientv3.WithRev(it.rev))
	}
	if len(it.opts) > 0 {
		opts = append(opts, it.opts...)
	}
	it.requests++
	resp, err := it.etc.Get(ctx, it.pref, opts...)
	if err != nil {
		it.err = err
		return 0, false
	}
	if it.rev <= 0 { // fix revision
		it.rev = resp.Header.Revision
	}
	it.size = resp.Count
	return it.size, true
}
func (it *Iterator) Contains(key string) bool {
	ctx := context.TODO()
	opts := []clientv3.OpOption{
		clientv3.WithCountOnly(),
	}
	if it.rev > 0 {
		opts = append(opts, clientv3.WithRev(it.rev))
	}
	resp, err := it.etc.Get(ctx, key, opts...)
	if err != nil {
		it.err = err
		return false
	}
	if it.rev <= 0 { // fix revision
		it.rev = resp.Header.Revision
	}
	return resp.Count > 0
}
