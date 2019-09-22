package kv

import (
	"context"

	"github.com/hidal-go/hidalgo/kv"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	mApplyBatch = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "cayley_apply_deltas_batch",
		Help: "Number of quads in a buffer for ApplyDeltas or WriteQuads.",
	})
	mApplySeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "cayley_apply_deltas_seconds",
		Help: "Time to write a buffer in ApplyDeltas or WriteQuads.",
	})

	mNodesNew = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cayley_kv_nodes_new_count",
		Help: "Number new nodes created.",
	})
	mNodesUpd = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cayley_kv_nodes_upd_count",
		Help: "Number of node refcount updates.",
	})
	mNodesDel = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cayley_kv_nodes_del_count",
		Help: "Number of node deleted.",
	})

	mQuadsBloomHit = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cayley_kv_quads_bloom_hits",
		Help: "Number of times the quad bloom filter returned a negative result.",
	})
	mQuadsBloomMiss = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cayley_kv_quads_bloom_miss",
		Help: "Number of times the quad bloom filter returned a positive result.",
	})

	mPrimitiveFetch = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cayley_kv_primitive_fetch",
		Help: "Number of primitives fetched from KV.",
	})
	mPrimitiveFetchMiss = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cayley_kv_primitive_fetch_miss",
		Help: "Number of primitives that were not found in KV.",
	})
	mPrimitiveAppend = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cayley_kv_primitive_append",
		Help: "Number of primitives appended to log.",
	})

	mIndexWriteBufferEntries = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cayley_kv_index_buffer_entries",
		Help: "Number of entries in the index write buffer.",
	}, []string{"index"})
	mIndexWriteBufferFlushBatch = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "cayley_kv_index_buffer_flush_batch",
		Help: "Number of entries in the batch for flushing index entries.",
	}, []string{"index"})
	mIndexEntrySizeBytes = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "cayley_kv_index_entry_size_bytes",
		Help: "Size of a single index entry.",
	}, []string{"index"})

	mKVGet = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cayley_kv_get_count",
		Help: "Number of get KV calls.",
	})
	mKVGetMiss = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cayley_kv_get_miss",
		Help: "Number of get KV calls that found no value.",
	})
	mKVGetSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "cayley_kv_get_size",
		Help: "Size of values returned from KV.",
	})
	mKVPut = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cayley_kv_put_count",
		Help: "Number of put KV calls.",
	})
	mKVPutSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "cayley_kv_put_size",
		Help: "Size of values put to KV.",
	})
	mKVDel = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cayley_kv_del_count",
		Help: "Number of del KV calls.",
	})
	mKVScan = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cayley_kv_scan_count",
		Help: "Number of scan KV calls.",
	})
	mKVCommit = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cayley_kv_commit",
		Help: "Number of KV commits.",
	})
	mKVCommitSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "cayley_kv_commit_seconds",
		Help: "Time to commit to KV.",
	})
	mKVRollback = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cayley_kv_rollback",
		Help: "Number of KV rollbacks.",
	})
)

func wrapTx(tx kv.Tx) kv.Tx {
	return &mTx{tx: tx}
}

type mTx struct {
	tx   kv.Tx
	done bool
}

func (tx *mTx) Commit(ctx context.Context) error {
	if !tx.done {
		tx.done = true
		mKVCommit.Inc()
		defer prometheus.NewTimer(mKVCommitSeconds).ObserveDuration()
	}
	return tx.tx.Commit(ctx)
}

func (tx *mTx) Close() error {
	if !tx.done {
		tx.done = true
		mKVRollback.Inc()
	}
	return tx.tx.Close()
}

func (tx *mTx) Get(ctx context.Context, key kv.Key) (kv.Value, error) {
	mKVGet.Inc()
	val, err := tx.tx.Get(ctx, key)
	if err == kv.ErrNotFound {
		mKVGetMiss.Inc()
	} else if err == nil {
		mKVGetSize.Observe(float64(len(val)))
	}
	return val, err
}

func (tx *mTx) GetBatch(ctx context.Context, keys []kv.Key) ([]kv.Value, error) {
	mKVGet.Add(float64(len(keys)))
	vals, err := tx.tx.GetBatch(ctx, keys)
	for _, v := range vals {
		if v == nil {
			mKVGetMiss.Inc()
		} else {
			mKVGetSize.Observe(float64(len(v)))
		}
	}
	return vals, err
}

func (tx *mTx) Put(k kv.Key, v kv.Value) error {
	mKVPut.Inc()
	mKVPutSize.Observe(float64(len(v)))
	return tx.tx.Put(k, v)
}

func (tx *mTx) Del(k kv.Key) error {
	mKVDel.Inc()
	return tx.tx.Del(k)
}

func (tx *mTx) Scan(pref kv.Key) kv.Iterator {
	mKVScan.Inc()
	return tx.tx.Scan(pref)
}
