// bolt2 package is moved to ./graph/kv/bolt. This file remains for backward compatibility.
package bolt2

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/kv"
	"github.com/cayleygraph/cayley/graph/kv/bolt"
)

const (
	Type = bolt.Type
)

func Create(path string, opt graph.Options) (kv.BucketKV, error) {
	return bolt.Create(path, opt)
}

func Open(path string, opt graph.Options) (kv.BucketKV, error) {
	return bolt.Open(path, opt)
}
