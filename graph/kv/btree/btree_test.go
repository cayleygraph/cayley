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

package btree

import (
	"testing"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/kv/kvtest"
	hkv "github.com/hidal-go/hidalgo/kv"
	"github.com/hidal-go/hidalgo/kv/kvdebug"
)

const debug = false

func makeBtree(t testing.TB) (hkv.KV, graph.Options, func()) {
	if debug {
		return makeBtreeDebug(t)
	}
	return New(), nil, func() {}
}

func makeBtreeDebug(t testing.TB) (hkv.KV, graph.Options, func()) {
	db := New()
	d := kvdebug.New(db)
	d.Log(true)
	return d, nil, func() {
		d.Close()
		t.Logf("kv stats: %+v", d.Stats())
	}
}

var conf = &kvtest.Config{
	AlwaysRunIntegration: true,
}

func TestBtree(t *testing.T) {
	kvtest.TestAll(t, makeBtree, conf)
}

func BenchmarkBtree(b *testing.B) {
	kvtest.BenchmarkAll(b, makeBtree, conf)
}
