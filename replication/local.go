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

package local

import (
	"sync"

	"github.com/google/cayley/graph"
)

type LocalReplication struct {
	lastId int64
	ts     graph.TripleStore
	mut    sync.Mutex
}

func NewLocalReplication(ts graph.TripleStore, opts graph.Options) (graph.Replication, error) {
	rep := &LocalReplication{lastId: ts.Horizon(), ts: ts}
	ts.SetReplication(rep)
	return rep, nil
}

func (l *LocalReplication) AcquireNextIds(size int64) (start int64, end int64) {
	l.mut.Lock()
	defer l.mut.Unlock()
	start = l.lastId + 1
	end = l.lastId + size
	l.lastId += size
	return
}

func (l *LocalReplication) GetLastId() int64 {
	return l.lastId
}

func (l *LocalReplication) Replicate([]*graph.Transaction) {
	// Noop, single-machines don't replicate out anywhere.
}
func (l *LocalReplication) RequestTransactionRange(int64, int64) {
	// Single machines also can't catch up.
}

func init() {
	graph.RegisterReplication("local", NewLocalReplication)
}
