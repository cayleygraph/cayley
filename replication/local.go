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

package replication

import (
	"sync"

	"github.com/google/cayley/graph"
)

type Single struct {
	lastID int64
	ts     graph.TripleStore
	mut    sync.Mutex
}

func NewSingleReplication(ts graph.TripleStore, opts graph.Options) (graph.Replication, error) {
	rep := &Single{lastID: ts.Horizon(), ts: ts}
	ts.SetReplication(rep)
	return rep, nil
}

func (s *Single) AcquireNextIds(size int64) (start int64, end int64) {
	s.mut.Lock()
	defer s.mut.Unlock()
	start = s.lastID + 1
	end = s.lastID + size
	s.lastID += size
	return
}

func (s *Single) GetLastID() int64 {
	return s.lastID
}

func (s *Single) Replicate([]*graph.Transaction) {
	// Noop, single-machines don't replicate out anywhere.
}
func (s *Single) RequestTransactionRange(int64, int64) {
	// Single machines also can't catch up.
}

func init() {
	graph.RegisterReplication("local", NewSingleReplication)
}
