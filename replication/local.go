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
	nextID int64
	ts     graph.TripleStore
	mut    sync.Mutex
}

func NewSingleReplication(ts graph.TripleStore, opts graph.Options) (graph.TripleWriter, error) {
	rep := &Single{nextID: ts.Horizon(), ts: ts}
	if rep.nextID == -1 {
		rep.nextID = 1
	}
	return rep, nil
}

func (s *Single) AcquireNextId() int64 {
	s.mut.Lock()
	defer s.mut.Unlock()
	id := s.nextID
	s.nextID += 1
	return id
}

func AddTriple(*graph.Triple) error {
	return nil
}

func init() {
	graph.RegisterWriter("single", NewSingleReplication)
}
