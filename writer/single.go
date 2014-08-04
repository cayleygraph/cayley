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

package writer

import (
	"sync"
	"time"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/quad"
)

type Single struct {
	nextID int64
	ts     graph.TripleStore
	mut    sync.Mutex
}

func NewSingleReplication(ts graph.TripleStore, opts graph.Options) (graph.QuadWriter, error) {
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

func (s *Single) AddQuad(t *quad.Quad) error {
	trans := make([]*graph.Transaction, 1)
	trans[0] = &graph.Transaction{
		ID:        s.AcquireNextId(),
		Quad:      t,
		Action:    graph.Add,
		Timestamp: time.Now(),
	}
	return s.ts.ApplyTransactions(trans)
}

func (s *Single) AddQuadSet(set []*quad.Quad) error {
	trans := make([]*graph.Transaction, len(set))
	for i, t := range set {
		trans[i] = &graph.Transaction{
			ID:        s.AcquireNextId(),
			Quad:      t,
			Action:    graph.Add,
			Timestamp: time.Now(),
		}
	}
	s.ts.ApplyTransactions(trans)
	return nil
}

func (s *Single) RemoveQuad(t *graph.Quad) error {
	trans := make([]*graph.Transaction, 1)
	trans[0] = &graph.Transaction{
		ID:        s.AcquireNextId(),
		Triple:    t,
		Action:    graph.Delete,
		Timestamp: time.Now(),
	}
	return s.ts.ApplyTransactions(trans)
}

func init() {
	graph.RegisterWriter("single", NewSingleReplication)
}
