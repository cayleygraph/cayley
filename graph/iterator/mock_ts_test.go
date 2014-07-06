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

package iterator

// A quickly mocked version of the TripleStore interface, for use in tests.
// Can better used Mock.Called but will fill in as needed.

import "github.com/google/cayley/graph"

type store struct {
	data []string
	iter graph.Iterator
}

func (ts *store) ValueOf(s string) graph.Value {
	for i, v := range ts.data {
		if s == v {
			return i
		}
	}
	return nil
}

func (ts *store) AddTriple(*graph.Triple) {}

func (ts *store) AddTripleSet([]*graph.Triple) {}

func (ts *store) Triple(graph.Value) *graph.Triple { return &graph.Triple{} }

func (ts *store) TripleIterator(d graph.Direction, i graph.Value) graph.Iterator {
	return ts.iter
}

func (ts *store) NodesAllIterator() graph.Iterator { return &Null{} }

func (ts *store) TriplesAllIterator() graph.Iterator { return &Null{} }

func (ts *store) NameOf(v graph.Value) string {
	i := v.(int)
	if i < 0 || i >= len(ts.data) {
		return ""
	}
	return ts.data[i]
}

func (ts *store) Size() int64 { return 0 }

func (ts *store) DebugPrint() {}

func (ts *store) OptimizeIterator(it graph.Iterator) (graph.Iterator, bool) {
	return &Null{}, false
}

func (ts *store) FixedIterator() graph.FixedIterator {
	return NewFixedIteratorWithCompare(BasicEquality)
}

func (ts *store) Close() {}

func (ts *store) TripleDirection(graph.Value, graph.Direction) graph.Value { return 0 }

func (ts *store) RemoveTriple(t *graph.Triple) {}
