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

import (
	"github.com/google/cayley/graph"
	"github.com/google/cayley/quad"
)

// store is a mocked version of the QuadStore interface, for use in tests.
type store struct {
	data []string
	iter graph.Iterator
}

func (qs *store) ValueOf(s string) graph.Value {
	for i, v := range qs.data {
		if s == v {
			return i
		}
	}
	return nil
}

func (qs *store) ApplyDeltas([]graph.Delta) error { return nil }

func (qs *store) Quad(graph.Value) quad.Quad { return quad.Quad{} }

func (qs *store) QuadIterator(d quad.Direction, i graph.Value) graph.Iterator {
	return qs.iter
}

func (qs *store) NodesAllIterator() graph.Iterator { return &Null{} }

func (qs *store) QuadsAllIterator() graph.Iterator { return &Null{} }

func (qs *store) NameOf(v graph.Value) string {
	i := v.(int)
	if i < 0 || i >= len(qs.data) {
		return ""
	}
	return qs.data[i]
}

func (qs *store) Size() int64 { return 0 }

func (qs *store) Horizon() int64 { return 0 }

func (qs *store) DebugPrint() {}

func (qs *store) OptimizeIterator(it graph.Iterator) (graph.Iterator, bool) {
	return &Null{}, false
}

func (qs *store) FixedIterator() graph.FixedIterator {
	return NewFixed(Identity)
}

func (qs *store) Close() {}

func (qs *store) QuadDirection(graph.Value, quad.Direction) graph.Value { return 0 }

func (qs *store) RemoveQuad(t quad.Quad) {}
