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
	"testing"

	"github.com/google/cayley/graph"
)

func TestLinksTo(t *testing.T) {
	ts := new(TestTripleStore)
	tsFixed := newFixed()
	tsFixed.AddValue(2)
	ts.On("ValueOf", "cool").Return(1)
	ts.On("TripleIterator", graph.Object, 1).Return(tsFixed)
	fixed := newFixed()
	fixed.AddValue(ts.ValueOf("cool"))
	lto := NewLinksTo(ts, fixed, graph.Object)
	val, ok := lto.Next()
	if !ok {
		t.Error("At least one triple matches the fixed object")
	}
	if val != 2 {
		t.Errorf("Triple index 2, such as %s, should match %s", ts.Triple(2), ts.Triple(val))
	}
}
