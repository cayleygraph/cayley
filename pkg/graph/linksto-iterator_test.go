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

package graph

import (
	"testing"
)

func TestLinksTo(t *testing.T) {
	ts := new(TestTripleStore)
	tsFixed := newFixedIterator()
	tsFixed.AddValue(2)
	ts.On("GetIdFor", "cool").Return(1)
	ts.On("GetTripleIterator", "o", 1).Return(tsFixed)
	fixed := newFixedIterator()
	fixed.AddValue(ts.GetIdFor("cool"))
	lto := NewLinksToIterator(ts, fixed, "o")
	val, ok := lto.Next()
	if !ok {
		t.Error("At least one triple matches the fixed object")
	}
	if val != 2 {
		t.Errorf("Triple index 2, such as %s, should match %s", ts.GetTriple(2), ts.GetTriple(val))
	}
}
