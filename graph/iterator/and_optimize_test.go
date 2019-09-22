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

package iterator_test

// Tests relating to methods in and-iterator-optimize. Many are pretty simplistic, but
// nonetheless cover a lot of basic cases.

import (
	"testing"

	. "github.com/cayleygraph/cayley/graph/iterator"
)

func TestNullIteratorAnd(t *testing.T) {
	all := newInt64(1, 3, true)
	null := NewNull()
	a := NewAnd(all, null)
	newIt, changed := a.Optimize()
	if !changed {
		t.Error("Didn't change")
	}
	if _, ok := newIt.(*Null); !ok {
		t.Errorf("Expected null iterator, got %T", newIt)
	}
}

func TestReorderWithTag(t *testing.T) {
	all := NewFixed(Int64Node(3))
	all2 := NewFixed(
		Int64Node(3),
		Int64Node(4),
		Int64Node(5),
		Int64Node(6),
	)
	a := NewAnd()
	// Make all2 the default iterator
	a.AddSubIterator(all2)
	a.AddSubIterator(all)

	newIt, changed := a.Optimize()
	if !changed {
		t.Error("Expected new iterator")
	}
	newIt.Close()
}

func TestAndStatistics(t *testing.T) {
	all := newInt64(100, 300, true)
	all2 := newInt64(1, 30000, true)
	a := NewAnd()
	// Make all2 the default iterator
	a.AddSubIterator(all2)
	a.AddSubIterator(all)
	stats1 := a.Stats()
	newIt, changed := a.Optimize()
	if !changed {
		t.Error("Didn't optimize")
	}
	stats2 := newIt.Stats()
	if stats2.NextCost > stats1.NextCost {
		t.Error("And didn't optimize. Next cost old ", stats1.NextCost, "and new ", stats2.NextCost)
	}
}
