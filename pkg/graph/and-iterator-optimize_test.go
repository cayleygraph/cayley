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

// Tests relating to methods in and-iterator-optimize. Many are pretty simplistic, but
// nonetheless cover a lot of basic cases.

import (
	"reflect"
	"sort"
	"testing"
)

func TestIteratorPromotion(t *testing.T) {
	all := NewInt64AllIterator(1, 3)
	fixed := newFixedIterator()
	fixed.AddValue(3)
	a := NewAndIterator()
	a.AddSubIterator(all)
	a.AddSubIterator(fixed)
	all.AddTag("a")
	fixed.AddTag("b")
	a.AddTag("c")
	newIt, changed := a.Optimize()
	if !changed {
		t.Error("Iterator didn't optimize")
	}
	if newIt.Type() != "fixed" {
		t.Error("Expected fixed iterator")
	}
	tagsExpected := []string{"a", "b", "c"}
	tags := newIt.Tags()
	sort.Strings(tags)
	if !reflect.DeepEqual(tags, tagsExpected) {
		t.Fatal("Tags don't match")
	}
}

func TestNullIteratorAnd(t *testing.T) {
	all := NewInt64AllIterator(1, 3)
	null := NewNullIterator()
	a := NewAndIterator()
	a.AddSubIterator(all)
	a.AddSubIterator(null)
	newIt, changed := a.Optimize()
	if !changed {
		t.Error("Didn't change")
	}
	if newIt.Type() != "null" {
		t.Error("Expected null iterator, got ", newIt.Type())
	}
}

func TestReorderWithTag(t *testing.T) {
	all := NewInt64AllIterator(100, 300)
	all.AddTag("good")
	all2 := NewInt64AllIterator(1, 30000)
	all2.AddTag("slow")
	a := NewAndIterator()
	// Make all2 the default iterator
	a.AddSubIterator(all2)
	a.AddSubIterator(all)

	newIt, changed := a.Optimize()
	if !changed {
		t.Error("Expected new iterator")
	}
	expectedTags := []string{"good", "slow"}
	tagsOut := make([]string, 0)
	l := newIt.GetSubIterators()
	for e := l.Front(); e != nil; e = e.Next() {
		for _, x := range e.Value.(Iterator).Tags() {
			tagsOut = append(tagsOut, x)
		}
	}
	if !reflect.DeepEqual(expectedTags, tagsOut) {
		t.Fatal("Tags don't match")
	}
}

func TestAndStatistics(t *testing.T) {
	all := NewInt64AllIterator(100, 300)
	all.AddTag("good")
	all2 := NewInt64AllIterator(1, 30000)
	all2.AddTag("slow")
	a := NewAndIterator()
	// Make all2 the default iterator
	a.AddSubIterator(all2)
	a.AddSubIterator(all)
	stats1 := a.GetStats()
	newIt, changed := a.Optimize()
	if !changed {
		t.Error("Didn't optimize")
	}
	stats2 := newIt.GetStats()
	if stats2.NextCost > stats1.NextCost {
		t.Error("And didn't optimize. Next cost old ", stats1.NextCost, "and new ", stats2.NextCost)
	}
}
