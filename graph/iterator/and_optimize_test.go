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
	"reflect"
	"sort"
	"testing"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/graphmock"
	. "github.com/cayleygraph/cayley/graph/iterator"
)

func TestIteratorPromotion(t *testing.T) {
	qs := &graphmock.Oldstore{
		Data: []string{},
		Iter: NewFixed(Identity),
	}
	all := NewInt64(1, 3, true)
	fixed := NewFixed(Identity, Int64Node(3))
	a := NewAnd(qs, all, fixed)
	all.Tagger().Add("a")
	fixed.Tagger().Add("b")
	a.Tagger().Add("c")
	newIt, changed := a.Optimize()
	if !changed {
		t.Error("Iterator didn't optimize")
	}
	if newIt.Type() != graph.Fixed {
		t.Error("Expected fixed iterator")
	}
	tagsExpected := []string{"a", "b", "c"}
	tags := newIt.Tagger().Tags()
	sort.Strings(tags)
	if !reflect.DeepEqual(tags, tagsExpected) {
		t.Fatal("Tags don't match")
	}
}

func TestNullIteratorAnd(t *testing.T) {
	qs := &graphmock.Oldstore{
		Data: []string{},
		Iter: NewFixed(Identity),
	}
	all := NewInt64(1, 3, true)
	null := NewNull()
	a := NewAnd(qs, all, null)
	newIt, changed := a.Optimize()
	if !changed {
		t.Error("Didn't change")
	}
	if newIt.Type() != graph.Null {
		t.Error("Expected null iterator, got ", newIt.Type())
	}
}

func TestAllPromotion(t *testing.T) {
	qs := &graphmock.Oldstore{
		Data: []string{},
		Iter: NewFixed(Identity),
	}
	all := NewInt64(100, 300, true)
	all.Tagger().Add("good")
	all2 := NewInt64(1, 30000, true)
	all2.Tagger().Add("slow")
	a := NewAnd(qs)
	// Make all2 the default iterator
	a.AddSubIterator(all2)
	a.AddSubIterator(all)

	newIt, changed := a.Optimize()
	if !changed {
		t.Error("Expected new iterator")
	}
	if newIt.Type() != graph.All {
		t.Error("Should have promoted the All iterator")
	}
	expectedTags := []string{"good", "slow"}
	tagsOut := make([]string, 0)
	for _, x := range newIt.Tagger().Tags() {
		tagsOut = append(tagsOut, x)
	}
	sort.Strings(tagsOut)
	if !reflect.DeepEqual(expectedTags, tagsOut) {
		t.Fatalf("Tags don't match: expected: %#v, got: %#v", expectedTags, tagsOut)
	}
}

func TestReorderWithTag(t *testing.T) {
	qs := &graphmock.Oldstore{
		Data: []string{},
		Iter: NewFixed(Identity),
	}
	all := NewFixed(Identity, Int64Node(3))
	all.Tagger().Add("good")
	all2 := NewFixed(Identity,
		Int64Node(3),
		Int64Node(4),
		Int64Node(5),
		Int64Node(6),
	)
	all2.Tagger().Add("slow")
	a := NewAnd(qs)
	// Make all2 the default iterator
	a.AddSubIterator(all2)
	a.AddSubIterator(all)

	newIt, changed := a.Optimize()
	if !changed {
		t.Error("Expected new iterator")
	}
	expectedTags := []string{"good", "slow"}
	tagsOut := make([]string, 0)
	for _, sub := range newIt.SubIterators() {
		for _, x := range sub.Tagger().Tags() {
			tagsOut = append(tagsOut, x)
		}
	}
	for _, x := range newIt.Tagger().Tags() {
		tagsOut = append(tagsOut, x)
	}
	sort.Strings(tagsOut)
	if !reflect.DeepEqual(expectedTags, tagsOut) {
		t.Fatalf("Tags don't match: expected: %#v, got: %#v", expectedTags, tagsOut)
	}
}

func TestAndStatistics(t *testing.T) {
	qs := &graphmock.Oldstore{
		Data: []string{},
		Iter: NewFixed(Identity),
	}
	all := NewInt64(100, 300, true)
	all.Tagger().Add("good")
	all2 := NewInt64(1, 30000, true)
	all2.Tagger().Add("slow")
	a := NewAnd(qs)
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
