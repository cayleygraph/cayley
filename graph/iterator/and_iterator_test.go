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
	"errors"
	"testing"

	"github.com/google/cayley/graph"
)

// Make sure that tags work on the And.
func TestTag(t *testing.T) {
	qs := &store{
		data: []string{},
		iter: NewFixed(Identity),
	}
	fix1 := NewFixed(Identity)
	fix1.Add(234)
	fix1.Tagger().Add("foo")
	and := NewAnd(qs)
	and.AddSubIterator(fix1)
	and.Tagger().Add("bar")
	out := fix1.Tagger().Tags()
	if len(out) != 1 {
		t.Errorf("Expected length 1, got %d", len(out))
	}
	if out[0] != "foo" {
		t.Errorf("Cannot get tag back, got %s", out[0])
	}

	if !and.Next() {
		t.Errorf("And did not next")
	}
	val := and.Result()
	if val != 234 {
		t.Errorf("Unexpected value")
	}
	tags := make(map[string]graph.Value)
	and.TagResults(tags)
	if tags["bar"] != 234 {
		t.Errorf("no bar tag")
	}
	if tags["foo"] != 234 {
		t.Errorf("no foo tag")
	}
}

// Do a simple itersection of fixed values.
func TestAndAndFixedIterators(t *testing.T) {
	qs := &store{
		data: []string{},
		iter: NewFixed(Identity),
	}
	fix1 := NewFixed(Identity)
	fix1.Add(1)
	fix1.Add(2)
	fix1.Add(3)
	fix1.Add(4)
	fix2 := NewFixed(Identity)
	fix2.Add(3)
	fix2.Add(4)
	fix2.Add(5)
	and := NewAnd(qs)
	and.AddSubIterator(fix1)
	and.AddSubIterator(fix2)
	// Should be as big as smallest subiterator
	size, accurate := and.Size()
	if size != 3 {
		t.Error("Incorrect size")
	}
	if !accurate {
		t.Error("not accurate")
	}

	if !and.Next() || and.Result() != 3 {
		t.Error("Incorrect first value")
	}

	if !and.Next() || and.Result() != 4 {
		t.Error("Incorrect second value")
	}

	if and.Next() {
		t.Error("Too many values")
	}

}

// If there's no intersection, the size should still report the same,
// but there should be nothing to Next()
func TestNonOverlappingFixedIterators(t *testing.T) {
	qs := &store{
		data: []string{},
		iter: NewFixed(Identity),
	}
	fix1 := NewFixed(Identity)
	fix1.Add(1)
	fix1.Add(2)
	fix1.Add(3)
	fix1.Add(4)
	fix2 := NewFixed(Identity)
	fix2.Add(5)
	fix2.Add(6)
	fix2.Add(7)
	and := NewAnd(qs)
	and.AddSubIterator(fix1)
	and.AddSubIterator(fix2)
	// Should be as big as smallest subiterator
	size, accurate := and.Size()
	if size != 3 {
		t.Error("Incorrect size")
	}
	if !accurate {
		t.Error("not accurate")
	}

	if and.Next() {
		t.Error("Too many values")
	}

}

func TestAllIterators(t *testing.T) {
	qs := &store{
		data: []string{},
		iter: NewFixed(Identity),
	}
	all1 := NewInt64(1, 5)
	all2 := NewInt64(4, 10)
	and := NewAnd(qs)
	and.AddSubIterator(all2)
	and.AddSubIterator(all1)

	if !and.Next() || and.Result() != int64(4) {
		t.Error("Incorrect first value")
	}

	if !and.Next() || and.Result() != int64(5) {
		t.Error("Incorrect second value")
	}

	if and.Next() {
		t.Error("Too many values")
	}
}

func TestAndIteratorErr(t *testing.T) {
	qs := &store{
		data: []string{},
		iter: NewFixed(Identity),
	}
	wantErr := errors.New("unique")
	allErr := newTestIterator(false, wantErr)

	and := NewAnd(qs)
	and.AddSubIterator(allErr)
	and.AddSubIterator(NewInt64(1, 5))

	if and.Next() != false {
		t.Errorf("And iterator did not pass through initial 'false'")
	}
	if and.Err() != wantErr {
		t.Errorf("And iterator did not pass through underlying Err")
	}
}
