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
	"reflect"
	"testing"

	"github.com/google/cayley/graph"
)

func iterated(it graph.Iterator) []int {
	var res []int
	for graph.Next(it) {
		res = append(res, it.Result().(int))
	}
	return res
}

func TestOrIteratorBasics(t *testing.T) {
	or := NewOr()
	f1 := NewFixed(Identity)
	f1.Add(1)
	f1.Add(2)
	f1.Add(3)
	f2 := NewFixed(Identity)
	f2.Add(3)
	f2.Add(9)
	f2.Add(20)
	f2.Add(21)
	or.AddSubIterator(f1)
	or.AddSubIterator(f2)

	if v, _ := or.Size(); v != 7 {
		t.Errorf("Unexpected iterator size, got:%d expected %d", v, 7)
	}

	expect := []int{1, 2, 3, 3, 9, 20, 21}
	for i := 0; i < 2; i++ {
		if got := iterated(or); !reflect.DeepEqual(got, expect) {
			t.Errorf("Failed to iterate Or correctly on repeat %d, got:%v expect:%v", i, got, expect)
		}
		or.Reset()
	}

	// Check that optimization works.
	optOr, _ := or.Optimize()
	if got := iterated(optOr); !reflect.DeepEqual(got, expect) {
		t.Errorf("Failed to iterate optimized Or correctly, got:%v expect:%v", got, expect)
	}

	for _, v := range []int{2, 3, 21} {
		if !or.Contains(v) {
			t.Errorf("Failed to correctly check %d as true", v)
		}
	}

	for _, v := range []int{22, 5, 0} {
		if or.Contains(v) {
			t.Errorf("Failed to correctly check %d as false", v)
		}
	}
}

func TestShortCircuitingOrBasics(t *testing.T) {
	var or *Or

	f1 := NewFixed(Identity)
	f1.Add(1)
	f1.Add(2)
	f1.Add(3)
	f2 := NewFixed(Identity)
	f2.Add(3)
	f2.Add(9)
	f2.Add(20)
	f2.Add(21)

	or = NewShortCircuitOr()
	or.AddSubIterator(f1)
	or.AddSubIterator(f2)
	size, exact := or.Size()
	if size != 4 {
		t.Errorf("Unexpected iterator size, got:%d expected %d", size, 4)
	}
	if !exact {
		t.Error("Size not exact.")
	}

	// It should extract the first iterators' numbers.
	or = NewShortCircuitOr()
	or.AddSubIterator(f1)
	or.AddSubIterator(f2)
	expect := []int{1, 2, 3}
	for i := 0; i < 2; i++ {
		if got := iterated(or); !reflect.DeepEqual(got, expect) {
			t.Errorf("Failed to iterate Or correctly on repeat %d, got:%v expect:%v", i, got, expect)
		}
		or.Reset()
	}

	// Check optimization works.
	optOr, _ := or.Optimize()
	if got := iterated(optOr); !reflect.DeepEqual(got, expect) {
		t.Errorf("Failed to iterate optimized Or correctly, got:%v expect:%v", got, expect)
	}

	// Check that numbers in either iterator exist.
	or = NewShortCircuitOr()
	or.AddSubIterator(f1)
	or.AddSubIterator(f2)
	for _, v := range []int{2, 3, 21} {
		if !or.Contains(v) {
			t.Errorf("Failed to correctly check %d as true", v)
		}
	}
	for _, v := range []int{22, 5, 0} {
		if or.Contains(v) {
			t.Errorf("Failed to correctly check %d as false", v)
		}
	}

	// Check that it pulls the second iterator's numbers if the first is empty.
	or = NewShortCircuitOr()
	or.AddSubIterator(NewFixed(Identity))
	or.AddSubIterator(f2)
	expect = []int{3, 9, 20, 21}
	for i := 0; i < 2; i++ {
		if got := iterated(or); !reflect.DeepEqual(got, expect) {
			t.Errorf("Failed to iterate Or correctly on repeat %d, got:%v expect:%v", i, got, expect)
		}
		or.Reset()
	}
	// Check optimization works.
	optOr, _ = or.Optimize()
	if got := iterated(optOr); !reflect.DeepEqual(got, expect) {
		t.Errorf("Failed to iterate optimized Or correctly, got:%v expect:%v", got, expect)
	}
}

func TestOrIteratorErr(t *testing.T) {
	wantErr := errors.New("unique")
	orErr := newTestIterator(false, wantErr)

	fix1 := NewFixed(Identity)
	fix1.Add(1)

	or := NewOr()
	or.AddSubIterator(fix1)
	or.AddSubIterator(orErr)
	or.AddSubIterator(NewInt64(1, 5))

	if !or.Next() {
		t.Errorf("Failed to iterate Or correctly")
	}
	if got := or.Result(); got != 1 {
		t.Errorf("Failed to iterate Or correctly, got:%v expect:1", got)
	}

	if or.Next() != false {
		t.Errorf("Or iterator did not pass through underlying 'false'")
	}
	if or.Err() != wantErr {
		t.Errorf("Or iterator did not pass through underlying Err")
	}
}

func TestShortCircuitOrIteratorErr(t *testing.T) {
	wantErr := errors.New("unique")
	orErr := newTestIterator(false, wantErr)

	or := NewOr()
	or.AddSubIterator(orErr)
	or.AddSubIterator(NewInt64(1, 5))

	if or.Next() != false {
		t.Errorf("Or iterator did not pass through underlying 'false'")
	}
	if or.Err() != wantErr {
		t.Errorf("Or iterator did not pass through underlying Err")
	}
}
