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

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/cayleygraph/cayley/graph"
	. "github.com/cayleygraph/cayley/graph/iterator"
)

func iterated(it graph.Iterator) []int {
	ctx := context.TODO()
	var res []int
	for it.Next(ctx) {
		res = append(res, int(it.Result().(Int64Node)))
	}
	return res
}

func TestOrIteratorBasics(t *testing.T) {
	ctx := context.TODO()
	or := NewOr()
	f1 := NewFixed(
		Int64Node(1),
		Int64Node(2),
		Int64Node(3),
	)
	f2 := NewFixed(
		Int64Node(3),
		Int64Node(9),
		Int64Node(20),
		Int64Node(21),
	)
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
		if !or.Contains(ctx, Int64Node(v)) {
			t.Errorf("Failed to correctly check %d as true", v)
		}
	}

	for _, v := range []int{22, 5, 0} {
		if or.Contains(ctx, Int64Node(v)) {
			t.Errorf("Failed to correctly check %d as false", v)
		}
	}
}

func TestShortCircuitingOrBasics(t *testing.T) {
	ctx := context.TODO()
	var or *Or

	f1 := NewFixed(
		Int64Node(1),
		Int64Node(2),
		Int64Node(3),
	)
	f2 := NewFixed(
		Int64Node(3),
		Int64Node(9),
		Int64Node(20),
		Int64Node(21),
	)

	or = NewShortCircuitOr()
	or.AddSubIterator(f1)
	or.AddSubIterator(f2)
	f2.Reset()
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
	f2.Reset()
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
	f2.Reset()
	for _, v := range []int{2, 3, 21} {
		if !or.Contains(ctx, Int64Node(v)) {
			t.Errorf("Failed to correctly check %d as true", v)
		}
	}
	for _, v := range []int{22, 5, 0} {
		if or.Contains(ctx, Int64Node(v)) {
			t.Errorf("Failed to correctly check %d as false", v)
		}
	}

	// Check that it pulls the second iterator's numbers if the first is empty.
	or = NewShortCircuitOr()
	or.AddSubIterator(NewFixed())
	or.AddSubIterator(f2)
	f2.Reset()
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
	ctx := context.TODO()
	wantErr := errors.New("unique")
	orErr := newTestIterator(false, wantErr)

	fix1 := NewFixed(Int64Node(1))

	or := NewOr(
		fix1,
		orErr,
		newInt64(1, 5, true),
	)

	if !or.Next(ctx) {
		t.Errorf("Failed to iterate Or correctly")
	}
	if got := or.Result(); got.(Int64Node) != 1 {
		t.Errorf("Failed to iterate Or correctly, got:%v expect:1", got)
	}

	if or.Next(ctx) != false {
		t.Errorf("Or iterator did not pass through underlying 'false'")
	}
	if or.Err() != wantErr {
		t.Errorf("Or iterator did not pass through underlying Err")
	}
}

func TestShortCircuitOrIteratorErr(t *testing.T) {
	ctx := context.TODO()
	wantErr := errors.New("unique")
	orErr := newTestIterator(false, wantErr)

	or := NewOr(
		orErr,
		newInt64(1, 5, true),
	)

	if or.Next(ctx) != false {
		t.Errorf("Or iterator did not pass through underlying 'false'")
	}
	if or.Err() != wantErr {
		t.Errorf("Or iterator did not pass through underlying Err")
	}
}
