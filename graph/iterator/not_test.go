package iterator_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	. "github.com/cayleygraph/cayley/graph/iterator"
)

func TestNotIteratorBasics(t *testing.T) {
	ctx := context.TODO()
	allIt := NewFixed(
		Int64Node(1),
		Int64Node(2),
		Int64Node(3),
		Int64Node(4),
	)

	toComplementIt := NewFixed(
		Int64Node(2),
		Int64Node(4),
	)

	not := NewNot(toComplementIt, allIt)

	if v, _ := not.Size(); v != 2 {
		t.Errorf("Unexpected iterator size: got:%d, expected: %d", v, 2)
	}

	expect := []int{1, 3}
	for i := 0; i < 2; i++ {
		if got := iterated(not); !reflect.DeepEqual(got, expect) {
			t.Errorf("Failed to iterate Not correctly on repeat %d: got:%v expected:%v", i, got, expect)
		}
		not.Reset()
	}

	for _, v := range []int{1, 3} {
		if !not.Contains(ctx, Int64Node(v)) {
			t.Errorf("Failed to correctly check %d as true", v)
		}
	}

	for _, v := range []int{2, 4} {
		if not.Contains(ctx, Int64Node(v)) {
			t.Errorf("Failed to correctly check %d as false", v)
		}
	}
}

func TestNotIteratorErr(t *testing.T) {
	ctx := context.TODO()
	wantErr := errors.New("unique")
	allIt := newTestIterator(false, wantErr)

	toComplementIt := NewFixed()

	not := NewNot(toComplementIt, allIt)

	if not.Next(ctx) != false {
		t.Errorf("Not iterator did not pass through initial 'false'")
	}
	if not.Err() != wantErr {
		t.Errorf("Not iterator did not pass through underlying Err")
	}
}
