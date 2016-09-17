package iterator

import (
	"reflect"
	"testing"
)

func TestLimitIteratorBasics(t *testing.T) {
	allIt := NewFixed(Identity,
		Int64Node(1),
		Int64Node(2),
		Int64Node(3),
		Int64Node(4),
		Int64Node(5),
	)

	u := NewLimit(allIt, 0)
	expectSz, _ := allIt.Size()
	if sz, _ := u.Size(); sz != expectSz {
		t.Errorf("Failed to check Limit size: got:%v expected:%v", sz, expectSz)
	}
	expect := []int{1, 2, 3, 4, 5}
	if got := iterated(u); !reflect.DeepEqual(got, expect) {
		t.Errorf("Failed to iterate Limit correctly: got:%v expected:%v", got, expect)
	}

	allIt.Reset()

	u = NewLimit(allIt, 3)
	expectSz = 3
	if sz, _ := u.Size(); sz != expectSz {
		t.Errorf("Failed to check Limit size: got:%v expected:%v", sz, expectSz)
	}
	expect = []int{1, 2, 3}
	if got := iterated(u); !reflect.DeepEqual(got, expect) {
		t.Errorf("Failed to iterate Limit correctly: got:%v expected:%v", got, expect)
	}

	for _, v := range []int{1, 2, 3, 4, 5} {
		if !u.Contains(Int64Node(v)) {
			t.Errorf("Failed to find a correct value in the Limit iterator.")
		}
	}
}
