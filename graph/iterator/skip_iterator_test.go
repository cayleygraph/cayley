package iterator

import (
	"reflect"
	"testing"
)

func TestSkipIteratorBasics(t *testing.T) {
	allIt := NewFixed(Identity)
	allIt.Add(1)
	allIt.Add(2)
	allIt.Add(3)
	allIt.Add(4)
	allIt.Add(5)

	u := NewSkip(allIt, 0)
	expectSz, _ := allIt.Size()
	if sz, _ := u.Size(); sz != expectSz {
		t.Errorf("Failed to check Skip size: got:%v expected:%v", sz, expectSz)
	}
	expect := []int{1, 2, 3, 4, 5}
	if got := iterated(u); !reflect.DeepEqual(got, expect) {
		t.Errorf("Failed to iterate Skip correctly: got:%v expected:%v", got, expect)
	}

	allIt.Reset()

	u = NewSkip(allIt, 3)
	expectSz = 2
	if sz, _ := u.Size(); sz != expectSz {
		t.Errorf("Failed to check Skip size: got:%v expected:%v", sz, expectSz)
	}
	expect = []int{4, 5}
	if got := iterated(u); !reflect.DeepEqual(got, expect) {
		t.Errorf("Failed to iterate Skip correctly: got:%v expected:%v", got, expect)
	}

	for _, v := range []int{1, 2, 3, 4, 5} {
		if !u.Contains(v) {
			t.Errorf("Failed to find a correct value in the Skip iterator.")
		}
	}
}
