package iterator_test

import (
	"reflect"
	"testing"

	. "github.com/cayleygraph/cayley/graph/iterator"
)

func TestUniqueIteratorBasics(t *testing.T) {
	allIt := NewFixed(Identity,
		Int64Node(1),
		Int64Node(2),
		Int64Node(3),
		Int64Node(3),
		Int64Node(2),
	)

	u := NewUnique(allIt)

	expect := []int{1, 2, 3}
	for i := 0; i < 2; i++ {
		if got := iterated(u); !reflect.DeepEqual(got, expect) {
			t.Errorf("Failed to iterate Unique correctly on repeat %d: got:%v expected:%v", i, got, expect)
		}
		u.Reset()
	}

	for _, v := range []int{1, 2, 3} {
		if !u.Contains(Int64Node(v)) {
			t.Errorf("Failed to find a correct value in the unique iterator.")
		}
	}
}
