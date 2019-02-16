package mapset

import (
	"testing"
)

func TestMap(t *testing.T) {
	tree := NewMapWithComparator(func(a, b interface{}) int {
		aa := a.(int)
		bb := b.(int)
		switch {
		case aa > bb:
			return 1
		case aa < bb:
			return -1
		default:
			return 0
		}
	})
	tree.Put(1, "a")
	tree.Put(2, "b")
	tree.Put(3, "c")
	tree.Put(4, "d")
	tree.Put(5, "e")
	tree.Put(6, "f")
	tree.Put(7, "g")

	tests := [][]interface{}{
		{0, nil, false},
		{1, "a", true},
		{2, "b", true},
		{3, "c", true},
		{4, "d", true},
		{5, "e", true},
		{6, "f", true},
		{7, "g", true},
		{8, nil, false},
	}

	// Test values
	for _, test := range tests {
		if value, found := tree.Get(test[0]); value != test[1] || found != test[2] {
			t.Errorf("Got %v,%v expected %v,%v", value, found, test[1], test[2])
		}
	}

	// Test updates
	sz := tree.Size()
	tree.Put(7, "g")
	tree.Put(7, "this doesn't matter either...")
	if sz != tree.Size() {
		t.Errorf("Got %v expected %v", tree.Size(), sz)
	}

	// Test contains
	if !tree.Contains(7) {
		t.Errorf("Got false, expected true")
	}

	if tree.Contains(10) {
		t.Errorf("Got true, expected false")
	}
}
