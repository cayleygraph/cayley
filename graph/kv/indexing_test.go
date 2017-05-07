package kv

import "testing"

func TestIntersectSorted(t *testing.T) {
	tt := []struct {
		a      []uint64
		b      []uint64
		expect []uint64
	}{
		{
			a:      []uint64{1, 2, 3, 4, 5, 6},
			b:      []uint64{2, 4, 6, 8, 10},
			expect: []uint64{2, 4, 6},
		},
		{
			a:      []uint64{6, 7, 8, 9, 10, 11},
			b:      []uint64{1, 2, 3, 4, 5, 6},
			expect: []uint64{6},
		},
	}

	for i, x := range tt {
		c := intersectSortedUint64(x.a, x.b)
		if len(c) != len(x.expect) {
			t.Errorf("unexpected length: %d expected %d for test %d", len(c), len(x.expect), i)
		}
		for i, y := range c {
			if y != x.expect[i] {
				t.Errorf("unexpected entry: %#v expected %#v for test %d", c, x.expect, i)
			}
		}
	}
}

func TestIndexlist(t *testing.T) {
	init := []uint64{5, 10, 2340, 32432, 3243366}
	b := appendIndex(nil, init)
	out, err := decodeIndex(b)
	if err != nil {
		t.Fatalf("couldn't decodeIndex: %s", err)
	}
	if len(out) != len(init) {
		t.Fatalf("mismatched lengths. got %#v expected %#v", out, init)
	}
	for i := 0; i < len(out); i++ {
		if out[i] != init[i] {
			t.Fatalf("mismatched element %d. got %#v expected %#v", i, out, init)
		}
	}
}
