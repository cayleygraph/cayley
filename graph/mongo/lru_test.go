package mongo

import (
	"strconv"
	"testing"
)

func TestLRU(t *testing.T) {
	cache := newCache(10)

	if _, ok := cache.Get("0"); ok {
		t.Errorf("Empty cache Get test failed\n")
	}

	for i := 0; i < 10; i++ {
		cache.Put(strconv.Itoa(i), i)
	}

	for i := 0; i < 10; i++ {
		if v, ok := cache.Get(strconv.Itoa(i)); !ok {
			t.Errorf("catch.Get(%d) not found\n", i)
		} else {
			if v != i {
				t.Errorf("catchGet(%d) got %d\n", i, v)
			}
		}
	}

	for i := 0; i < 10; i++ {
		cache.Put(strconv.Itoa(i), 10-i)
	}

	for i := 0; i < 10; i++ {
		if v, ok := cache.Get(strconv.Itoa(i)); !ok {
			t.Errorf("Update test: catch.Get(%d) not found\n", i)
		} else {
			if v != 10-i {
				t.Errorf("Update test: catchGet(%d) got %d, expect: %d\n", i, v, 10-i)
			}
		}
	}

	for i := 10; i < 20; i++ {
		cache.Put(strconv.Itoa(i), i)
	}

	for i := 0; i < 10; i++ {
		if _, ok := cache.Get(strconv.Itoa(i)); ok {
			t.Errorf("Remove test: catch.Get(%d) should not been found\n", i)
		}
	}
	for i := 10; i < 20; i++ {
		if v, ok := cache.Get(strconv.Itoa(i)); !ok {
			t.Errorf("Remove test: catch.Get(%d) not found\n", i)
		} else {
			if v != i {
				t.Errorf("Remove test: catchGet(%d) got %d, expect: %d\n", i, v, i)
			}
		}
	}
}

