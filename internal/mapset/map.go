package mapset

import (
	"github.com/emirpasic/gods/trees/btree"
	"github.com/emirpasic/gods/utils"
)

type Map interface {
	Put(k, v interface{}) bool

	Get(k interface{}) (interface{}, bool)

	Remove(k interface{})

	Contains(k ...interface{}) bool

	Each(func(k, v interface{}) int)

	Keys() []interface{}

	Values() []interface{}

	Size() int

	Clear()

	String() string
}

func NewBytesMap() Map {
	return NewMapWithComparator(utils.ByteComparator)
}

func NewMapWithComparator(cmp func(a, b interface{}) int) Map {
	m := &btreeMap{
		inner: btree.NewWith(10, cmp),
	}
	return m
}

type btreeMap struct {
	inner *btree.Tree
	cmp   func(a, b interface{}) int
}

func (m btreeMap) Each(f func(k, v interface{}) int) {
	it := m.inner.Iterator()
	for it.Next() {
		f(it.Key(), it.Value())
	}
}

func (m *btreeMap) Put(k, v interface{}) bool {
	m.inner.Put(k, v)
	return true
}

func (m btreeMap) Get(k interface{}) (interface{}, bool) {
	return m.inner.Get(k)
}

func (m *btreeMap) Remove(k interface{}) {
	m.inner.Remove(k)
}

func (b *btreeMap) Contains(k ...interface{}) bool {
	for _, n := range k {
		if _, exists := b.inner.Get(n); exists {
			return true
		}
	}
	return false
}

func (m *btreeMap) Clear() {
	m.inner.Clear()
}

func (m btreeMap) Size() int {
	return m.inner.Size()
}

func (m btreeMap) Keys() []interface{} {
	return m.inner.Keys()
}

func (m btreeMap) Values() []interface{} {
	return m.inner.Values()
}

func (m btreeMap) String() string {
	return m.inner.String()
}
