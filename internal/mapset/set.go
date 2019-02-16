package mapset

import (
	mset "github.com/deckarep/golang-set"
)

type Set interface {
	Add(interface{}) bool

	Remove(interface{})

	Contains(...interface{}) bool

	Each(func(interface{}) bool)

	Clear()

	Clone() Set

	Size() int

	String() string

	ToSlice() []interface{}
}

type set struct {
	inner mset.Set
}

func NewSet(s ...interface{}) Set {
	return &set{mset.NewSet(s)}
}

func NewSetFromSlice(s []interface{}) Set {
	return &set{mset.NewSet(s...)}
}

func NewThreadUnsafeSet() Set {
	return &set{mset.NewThreadUnsafeSet()}
}

func NewThreadUnsafeSetFromSlice(s []interface{}) Set {
	return &set{mset.NewThreadUnsafeSetFromSlice(s)}
}

func (s *set) Each(f func(interface{}) bool) {
	s.inner.Each(f)
}

func (s *set) Add(i interface{}) bool {
	return s.inner.Add(i)
}

func (s *set) Remove(i interface{}) {
	s.inner.Remove(i)
}

func (s *set) Contains(i ...interface{}) bool {
	return s.inner.Contains(i...)
}

func (s *set) Clear() {
	s.inner.Clear()
}

func (s *set) Clone() Set {
	return &set{s.inner.Clone()}
}

func (s *set) Size() int {
	return s.inner.Cardinality()
}

func (s *set) String() string {
	return s.inner.String()
}

func (s *set) ToSlice() []interface{} {
	return s.inner.ToSlice()
}
