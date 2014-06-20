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

package graph

// A quickly mocked version of the TripleStore interface, for use in tests.
// Can better used Mock.Called but will fill in as needed.

import (
	"github.com/stretchrcom/testify/mock"
)

type TestTripleStore struct {
	mock.Mock
}

func (ts *TestTripleStore) GetIdFor(s string) TSVal {
	args := ts.Mock.Called(s)
	return args.Get(0)
}
func (ts *TestTripleStore) AddTriple(*Triple)       {}
func (ts *TestTripleStore) AddTripleSet([]*Triple)  {}
func (ts *TestTripleStore) GetTriple(TSVal) *Triple { return &Triple{} }
func (ts *TestTripleStore) GetTripleIterator(s string, i TSVal) Iterator {
	args := ts.Mock.Called(s, i)
	return args.Get(0).(Iterator)
}
func (ts *TestTripleStore) GetNodesAllIterator() Iterator   { return &NullIterator{} }
func (ts *TestTripleStore) GetTriplesAllIterator() Iterator { return &NullIterator{} }
func (ts *TestTripleStore) GetIteratorByString(string, string, string) Iterator {
	return &NullIterator{}
}
func (ts *TestTripleStore) GetNameFor(v TSVal) string {
	args := ts.Mock.Called(v)
	return args.Get(0).(string)
}
func (ts *TestTripleStore) Size() int64 { return 0 }
func (ts *TestTripleStore) DebugPrint() {}
func (ts *TestTripleStore) OptimizeIterator(it Iterator) (Iterator, bool) {
	return &NullIterator{}, false
}
func (ts *TestTripleStore) MakeFixed() *FixedIterator {
	return NewFixedIteratorWithCompare(BasicEquality)
}
func (ts *TestTripleStore) Close()                                 {}
func (ts *TestTripleStore) GetTripleDirection(TSVal, string) TSVal { return 0 }
func (ts *TestTripleStore) RemoveTriple(t *Triple)                 {}
