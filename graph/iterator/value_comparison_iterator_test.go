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

package iterator

import (
	"testing"

	"github.com/google/cayley/graph"
)

func SetupMockTripleStore(nameMap map[string]int) *TestTripleStore {
	ts := new(TestTripleStore)
	for k, v := range nameMap {
		ts.On("ValueOf", k).Return(v)
		ts.On("NameOf", v).Return(k)
	}
	return ts
}

func SimpleValueTripleStore() *TestTripleStore {
	ts := SetupMockTripleStore(map[string]int{
		"0": 0,
		"1": 1,
		"2": 2,
		"3": 3,
		"4": 4,
		"5": 5,
	})
	return ts
}

func BuildFixedIterator() *Fixed {
	fixed := newFixed()
	fixed.Add(0)
	fixed.Add(1)
	fixed.Add(2)
	fixed.Add(3)
	fixed.Add(4)
	return fixed
}

func checkIteratorContains(ts graph.TripleStore, it graph.Iterator, expected []string, t *testing.T) {
	var actual []string
	actual = nil
	for {
		val, ok := it.Next()
		if !ok {
			break
		}
		actual = append(actual, ts.NameOf(val))
	}
	actualSet := actual[:]
	for _, a := range expected {
		found := false
		for j, b := range actualSet {
			if a == b {
				actualSet = append(actualSet[:j], actualSet[j+1:]...)
				found = true
				break
			}
		}
		if !found {
			t.Error("Couldn't find", a, "in actual output.\nActual:", actual, "\nExpected: ", expected, "\nRemainder: ", actualSet)
			return
		}
	}
	if len(actualSet) != 0 {
		t.Error("Actual output has more than expected.\nActual:", actual, "\nExpected: ", expected, "\nRemainder: ", actualSet)
	}
}

func TestWorkingIntValueComparison(t *testing.T) {
	ts := SimpleValueTripleStore()
	fixed := BuildFixedIterator()
	vc := NewComparison(fixed, kCompareLT, int64(3), ts)
	checkIteratorContains(ts, vc, []string{"0", "1", "2"}, t)
}

func TestFailingIntValueComparison(t *testing.T) {
	ts := SimpleValueTripleStore()
	fixed := BuildFixedIterator()
	vc := NewComparison(fixed, kCompareLT, int64(0), ts)
	checkIteratorContains(ts, vc, []string{}, t)
}

func TestWorkingGT(t *testing.T) {
	ts := SimpleValueTripleStore()
	fixed := BuildFixedIterator()
	vc := NewComparison(fixed, kCompareGT, int64(2), ts)
	checkIteratorContains(ts, vc, []string{"3", "4"}, t)
}

func TestWorkingGTE(t *testing.T) {
	ts := SimpleValueTripleStore()
	fixed := BuildFixedIterator()
	vc := NewComparison(fixed, kCompareGTE, int64(2), ts)
	checkIteratorContains(ts, vc, []string{"2", "3", "4"}, t)
}

func TestVCICheck(t *testing.T) {
	ts := SimpleValueTripleStore()
	fixed := BuildFixedIterator()
	vc := NewComparison(fixed, kCompareGTE, int64(2), ts)
	if vc.Check(1) {
		t.Error("1 is less than 2, should be GTE")
	}
	if !vc.Check(2) {
		t.Error("2 is GTE 2")
	}
	if !vc.Check(3) {
		t.Error("3 is GTE 2")
	}
	if vc.Check(5) {
		t.Error("5 is not in the underlying iterator")
	}
}
