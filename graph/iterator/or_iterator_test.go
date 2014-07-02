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

	. "github.com/smartystreets/goconvey/convey"

	"github.com/google/cayley/graph"
)

func extractNumbersFromIterator(it graph.Iterator) []int {
	var outputNumbers []int
	for {
		val, ok := it.Next()
		if !ok {
			break
		}
		outputNumbers = append(outputNumbers, val.(int))
	}
	return outputNumbers
}

func TestOrIteratorBasics(t *testing.T) {
	var orIt *Or

	Convey("Given an Or Iterator of two fixed iterators", t, func() {
		orIt = NewOr()
		fixed1 := newFixed()
		fixed1.Add(1)
		fixed1.Add(2)
		fixed1.Add(3)
		fixed2 := newFixed()
		fixed2.Add(3)
		fixed2.Add(9)
		fixed2.Add(20)
		fixed2.Add(21)
		orIt.AddSubIterator(fixed1)
		orIt.AddSubIterator(fixed2)

		Convey("It should guess its size.", func() {
			v, _ := orIt.Size()
			So(v, ShouldEqual, 7)
		})

		Convey("It should extract all the numbers, potentially twice.", func() {
			allNumbers := []int{1, 2, 3, 3, 9, 20, 21}
			So(extractNumbersFromIterator(orIt), ShouldResemble, allNumbers)
			orIt.Reset()
			So(extractNumbersFromIterator(orIt), ShouldResemble, allNumbers)
			// Optimization works
			newOr, _ := orIt.Optimize()
			So(extractNumbersFromIterator(newOr), ShouldResemble, allNumbers)
		})

		Convey("It should check that numbers in either iterator exist.", func() {
			So(orIt.Check(2), ShouldEqual, true)
			So(orIt.Check(3), ShouldEqual, true)
			So(orIt.Check(21), ShouldEqual, true)
		})

		Convey("It should check that numbers not in either iterator are false.", func() {
			So(orIt.Check(22), ShouldEqual, false)
			So(orIt.Check(5), ShouldEqual, false)
			So(orIt.Check(0), ShouldEqual, false)
		})

	})

}

func TestShortCircuitingOrBasics(t *testing.T) {
	var orIt *Or

	Convey("Given a short-circuiting Or of two fixed iterators", t, func() {
		orIt = NewShortCircuitOr()
		fixed1 := newFixed()
		fixed1.Add(1)
		fixed1.Add(2)
		fixed1.Add(3)
		fixed2 := newFixed()
		fixed2.Add(3)
		fixed2.Add(9)
		fixed2.Add(20)
		fixed2.Add(21)

		Convey("It should guess its size.", func() {
			orIt.AddSubIterator(fixed1)
			orIt.AddSubIterator(fixed2)
			v, _ := orIt.Size()
			So(v, ShouldEqual, 4)
		})

		Convey("It should extract the first iterators' numbers.", func() {
			orIt.AddSubIterator(fixed1)
			orIt.AddSubIterator(fixed2)
			allNumbers := []int{1, 2, 3}
			So(extractNumbersFromIterator(orIt), ShouldResemble, allNumbers)
			orIt.Reset()
			So(extractNumbersFromIterator(orIt), ShouldResemble, allNumbers)
			// Optimization works
			newOr, _ := orIt.Optimize()
			So(extractNumbersFromIterator(newOr), ShouldResemble, allNumbers)
		})

		Convey("It should check that numbers in either iterator exist.", func() {
			orIt.AddSubIterator(fixed1)
			orIt.AddSubIterator(fixed2)
			So(orIt.Check(2), ShouldEqual, true)
			So(orIt.Check(3), ShouldEqual, true)
			So(orIt.Check(21), ShouldEqual, true)
			So(orIt.Check(22), ShouldEqual, false)
			So(orIt.Check(5), ShouldEqual, false)
			So(orIt.Check(0), ShouldEqual, false)

		})

		Convey("It should check that it pulls the second iterator's numbers if the first is empty.", func() {
			orIt.AddSubIterator(newFixed())
			orIt.AddSubIterator(fixed2)
			allNumbers := []int{3, 9, 20, 21}
			So(extractNumbersFromIterator(orIt), ShouldResemble, allNumbers)
			orIt.Reset()
			So(extractNumbersFromIterator(orIt), ShouldResemble, allNumbers)
			// Optimization works
			newOr, _ := orIt.Optimize()
			So(extractNumbersFromIterator(newOr), ShouldResemble, allNumbers)
		})

	})

}
