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

package gremlin

import (
	"sort"
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	"github.com/google/cayley/graph/memstore"
)

//    +---+                        +---+
//    | A |-------               ->| F |<--
//    +---+       \------>+---+-/  +---+   \--+---+
//                 ------>|#B#|      |        | E |
//    +---+-------/      >+---+      |        +---+
//    | C |             /            v
//    +---+           -/           +---+
//      ----    +---+/             |#G#|
//          \-->|#D#|------------->+---+
//              +---+
//

func buildTripleStore() *Session {
	ts := memstore.MakeTestingMemstore()
	return NewSession(ts, -1, false)
}

func shouldBeUnordered(actual interface{}, expected ...interface{}) string {
	if len(expected) != 1 {
		return "Only one list supported"
	}
	actualStr := actual.([]string)
	expectedStr := expected[0].([]string)
	sort.Strings(actualStr)
	sort.Strings(expectedStr)
	return ShouldResemble(actualStr, expectedStr)
}

func runQueryGetTag(query string, tag string) ([]string, int) {
	js := buildTripleStore()
	output := make([]string, 0)
	c := make(chan interface{}, 5)
	js.ExecInput(query, c, -1)
	count := 0
	for result := range c {
		count++
		data := result.(*GremlinResult)
		if data.val == nil {
			val := (*data.actualResults)[tag]
			if val != nil {
				output = append(output, js.ts.NameOf(val))
			}
		}
	}
	return output, count
}

func ConveyQuery(doc string, query string, expected []string) {
	ConveyQueryTag(doc, query, TopResultTag, expected)
}

func ConveyQueryTag(doc string, query string, tag string, expected []string) {
	Convey(doc, func() {
		actual, _ := runQueryGetTag(query, tag)
		So(actual, shouldBeUnordered, expected)
	})
}

func TestGremlin(t *testing.T) {
	Convey("With a default memtriplestore", t, func() {

		ConveyQuery("Can get a single vertex",
			`g.V("A").All()`,
			[]string{"A"})

		ConveyQuery("Can use .Out()",
			`g.V("A").Out("follows").All()`,
			[]string{"B"})

		ConveyQuery("Can use .In()",
			`g.V("B").In("follows").All()`,
			[]string{"A", "C", "D"})

		ConveyQuery("Can use .Both()",
			`g.V("F").Both("follows").All()`,
			[]string{"B", "G", "E"})

		ConveyQuery("Can use .Tag()-.Is()-.Back()",
			`g.V("B").In("follows").Tag("foo").Out("status").Is("cool").Back("foo").All()`,
			[]string{"D"})

		ConveyQuery("Can separate .Tag()-.Is()-.Back()",
			`
			x = g.V("C").Out("follows").Tag("foo").Out("status").Is("cool").Back("foo")
			x.In("follows").Is("D").Back("foo").All()
			`,
			[]string{"B"})

		Convey("Can do multiple .Back()s", func() {
			query := `
				g.V("E").Out("follows").As("f").Out("follows").Out("status").Is("cool").Back("f").In("follows").In("follows").As("acd").Out("status").Is("cool").Back("f").All()
			`
			expected := []string{"D"}
			actual, _ := runQueryGetTag(query, "acd")
			So(actual, shouldBeUnordered, expected)
		})

	})
}

func TestGremlinMorphism(t *testing.T) {
	Convey("With a default memtriplestore", t, func() {

		ConveyQuery("Simple morphism works",
			`
			grandfollows = g.M().Out("follows").Out("follows")
			g.V("C").Follow(grandfollows).All()
			`,
			[]string{"G", "F", "B"})

		ConveyQuery("Reverse morphism works",
			`
		grandfollows = g.M().Out("follows").Out("follows")
		g.V("F").FollowR(grandfollows).All()
		`, []string{"A", "C", "D"})

	})
}

func TestGremlinIntersection(t *testing.T) {
	Convey("With a default memtriplestore", t, func() {
		ConveyQuery("Simple intersection",
			`
		function follows(x) { return g.V(x).Out("follows") }

		follows("D").And(follows("C")).All()
		`, []string{"B"})

		ConveyQuery("Simple Morphism Intersection",
			`
		grandfollows = g.M().Out("follows").Out("follows")
		function gfollows(x) { return g.V(x).Follow(grandfollows) }

		gfollows("A").And(gfollows("C")).All()
		`, []string{"F"})

		ConveyQuery("Double Morphism Intersection",
			`
		grandfollows = g.M().Out("follows").Out("follows")
		function gfollows(x) { return g.V(x).Follow(grandfollows) }

		gfollows("E").And(gfollows("C")).And(gfollows("B")).All()
		`, []string{"G"})

		ConveyQuery("Reverse Intersection",
			`
		grandfollows = g.M().Out("follows").Out("follows")

		g.V("G").FollowR(grandfollows).Intersect(g.V("F").FollowR(grandfollows)).All()
		`, []string{"C"})

		ConveyQuery("Standard sort of morphism intersection, continue follow",
			`
			gfollowers = g.M().In("follows").In("follows")
			function cool(x) { return g.V(x).As("a").Out("status").Is("cool").Back("a") }
			cool("G").Follow(gfollowers).Intersect(cool("B").Follow(gfollowers)).All()
		`, []string{"C"})

	})
}

func TestGremlinHas(t *testing.T) {
	Convey("With a default memtriplestore", t, func() {
		ConveyQuery("Test a simple Has",
			`g.V().Has("status", "cool").All()`,
			[]string{"G", "D", "B"})

		ConveyQuery("Test a double Has",
			`g.V().Has("status", "cool").Has("follows", "F").All()`,
			[]string{"B"})

	})
}

func TestGremlinTag(t *testing.T) {
	Convey("With a default memtriplestore", t, func() {
		ConveyQueryTag("Test a simple save",
			`g.V().Save("status", "somecool").All()`,
			"somecool",
			[]string{"cool", "cool", "cool"})

		ConveyQueryTag("Test a simple saveR",
			`g.V("cool").SaveR("status", "who").All()`,
			"who",
			[]string{"G", "D", "B"})

		ConveyQueryTag("Test an out save",
			`g.V("D").Out(null, "pred").All()`,
			"pred",
			[]string{"follows", "follows", "status"})

		ConveyQueryTag("Test a tag list",
			`g.V("D").Out(null, ["pred", "foo", "bar"]).All()`,
			"foo",
			[]string{"follows", "follows", "status"})

		ConveyQuery("Test a pred list",
			`g.V("D").Out(["follows", "status"]).All()`,
			[]string{"B", "G", "cool"})

		ConveyQuery("Test a predicate path",
			`g.V("D").Out(g.V("follows"), "pred").All()`,
			[]string{"B", "G"})
	})
}
