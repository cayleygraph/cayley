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

package nquads

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	"github.com/google/cayley/graph"
)

func TestParsingNTriples(t *testing.T) {
	Convey("When parsing", t, func() {
		Convey("It should not parse invalid triples", func() {
			x := Parse("invalid")
			So(x, ShouldBeNil)
		})
		Convey("It should not parse comments", func() {
			x := Parse("# nominally valid triple .")
			So(x, ShouldBeNil)
		})
		Convey("It should parse simple triples", func() {
			x := Parse("this is valid .")
			So(x, ShouldNotBeNil)
			So(x.Subject, ShouldEqual, "this")
		})
		Convey("It should parse quoted triples", func() {
			x := Parse("this is \"valid too\" .")
			So(x, ShouldNotBeNil)
			So(x.Object, ShouldEqual, "valid too")
			So(x.Provenance, ShouldEqual, "")
		})
		Convey("It should parse escaped quoted triples", func() {
			x := Parse("he said \"\\\"That's all folks\\\"\" .")
			So(x, ShouldNotBeNil)
			So(x.Object, ShouldEqual, "\"That's all folks\"")
			So(x.Provenance, ShouldEqual, "")
		})

		Convey("It should parse an example real triple", func() {
			x := Parse("\":/guid/9202a8c04000641f80000000010c843c\" \"name\" \"George Morris\" .")
			So(x, ShouldNotBeNil)
			So(x.Object, ShouldEqual, "George Morris")
			So(x.Provenance, ShouldEqual, "")
		})

		Convey("It should parse a pathologically spaced triple", func() {
			x := Parse("foo is \"\\tA big tough\\r\\nDeal\\\\\" .")
			So(x, ShouldNotBeNil)
			So(x.Object, ShouldEqual, "\tA big tough\r\nDeal\\")
			So(x.Provenance, ShouldEqual, "")
		})

		Convey("It should parse a simple quad", func() {
			x := Parse("this is valid quad .")
			So(x, ShouldNotBeNil)
			So(x.Object, ShouldEqual, "valid")
			So(x.Provenance, ShouldEqual, "quad")
		})

		Convey("It should parse a quoted quad", func() {
			x := Parse("this is valid \"quad thing\" .")
			So(x, ShouldNotBeNil)
			So(x.Object, ShouldEqual, "valid")
			So(x.Provenance, ShouldEqual, "quad thing")
		})

		Convey("It should parse crazy escaped quads", func() {
			x := Parse("\"\\\"this\" \"\\\"is\" \"\\\"valid\" \"\\\"quad thing\".")
			So(x, ShouldNotBeNil)
			So(x.Subject, ShouldEqual, "\"this")
			So(x.Predicate, ShouldEqual, "\"is")
			So(x.Object, ShouldEqual, "\"valid")
			So(x.Provenance, ShouldEqual, "\"quad thing")
		})
	})
}

func TestParsingNTriplesOfficial(t *testing.T) {
	Convey("When using some public test cases...", t, func() {
		Convey("It should handle some simple cases with comments", func() {
			var x *graph.Triple
			x = Parse("<http://example/s> <http://example/p> <http://example/o> . # comment")
			So(x, ShouldNotBeNil)
			So(x.Subject, ShouldEqual, "http://example/s")
			So(x.Predicate, ShouldEqual, "http://example/p")
			So(x.Object, ShouldEqual, "http://example/o")
			So(x.Provenance, ShouldEqual, "")
			x = Parse("<http://example/s> <http://example/p> _:o . # comment")
			So(x, ShouldNotBeNil)
			So(x.Subject, ShouldEqual, "http://example/s")
			So(x.Predicate, ShouldEqual, "http://example/p")
			So(x.Object, ShouldEqual, "_:o")
			So(x.Provenance, ShouldEqual, "")
			x = Parse("<http://example/s> <http://example/p> \"o\" . # comment")
			So(x, ShouldNotBeNil)
			So(x.Object, ShouldEqual, "o")
			So(x.Provenance, ShouldEqual, "")
			x = Parse("<http://example/s> <http://example/p> \"o\"^^<http://example/dt> . # comment")
			So(x, ShouldNotBeNil)
			So(x.Object, ShouldEqual, "o")
			So(x.Provenance, ShouldEqual, "")
			x = Parse("<http://example/s> <http://example/p> \"o\"@en . # comment")
			So(x, ShouldNotBeNil)
			So(x.Object, ShouldEqual, "o")
			So(x.Provenance, ShouldEqual, "")
		})
	})
}

func BenchmarkParser(b *testing.B) {
	for n := 0; n < b.N; n++ {
		x := Parse("<http://example/s> <http://example/p> \"object of some real\\tlength\"@en . # comment")
		if x.Object != "object of some real\tlength" {
			b.Fail()
		}
	}
}
