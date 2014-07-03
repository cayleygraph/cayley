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
	"reflect"
	"testing"

	"github.com/google/cayley/graph"
)

var testNTriples = []struct {
	message string
	input   string
	expect  *graph.Triple
}{
	// NTriple tests.
	{
		message: "not parse invalid triples",
		input:   "invalid",
		expect:  nil,
	},
	{
		message: "not parse comments",
		input:   "# nominally valid triple .",
		expect:  nil,
	},
	{
		message: "parse simple triples",
		input:   "this is valid .",
		expect:  &graph.Triple{"this", "is", "valid", ""},
	},
	{
		message: "parse quoted triples",
		input:   `this is "valid too" .`,
		expect:  &graph.Triple{"this", "is", "valid too", ""},
	},
	{
		message: "parse escaped quoted triples",
		input:   `he said "\"That's all folks\"" .`,
		expect:  &graph.Triple{"he", "said", `"That's all folks"`, ""},
	},
	{
		message: "parse an example real triple",
		input:   `":/guid/9202a8c04000641f80000000010c843c" "name" "George Morris" .`,
		expect:  &graph.Triple{":/guid/9202a8c04000641f80000000010c843c", "name", "George Morris", ""},
	},
	{
		message: "parse a pathologically spaced triple",
		input:   "foo is \"\\tA big tough\\r\\nDeal\\\\\" .",
		expect:  &graph.Triple{"foo", "is", "\tA big tough\r\nDeal\\", ""},
	},

	// NQuad tests.
	{
		message: "parse a simple quad",
		input:   "this is valid quad .",
		expect:  &graph.Triple{"this", "is", "valid", "quad"},
	},
	{
		message: "parse a quoted quad",
		input:   `this is valid "quad thing" .`,
		expect:  &graph.Triple{"this", "is", "valid", "quad thing"},
	},
	{
		message: "parse crazy escaped quads",
		input:   `"\"this" "\"is" "\"valid" "\"quad thing".`,
		expect:  &graph.Triple{`"this`, `"is`, `"valid`, `"quad thing`},
	},

	// NTriple official tests.
	{
		message: "handle simple case with comments",
		input:   "<http://example/s> <http://example/p> <http://example/o> . # comment",
		expect:  &graph.Triple{"http://example/s", "http://example/p", "http://example/o", ""},
	},
	{
		message: "handle simple case with comments",
		input:   "<http://example/s> <http://example/p> _:o . # comment",
		expect:  &graph.Triple{"http://example/s", "http://example/p", "_:o", ""},
	},
	{
		message: "handle simple case with comments",
		input:   "<http://example/s> <http://example/p> \"o\" . # comment",
		expect:  &graph.Triple{"http://example/s", "http://example/p", "o", ""},
	},
	{
		message: "handle simple case with comments",
		input:   "<http://example/s> <http://example/p> \"o\"^^<http://example/dt> . # comment",
		expect:  &graph.Triple{"http://example/s", "http://example/p", "o", ""},
	},
	{
		message: "handle simple case with comments",
		input:   "<http://example/s> <http://example/p> \"o\"@en . # comment",
		expect:  &graph.Triple{"http://example/s", "http://example/p", "o", ""},
	},
}

func TestParse(t *testing.T) {
	for _, test := range testNTriples {
		got := Parse(test.input)
		if !reflect.DeepEqual(got, test.expect) {
			t.Errorf("Failed to %s, %q, got:%q expect:%q", test.message, test.input, got, test.expect)
		}
	}
}

var result *graph.Triple

func BenchmarkParser(b *testing.B) {
	for n := 0; n < b.N; n++ {
		result = Parse("<http://example/s> <http://example/p> \"object of some real\\tlength\"@en . # comment")
	}
}
