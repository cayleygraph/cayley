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

package mql

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/google/cayley/graph"
	_ "github.com/google/cayley/graph/memstore"
)

// This is a simple test graph.
//
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
var simpleGraph = []*graph.Triple{
	{"A", "follows", "B", ""},
	{"C", "follows", "B", ""},
	{"C", "follows", "D", ""},
	{"D", "follows", "B", ""},
	{"B", "follows", "F", ""},
	{"F", "follows", "G", ""},
	{"D", "follows", "G", ""},
	{"E", "follows", "F", ""},
	{"B", "status", "cool", "status_graph"},
	{"D", "status", "cool", "status_graph"},
	{"G", "status", "cool", "status_graph"},
}

func makeTestSession(data []*graph.Triple) *Session {
	ts, _ := graph.NewTripleStore("memstore", "", nil)
	for _, t := range data {
		ts.AddTriple(t)
	}
	return NewSession(ts)
}

var testQueries = []struct {
	message string
	query   string
	tag     string
	expect  string
}{
	{
		message: "get all IDs in the database",
		query:   `[{"id": null}]`,
		expect: `
			[
				{"id": "A"},
				{"id": "follows"},
				{"id": "B"},
				{"id": "C"},
				{"id": "D"},
				{"id": "F"},
				{"id": "G"},
				{"id": "E"},
				{"id": "status"},
				{"id": "cool"},
				{"id": "status_graph"}
			]
		`,
	},
	{
		message: "get nodes by status",
		query:   `[{"id": null, "status": "cool"}]`,
		expect: `
			[
				{"id": "B", "status": "cool"},
				{"id": "D", "status": "cool"},
				{"id": "G", "status": "cool"}
			]
		`,
	},
	{
		message: "show correct null semantics",
		query:   `[{"id": "cool", "status": null}]`,
		expect: `
			[
				{"id": "cool", "status": null}
			]
		`,
	},
	{
		message: "get correct follows list",
		query:   `[{"id": "C", "follows": []}]`,
		expect: `
			[
				{"id": "C", "follows": ["B", "D"]}
			]
		`,
	},
	{
		message: "get correct reverse follows list",
		query:   `[{"id": "F", "!follows": []}]`,
		expect: `
			[
				{"id": "F", "!follows": ["B", "E"]}
			]
		`,
	},
	{
		message: "get correct follows struct",
		query:   `[{"id": null, "follows": {"id": null, "status": "cool"}}]`,
		expect: `
			[
				{"id": "A", "follows": {"id": "B", "status": "cool"}},
				{"id": "C", "follows": {"id": "D", "status": "cool"}},
				{"id": "D", "follows": {"id": "G", "status": "cool"}},
				{"id": "F", "follows": {"id": "G", "status": "cool"}}
			]
		`,
	},
	{
		message: "get correct reverse follows struct",
		query:   `[{"id": null, "!follows": [{"id": null, "status" : "cool"}]}]`,
		expect: `
			[
				{"id": "F", "!follows": [{"id": "B", "status": "cool"}]},
				{"id": "B", "!follows": [{"id": "D", "status": "cool"}]},
				{"id": "G", "!follows": [{"id": "D", "status": "cool"}]}
			]
		`,
	},
	{
		message: "get correct co-follows",
		query:   `[{"id": null, "@A:follows": "B", "@B:follows": "D"}]`,
		expect: `
			[
				{"id": "C", "@A:follows": "B", "@B:follows": "D"}
			]
		`,
	},
	{
		message: "get correct reverse co-follows",
		query:   `[{"id": null, "!follows": {"id": "C"}, "@A:!follows": "D"}]`,
		expect: `
			[
				{"id": "B", "!follows": {"id": "C"}, "@A:!follows": "D"}
			]
		`,
	},
}

func runQuery(g []*graph.Triple, query string) interface{} {
	s := makeTestSession(g)
	c := make(chan interface{}, 5)
	go s.ExecInput(query, c, -1)
	for result := range c {
		s.BuildJson(result)
	}
	result, _ := s.GetJson()
	return result
}

func TestMQL(t *testing.T) {
	for _, test := range testQueries {
		got := runQuery(simpleGraph, test.query)
		var expect interface{}
		json.Unmarshal([]byte(test.expect), &expect)
		if !reflect.DeepEqual(got, expect) {
			b, err := json.MarshalIndent(got, "", " ")
			if err != nil {
				t.Fatalf("unexpected JSON marshal error", err)
			}
			t.Errorf("Failed to %s, got: %s expected: %s", test.message, b, test.expect)
		}
	}
}
