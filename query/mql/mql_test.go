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
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/graphtest/testutil"
	_ "github.com/cayleygraph/cayley/graph/memstore"
	"github.com/cayleygraph/cayley/query"
	_ "github.com/cayleygraph/cayley/writer"
	"github.com/cayleygraph/quad"
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

func makeTestSession(data []quad.Quad) *Session {
	qs, _ := graph.NewQuadStore("memstore", "", nil)
	w, _ := graph.NewQuadWriter("single", qs, nil)
	for _, t := range data {
		w.AddQuad(t)
	}
	return NewSession(qs)
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
				{"id": "<alice>"},
				{"id": "<follows>"},
				{"id": "<bob>"},
				{"id": "<fred>"},
				{"id": "<status>"},
				{"id": "cool_person"},
				{"id": "<charlie>"},
				{"id": "<dani>"},
				{"id": "<greg>"},
				{"id": "<emily>"},
				{"id": "<predicates>"},
				{"id": "<are>"},
				{"id": "smart_person"},
				{"id": "<smart_graph>"}
			]
		`,
	},
	{
		message: "get nodes by status",
		query:   `[{"id": null, "<status>": "cool_person"}]`,
		expect: `
			[
				{"id": "<bob>", "<status>": "cool_person"},
				{"id": "<dani>", "<status>": "cool_person"},
				{"id": "<greg>", "<status>": "cool_person"}
			]
		`,
	},
	{
		message: "show correct null semantics",
		query:   `[{"id": "cool_person", "status": null}]`,
		expect: `
			[
				{"id": "cool_person", "status": null}
			]
		`,
	},
	{
		message: "get correct follows list",
		query:   `[{"id": "<charlie>", "<follows>": []}]`,
		expect: `
			[
				{"id": "<charlie>", "<follows>": ["<bob>", "<dani>"]}
			]
		`,
	},
	{
		message: "get correct reverse follows list",
		query:   `[{"id": "<fred>", "!<follows>": []}]`,
		expect: `
			[
				{"id": "<fred>", "!<follows>": ["<bob>", "<emily>"]}
			]
		`,
	},
	{
		message: "get correct follows struct",
		query:   `[{"id": null, "<follows>": {"id": null, "<status>": "cool_person"}}]`,
		expect: `
			[
				{"id": "<alice>", "<follows>": {"id": "<bob>", "<status>": "cool_person"}},
				{"id": "<charlie>", "<follows>": {"id": "<dani>", "<status>": "cool_person"}},
				{"id": "<dani>", "<follows>": {"id": "<greg>", "<status>": "cool_person"}},
				{"id": "<fred>", "<follows>": {"id": "<greg>", "<status>": "cool_person"}}
			]
		`,
	},
	{
		message: "get correct reverse follows struct",
		query:   `[{"id": null, "!<follows>": [{"id": null, "<status>" : "cool_person"}]}]`,
		expect: `
			[
				{"id": "<fred>", "!<follows>": [{"id": "<bob>", "<status>": "cool_person"}]},
				{"id": "<bob>", "!<follows>": [{"id": "<dani>", "<status>": "cool_person"}]},
				{"id": "<greg>", "!<follows>": [{"id": "<dani>", "<status>": "cool_person"}]}
			]
		`,
	},
	{
		message: "get correct co-follows",
		query:   `[{"id": null, "@A:<follows>": "<bob>", "@B:<follows>": "<dani>"}]`,
		expect: `
			[
				{"id": "<charlie>", "@A:<follows>": "<bob>", "@B:<follows>": "<dani>"}
			]
		`,
	},
	{
		message: "get correct reverse co-follows",
		query:   `[{"id": null, "!<follows>": {"id": "<charlie>"}, "@A:!<follows>": "<dani>"}]`,
		expect: `
			[
				{"id": "<bob>", "!<follows>": {"id": "<charlie>"}, "@A:!<follows>": "<dani>"}
			]
		`,
	},
}

func runQuery(t testing.TB, g []quad.Quad, qu string) interface{} {
	s := makeTestSession(g)
	ctx := context.TODO()
	it, err := s.Execute(ctx, qu, query.Options{Collation: query.JSON})
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	var out []interface{}
	for it.Next(ctx) {
		out = append(out, it.Result())
	}
	return out
}

func TestMQL(t *testing.T) {
	simpleGraph := testutil.LoadGraph(t, "../../data/testdata.nq")
	for _, test := range testQueries {
		t.Run(test.message, func(t *testing.T) {
			got := runQuery(t, simpleGraph, test.query)
			var expect interface{}
			json.Unmarshal([]byte(test.expect), &expect)
			require.Equal(t, expect, got)
		})
	}
}
