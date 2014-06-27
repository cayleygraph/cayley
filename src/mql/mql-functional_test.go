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
	"github.com/google/cayley/src/graph_memstore"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
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

func buildTripleStore() *MqlSession {
	ts := graph_memstore.MakeTestingMemstore()
	return NewMqlSession(ts)
}

func compareJsonInterfaces(actual interface{}, expected interface{}, path MqlPath, t *testing.T) {
	isError := false
	switch ex := expected.(type) {
	case bool:
		switch ac := actual.(type) {
		case bool:
			if ac != ex {
				isError = true
			}
		default:
			t.Log("Mismatched type")
			isError = true
		}
	case float64:
		switch ac := actual.(type) {
		case float64:
			if ac != ex {
				isError = true
			}
		default:
			t.Log("Mismatched type")
			isError = true
		}
	case string:
		switch ac := actual.(type) {
		case string:
			if ac != ex {
				isError = true
			}
		default:
			isError = true
		}
	case []interface{}:
		switch ac := actual.(type) {
		case []interface{}:
			if len(ac) != len(ex) {
				t.Log("Different lengths")
				isError = true
			} else {
				for i, elem := range ex {
					compareJsonInterfaces(ac[i], elem, path.Follow(string(i)), t)
				}
			}
		default:
			t.Log("Mismatched type")
			isError = true
		}
	case map[string]interface{}:
		switch ac := actual.(type) {
		case map[string]interface{}:
			for k, v := range ex {
				actual_value, ok := ac[k]
				if !ok {
					t.Log("Key", k, "not in actual output.")
					isError = true
				} else {
					compareJsonInterfaces(actual_value, v, path.Follow(string(k)), t)
				}
			}
		default:
			t.Log("Mismatched type")
			isError = true
		}
	case nil:
		switch ac := actual.(type) {
		case nil:
			if ac != ex {
				isError = true
			}
		default:
			t.Log("Mismatched type")
			isError = true
		}
	default:
		t.Error("Unknown JSON type?", expected)
	}

	if isError {
		actual_bytes, _ := json.MarshalIndent(actual, "", " ")
		expected_bytes, _ := json.MarshalIndent(expected, "", " ")
		t.Error(path.DisplayString(), ":\n", string(actual_bytes), "\nexpected", string(expected_bytes))
	}
}

func runAndTestQuery(query string, expected string, t *testing.T) {
	ses := buildTripleStore()
	c := make(chan interface{}, 5)
	go ses.ExecInput(query, c, -1)
	for result := range c {
		ses.BuildJson(result)
	}
	actual_struct, _ := ses.GetJson()
	var expected_struct interface{}
	json.Unmarshal([]byte(expected), &expected_struct)
	compareJsonInterfaces(actual_struct, expected_struct, NewMqlPath(), t)
	ses.ClearJson()
}

func TestGetAllIds(t *testing.T) {
	Convey("Should get all IDs in the database", t, func() {
		query := `
		[{"id": null}]
		`
		expected := `
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
		`
		runAndTestQuery(query, expected, t)
	})
}

func TestGetCool(t *testing.T) {
	query := `
	[{"id": null, "status": "cool"}]
	`
	expected := `
		[
		{"id": "B", "status": "cool"},
		{"id": "D", "status": "cool"},
		{"id": "G", "status": "cool"}
		]
	`
	runAndTestQuery(query, expected, t)
}

func TestGetFollowsList(t *testing.T) {
	query := `
	[{"id": "C", "follows": []}]
	`
	expected := `
	[{
	 "id": "C",
	 "follows": [
	   "B", "D"
	 ]
  }]
	`
	runAndTestQuery(query, expected, t)
}

func TestGetFollowsStruct(t *testing.T) {
	query := `
	[{"id": null, "follows": {"id": null, "status": "cool"}}]
	`
	expected := `
	[
	{"id": "A", "follows": {"id": "B", "status": "cool"}},
	{"id": "C", "follows": {"id": "D", "status": "cool"}},
	{"id": "D", "follows": {"id": "G", "status": "cool"}},
	{"id": "F", "follows": {"id": "G", "status": "cool"}}
	]
	`
	runAndTestQuery(query, expected, t)
}

func TestGetFollowsReverseStructList(t *testing.T) {
	query := `
	[{"id": null, "!follows": [{"id": null, "status" : "cool"}]}]
	`
	expected := `
	[
	{"id": "F", "!follows": [{"id": "B", "status": "cool"}]},
	{"id": "B", "!follows": [{"id": "D", "status": "cool"}]},
	{"id": "G", "!follows": [{"id": "D", "status": "cool"}]}
	]
	`
	runAndTestQuery(query, expected, t)
}

func TestGetRevFollowsList(t *testing.T) {
	query := `
	[{"id": "F", "!follows": []}]
	`
	expected := `
	[{
	 "id": "F",
	 "!follows": [
	   "B", "E"
	 ]
  }]
	`
	runAndTestQuery(query, expected, t)
}

func TestCoFollows(t *testing.T) {
	query := `
	[{"id": null, "@A:follows": "B", "@B:follows": "D"}]
	`
	expected := `
	[{
	 "id": "C",
	 "@A:follows": "B",
	 "@B:follows": "D"
  }]
	`
	runAndTestQuery(query, expected, t)
}

func TestRevCoFollows(t *testing.T) {
	query := `
	[{"id": null, "!follows": {"id": "C"}, "@a:!follows": "D"}]
	`
	expected := `
	[{
	 "id": "B",
	 "!follows": {"id": "C"},
	 "@a:!follows": "D"
  }]
	`
	runAndTestQuery(query, expected, t)
}
