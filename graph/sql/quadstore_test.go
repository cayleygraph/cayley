// Copyright 2016 The Cayley Authors. All rights reserved.
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

package sql

import (
	"testing"

	"github.com/google/cayley/graph"
)

func TestGetSQLFlavor(t *testing.T) {
	testData := []struct {
		flavor      string
		expected    sqlFlavor
		expectError bool
	}{
		{"postgres", postgres, false},
		{"cockroach", cockroach, false},
		{"nosql", "", true},
		{"", postgres, false},
	}

	for _, td := range testData {
		opts := make(graph.Options)
		if len(td.flavor) > 0 {
			opts["db_sql_flavor"] = td.flavor
		}
		f, err := getSQLFlavor(opts)
		if err != nil && !td.expectError {
			t.Error(err)
		} else if err == nil && td.expectError {
			t.Errorf("getSQLFlavor(%v) expected error", td.flavor)
		} else if td.expected != f {
			t.Errorf("getSQLFlavor(%v) = %v; not %v", td.flavor, f, td.expected)
		}
	}
}
