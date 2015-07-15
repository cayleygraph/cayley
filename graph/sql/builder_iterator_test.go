// Copyright 2015 The Cayley Authors. All rights reserved.
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
	"flag"
	"fmt"
	"testing"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/quad"
)

var dbpath = flag.String("dbpath", "", "Path to running DB")

func TestSimpleSQL(t *testing.T) {
	it := NewStatementIterator(nil, quad.Object, "cool")
	s, v := it.buildQuery(false, nil)
	fmt.Println(s, v)
}

// Functional tests

func TestQuadIteration(t *testing.T) {
	if *dbpath == "" {
		t.SkipNow()
	}
	db, err := newQuadStore(*dbpath, nil)
	if err != nil {
		t.Fatal(err)
	}
	it := NewStatementIterator(db.(*QuadStore), quad.Object, "Humphrey Bogart")
	for graph.Next(it) {
		fmt.Println(it.Result())
	}
	it = NewStatementIterator(db.(*QuadStore), quad.Subject, "/en/casablanca_1942")
	s, v := it.buildQuery(false, nil)
	fmt.Println(s, v)
	c := 0
	for graph.Next(it) {
		fmt.Println(it.Result())
		c += 1
	}
	if c != 18 {
		t.Errorf("Not enough results, got %d expected 18")
	}
}

func TestNodeIteration(t *testing.T) {
	if *dbpath == "" {
		t.SkipNow()
	}
	db, err := newQuadStore(*dbpath, nil)
	if err != nil {
		t.Fatal(err)
	}
	it := &StatementIterator{
		uid:    iterator.NextUID(),
		qs:     db.(*QuadStore),
		stType: node,
		dir:    quad.Object,
		tags: []tag{
			tag{
				pair: tableDir{
					table: "t_4",
					dir:   quad.Subject,
				},
				t: "x",
			},
		},
		where: baseClause{
			pair: tableDir{
				table: "t_4",
				dir:   quad.Subject,
			},
			strTarget: []string{"/en/casablanca_1942"},
		},
	}
	s, v := it.buildQuery(false, nil)
	it.Tagger().Add("id")
	fmt.Println(s, v)
	for graph.Next(it) {
		fmt.Println(it.Result())
		out := make(map[string]graph.Value)
		it.TagResults(out)
		for k, v := range out {
			fmt.Printf("%s: %v\n", k, v.(string))
		}
	}
	contains := it.Contains("Casablanca")
	s, v = it.buildQuery(true, "Casablanca")
	fmt.Println(s, v)
	it.Tagger().Add("id")
	if !contains {
		t.Error("Didn't contain Casablanca")
	}
}
