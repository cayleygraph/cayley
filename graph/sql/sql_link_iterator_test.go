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

func TestSQLLink(t *testing.T) {
	it := NewSQLLinkIterator(nil, quad.Object, "cool")
	s, v := it.buildSQL(true, nil)
	fmt.Println(s, v)
}

func TestSQLLinkIteration(t *testing.T) {
	if *dbpath == "" {
		t.SkipNow()
	}
	db, err := newQuadStore(*dbpath, nil)
	if err != nil {
		t.Fatal(err)
	}
	it := NewSQLLinkIterator(db.(*QuadStore), quad.Object, "Humphrey Bogart")
	for graph.Next(it) {
		fmt.Println(it.Result())
	}
	it = NewSQLLinkIterator(db.(*QuadStore), quad.Subject, "/en/casablanca_1942")
	s, v := it.buildSQL(true, nil)
	fmt.Println(s, v)
	c := 0
	for graph.Next(it) {
		fmt.Println(it.Result())
		c += 1
	}
	if c != 18 {
		t.Errorf("Not enough results, got %d expected 18", c)
	}
}

func TestSQLNodeIteration(t *testing.T) {
	if *dbpath == "" {
		t.SkipNow()
	}
	db, err := newQuadStore(*dbpath, nil)
	if err != nil {
		t.Fatal(err)
	}
	link := NewSQLLinkIterator(db.(*QuadStore), quad.Object, "/en/humphrey_bogart")
	it := &SQLNodeIterator{
		uid:       iterator.NextUID(),
		qs:        db.(*QuadStore),
		tableName: newTableName(),
		linkIts: []sqlItDir{
			sqlItDir{it: link,
				dir: quad.Subject,
			},
		},
	}
	s, v := it.buildSQL(true, nil)
	fmt.Println(s, v)
	c := 0
	for graph.Next(it) {
		fmt.Println(it.Result())
		c += 1
	}
	if c != 56 {
		t.Errorf("Not enough results, got %d expected 56", c)
	}

}
