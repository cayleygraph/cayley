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

	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/quad"
)

var postgres_path = flag.String("postgres_path", "", "Path to running DB")

func TestSQLLink(t *testing.T) {
	qs := testQS()
	it := NewSQLLinkIterator(qs, quad.Object, quad.Raw("cool"))
	s, v := it.sql.buildSQL(&qs.flavor, true, nil)
	t.Log(s, v)
}

func TestSQLLinkIteration(t *testing.T) {
	if *postgres_path == "" {
		t.SkipNow()
	}
	db, err := New("postgres", *postgres_path, nil)
	qs := db.(*QuadStore)
	if err != nil {
		t.Fatal(err)
	}
	it := NewSQLLinkIterator(qs, quad.Object, quad.Raw("Humphrey Bogart"))
	for it.Next() {
		fmt.Println(it.Result())
	}
	it = NewSQLLinkIterator(qs, quad.Subject, quad.Raw("/en/casablanca_1942"))
	s, v := it.sql.buildSQL(&qs.flavor, true, nil)
	t.Log(s, v)
	c := 0
	for it.Next() {
		fmt.Println(it.Result())
		c += 1
	}
	if c != 18 {
		t.Errorf("Not enough results, got %d expected 18", c)
	}
}

func TestSQLNodeIteration(t *testing.T) {
	if *postgres_path == "" {
		t.SkipNow()
	}
	db, err := New("postgres", *postgres_path, nil)
	if err != nil {
		t.Fatal(err)
	}
	qs := db.(*QuadStore)
	link := NewSQLLinkIterator(qs, quad.Object, quad.Raw("/en/humphrey_bogart"))
	it := &SQLIterator{
		uid: iterator.NextUID(),
		qs:  qs,
		sql: &SQLNodeIterator{
			tableName: newTableName(),
			linkIt: sqlItDir{
				it:  link.sql,
				dir: quad.Subject,
			},
		},
	}
	s, v := it.sql.buildSQL(&qs.flavor, true, nil)
	t.Log(s, v)
	c := 0
	for it.Next() {
		t.Log(it.Result())
		c += 1
	}
	if c != 56 {
		t.Errorf("Not enough results, got %d expected 56", c)
	}

}
