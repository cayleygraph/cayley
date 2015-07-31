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
	"testing"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/quad"
)

func TestBuildIntersect(t *testing.T) {
	a := NewSQLLinkIterator(nil, quad.Subject, "Foo")
	b := NewSQLLinkIterator(nil, quad.Predicate, "is_equivalent_to")
	it, err := intersect(a.sql, b.sql, nil)
	if err != nil {
		t.Error(err)
	}
	s, v := it.sql.buildSQL(true, nil)
	t.Log(s, v)
}

func TestBuildHasa(t *testing.T) {
	a := NewSQLLinkIterator(nil, quad.Subject, "Foo")
	a.Tagger().Add("foo")
	b := NewSQLLinkIterator(nil, quad.Predicate, "is_equivalent_to")
	it1, err := intersect(a.sql, b.sql, nil)
	if err != nil {
		t.Error(err)
	}
	it2, err := hasa(it1.sql, quad.Object, nil)
	if err != nil {
		t.Error(err)
	}
	s, v := it2.sql.buildSQL(true, nil)
	t.Log(s, v)
}

func TestBuildLinksTo(t *testing.T) {
	a := NewSQLLinkIterator(nil, quad.Subject, "Foo")
	b := NewSQLLinkIterator(nil, quad.Predicate, "is_equivalent_to")
	it1, err := intersect(a.sql, b.sql, nil)
	if err != nil {
		t.Error(err)
	}
	it2, err := hasa(it1.sql, quad.Object, nil)
	it2.Tagger().Add("foo")
	if err != nil {
		t.Error(err)
	}
	it3, err := linksto(it2.sql, quad.Subject, nil)
	if err != nil {
		t.Error(err)
	}
	s, v := it3.sql.buildSQL(true, nil)
	t.Log(s, v)
}

func TestInterestingQuery(t *testing.T) {
	if *postgres_path == "" {
		t.SkipNow()
	}
	db, err := newQuadStore(*postgres_path, nil)
	if err != nil {
		t.Fatal(err)
	}
	qs := db.(*QuadStore)
	a := NewSQLLinkIterator(qs, quad.Object, "Humphrey Bogart")
	b := NewSQLLinkIterator(qs, quad.Predicate, "name")
	it1, err := intersect(a.sql, b.sql, qs)
	if err != nil {
		t.Error(err)
	}
	it2, err := hasa(it1.sql, quad.Subject, qs)
	if err != nil {
		t.Error(err)
	}
	it2.Tagger().Add("hb")
	it3, err := linksto(it2.sql, quad.Object, qs)
	if err != nil {
		t.Error(err)
	}
	b = NewSQLLinkIterator(db.(*QuadStore), quad.Predicate, "/film/performance/actor")
	it4, err := intersect(it3.sql, b.sql, qs)
	if err != nil {
		t.Error(err)
	}
	it5, err := hasa(it4.sql, quad.Subject, qs)
	if err != nil {
		t.Error(err)
	}
	it6, err := linksto(it5.sql, quad.Object, qs)
	if err != nil {
		t.Error(err)
	}
	b = NewSQLLinkIterator(db.(*QuadStore), quad.Predicate, "/film/film/starring")
	it7, err := intersect(it6.sql, b.sql, qs)
	if err != nil {
		t.Error(err)
	}
	it8, err := hasa(it7.sql, quad.Subject, qs)
	if err != nil {
		t.Error(err)
	}
	s, v := it8.sql.buildSQL(true, nil)
	it8.Tagger().Add("id")
	t.Log(s, v)
	for graph.Next(it8) {
		t.Log(it8.Result())
		out := make(map[string]graph.Value)
		it8.TagResults(out)
		for k, v := range out {
			t.Log("%s: %v\n", k, v.(string))
		}
	}
}
