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
	"fmt"
	"testing"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/quad"
)

func TestBuildIntersect(t *testing.T) {
	a := NewSQLLinkIterator(nil, quad.Subject, "Foo")
	b := NewSQLLinkIterator(nil, quad.Predicate, "is_equivalent_to")
	it, err := intersect(a, b)
	i := it.(*SQLLinkIterator)
	if err != nil {
		t.Error(err)
	}
	s, v := i.buildSQL(true, nil)
	fmt.Println(s, v)
}

func TestBuildHasa(t *testing.T) {
	a := NewSQLLinkIterator(nil, quad.Subject, "Foo")
	a.tagger.Add("foo")
	b := NewSQLLinkIterator(nil, quad.Predicate, "is_equivalent_to")
	it1, err := intersect(a, b)
	if err != nil {
		t.Error(err)
	}
	it2, err := hasa(it1, quad.Object)
	i2 := it2.(*SQLNodeIterator)
	if err != nil {
		t.Error(err)
	}
	s, v := i2.buildSQL(true, nil)
	fmt.Println(s, v)
}

func TestBuildLinksTo(t *testing.T) {
	a := NewSQLLinkIterator(nil, quad.Subject, "Foo")
	b := NewSQLLinkIterator(nil, quad.Predicate, "is_equivalent_to")
	it1, err := intersect(a, b)
	if err != nil {
		t.Error(err)
	}
	it2, err := hasa(it1, quad.Object)
	it2.Tagger().Add("foo")
	if err != nil {
		t.Error(err)
	}
	it3, err := linksto(it2, quad.Subject)
	if err != nil {
		t.Error(err)
	}
	i3 := it3.(*SQLLinkIterator)
	s, v := i3.buildSQL(true, nil)
	fmt.Println(s, v)
}

func TestInterestingQuery(t *testing.T) {
	if *dbpath == "" {
		t.SkipNow()
	}
	db, err := newQuadStore(*dbpath, nil)
	if err != nil {
		t.Fatal(err)
	}
	a := NewSQLLinkIterator(db.(*QuadStore), quad.Object, "Humphrey Bogart")
	b := NewSQLLinkIterator(db.(*QuadStore), quad.Predicate, "name")
	it1, err := intersect(a, b)
	if err != nil {
		t.Error(err)
	}
	it2, err := hasa(it1, quad.Subject)
	if err != nil {
		t.Error(err)
	}
	it2.Tagger().Add("hb")
	it3, err := linksto(it2, quad.Object)
	if err != nil {
		t.Error(err)
	}
	b = NewSQLLinkIterator(db.(*QuadStore), quad.Predicate, "/film/performance/actor")
	it4, err := intersect(it3, b)
	if err != nil {
		t.Error(err)
	}
	it5, err := hasa(it4, quad.Subject)
	if err != nil {
		t.Error(err)
	}
	it6, err := linksto(it5, quad.Object)
	if err != nil {
		t.Error(err)
	}
	b = NewSQLLinkIterator(db.(*QuadStore), quad.Predicate, "/film/film/starring")
	it7, err := intersect(it6, b)
	if err != nil {
		t.Error(err)
	}
	it8, err := hasa(it7, quad.Subject)
	if err != nil {
		t.Error(err)
	}
	finalIt := it8.(*SQLNodeIterator)
	s, v := finalIt.buildSQL(true, nil)
	finalIt.Tagger().Add("id")
	fmt.Println(s, v)
	for graph.Next(finalIt) {
		fmt.Println(finalIt.Result())
		out := make(map[string]graph.Value)
		finalIt.TagResults(out)
		for k, v := range out {
			fmt.Printf("%s: %v\n", k, v.(string))
		}
	}
}
