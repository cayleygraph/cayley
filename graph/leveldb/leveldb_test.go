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

package leveldb

import (
	"io/ioutil"
	"os"
	"sort"
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
)

func makeTripleSet() []*graph.Triple {
	tripleSet := []*graph.Triple{
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
	return tripleSet
}

func extractTripleFromIterator(ts graph.TripleStore, it graph.Iterator) []string {
	var output []string
	for {
		val, ok := it.Next()
		if !ok {
			break
		}
		output = append(output, ts.Triple(val).String())
	}
	return output
}

func extractValuesFromIterator(ts graph.TripleStore, it graph.Iterator) []string {
	var output []string
	for {
		val, ok := it.Next()
		if !ok {
			break
		}
		output = append(output, ts.NameOf(val))
	}
	return output
}

func TestCreateDatabase(t *testing.T) {

	Convey("Given a database path", t, func() {
		tmpDir, err := ioutil.TempDir(os.TempDir(), "cayley_test")
		t.Log(tmpDir)
		if err != nil {
			t.Fatal("Cannot use ioutil.", err)
		}

		Convey("Creates a database", func() {
			ok := CreateNewLevelDB(tmpDir)
			So(ok, ShouldBeTrue)
			Convey("And has good defaults for a new database", func() {
				ts := NewTripleStore(tmpDir, nil)
				So(ts, ShouldNotBeNil)
				So(ts.Size(), ShouldEqual, 0)
				ts.Close()
			})
		})

		Convey("Fails if it cannot create the database", func() {
			ok := CreateNewLevelDB("/dev/null/some terrible path")
			So(ok, ShouldBeFalse)
			So(func() { NewTripleStore("/dev/null/some terrible path", nil) }, ShouldPanic)
		})

		Reset(func() {
			os.RemoveAll(tmpDir)
		})

	})

}

func TestLoadDatabase(t *testing.T) {
	var ts *TripleStore

	Convey("Given a created database path", t, func() {
		tmpDir, _ := ioutil.TempDir(os.TempDir(), "cayley_test")
		t.Log(tmpDir)
		ok := CreateNewLevelDB(tmpDir)
		So(ok, ShouldBeTrue)
		ts = NewTripleStore(tmpDir, nil)

		Convey("Can load a single triple", func() {
			ts.AddTriple(&graph.Triple{"Something", "points_to", "Something Else", "context"})
			So(ts.NameOf(ts.ValueOf("Something")), ShouldEqual, "Something")
			So(ts.Size(), ShouldEqual, 1)
		})

		Convey("Can load many triples", func() {

			ts.AddTripleSet(makeTripleSet())
			So(ts.Size(), ShouldEqual, 11)
			So(ts.GetSizeFor(ts.ValueOf("B")), ShouldEqual, 5)

			Convey("Can delete triples", func() {
				ts.RemoveTriple(&graph.Triple{"A", "follows", "B", ""})
				So(ts.Size(), ShouldEqual, 10)
				So(ts.GetSizeFor(ts.ValueOf("B")), ShouldEqual, 4)
			})
		})

		Reset(func() {
			ts.Close()
			os.RemoveAll(tmpDir)
		})

	})

}

func TestIterator(t *testing.T) {
	var ts *TripleStore

	Convey("Given a prepared database", t, func() {
		tmpDir, _ := ioutil.TempDir(os.TempDir(), "cayley_test")
		t.Log(tmpDir)
		defer os.RemoveAll(tmpDir)
		ok := CreateNewLevelDB(tmpDir)
		So(ok, ShouldBeTrue)
		ts = NewTripleStore(tmpDir, nil)
		ts.AddTripleSet(makeTripleSet())
		var it graph.Iterator

		Convey("Can create an all iterator for nodes", func() {
			it = ts.NodesAllIterator()
			So(it, ShouldNotBeNil)

			Convey("Has basics", func() {
				size, accurate := it.Size()
				So(size, ShouldBeBetween, 0, 20)
				So(accurate, ShouldBeFalse)
				So(it.Type(), ShouldEqual, "all")
				re_it, ok := it.Optimize()
				So(ok, ShouldBeFalse)
				So(re_it, ShouldPointTo, it)
			})

			Convey("Iterates all nodes", func() {
				expected := []string{
					"A",
					"B",
					"C",
					"D",
					"E",
					"F",
					"G",
					"follows",
					"status",
					"cool",
					"status_graph",
				}
				sort.Strings(expected)
				actual := extractValuesFromIterator(ts, it)
				sort.Strings(actual)
				So(actual, ShouldResemble, expected)
				it.Reset()
				actual = extractValuesFromIterator(ts, it)
				sort.Strings(actual)
				So(actual, ShouldResemble, expected)

			})

			Convey("Contains a couple nodes", func() {
				So(it.Check(ts.ValueOf("A")), ShouldBeTrue)
				So(it.Check(ts.ValueOf("cool")), ShouldBeTrue)
				//So(it.Check(ts.ValueOf("baller")), ShouldBeFalse)
			})

			Reset(func() {
				it.Reset()
			})
		})

		Convey("Can create an all iterator for edges", func() {
			it := ts.TriplesAllIterator()
			So(it, ShouldNotBeNil)
			Convey("Has basics", func() {
				size, accurate := it.Size()
				So(size, ShouldBeBetween, 0, 20)
				So(accurate, ShouldBeFalse)
				So(it.Type(), ShouldEqual, "all")
				re_it, ok := it.Optimize()
				So(ok, ShouldBeFalse)
				So(re_it, ShouldPointTo, it)
			})

			Convey("Iterates an edge", func() {
				edge_val, _ := it.Next()
				triple := ts.Triple(edge_val)
				set := makeTripleSet()
				var string_set []string
				for _, t := range set {
					string_set = append(string_set, t.String())
				}
				So(triple.String(), ShouldBeIn, string_set)
			})

			Reset(func() {
				ts.Close()
			})
		})
	})

}

func TestSetIterator(t *testing.T) {
	var ts *TripleStore
	var tmpDir string

	Convey("Given a prepared database", t, func() {
		tmpDir, _ = ioutil.TempDir(os.TempDir(), "cayley_test")
		t.Log(tmpDir)
		defer os.RemoveAll(tmpDir)
		ok := CreateNewLevelDB(tmpDir)
		So(ok, ShouldBeTrue)
		ts = NewTripleStore(tmpDir, nil)
		ts.AddTripleSet(makeTripleSet())
		var it graph.Iterator

		Convey("Can create a subject iterator", func() {
			it = ts.TripleIterator(graph.Subject, ts.ValueOf("C"))

			Convey("Containing the right things", func() {
				expected := []string{
					(&graph.Triple{"C", "follows", "B", ""}).String(),
					(&graph.Triple{"C", "follows", "D", ""}).String(),
				}
				actual := extractTripleFromIterator(ts, it)
				sort.Strings(actual)
				sort.Strings(expected)
				So(actual, ShouldResemble, expected)
			})

			Convey("And checkable", func() {
				and := iterator.NewAnd()
				and.AddSubIterator(ts.TriplesAllIterator())
				and.AddSubIterator(it)

				expected := []string{
					(&graph.Triple{"C", "follows", "B", ""}).String(),
					(&graph.Triple{"C", "follows", "D", ""}).String(),
				}
				actual := extractTripleFromIterator(ts, and)
				sort.Strings(actual)
				sort.Strings(expected)
				So(actual, ShouldResemble, expected)
			})
			Reset(func() {
				it.Reset()
			})

		})

		Convey("Can create an object iterator", func() {
			it = ts.TripleIterator(graph.Object, ts.ValueOf("F"))

			Convey("Containing the right things", func() {
				expected := []string{
					(&graph.Triple{"B", "follows", "F", ""}).String(),
					(&graph.Triple{"E", "follows", "F", ""}).String(),
				}
				actual := extractTripleFromIterator(ts, it)
				sort.Strings(actual)
				sort.Strings(expected)
				So(actual, ShouldResemble, expected)
			})

			Convey("Mutually and-checkable", func() {
				and := iterator.NewAnd()
				and.AddSubIterator(ts.TripleIterator(graph.Subject, ts.ValueOf("B")))
				and.AddSubIterator(it)

				expected := []string{
					(&graph.Triple{"B", "follows", "F", ""}).String(),
				}
				actual := extractTripleFromIterator(ts, and)
				sort.Strings(actual)
				sort.Strings(expected)
				So(actual, ShouldResemble, expected)
			})

		})

		Convey("Can create a predicate iterator", func() {
			it = ts.TripleIterator(graph.Predicate, ts.ValueOf("status"))

			Convey("Containing the right things", func() {
				expected := []string{
					(&graph.Triple{"B", "status", "cool", "status_graph"}).String(),
					(&graph.Triple{"D", "status", "cool", "status_graph"}).String(),
					(&graph.Triple{"G", "status", "cool", "status_graph"}).String(),
				}
				actual := extractTripleFromIterator(ts, it)
				sort.Strings(actual)
				sort.Strings(expected)
				So(actual, ShouldResemble, expected)
			})

		})

		Convey("Can create a provenance iterator", func() {
			it = ts.TripleIterator(graph.Provenance, ts.ValueOf("status_graph"))

			Convey("Containing the right things", func() {
				expected := []string{
					(&graph.Triple{"B", "status", "cool", "status_graph"}).String(),
					(&graph.Triple{"D", "status", "cool", "status_graph"}).String(),
					(&graph.Triple{"G", "status", "cool", "status_graph"}).String(),
				}
				actual := extractTripleFromIterator(ts, it)
				sort.Strings(actual)
				sort.Strings(expected)
				So(actual, ShouldResemble, expected)
			})

			Convey("Can be cross-checked", func() {
				and := iterator.NewAnd()
				// Order is important
				and.AddSubIterator(ts.TripleIterator(graph.Subject, ts.ValueOf("B")))
				and.AddSubIterator(it)

				expected := []string{
					(&graph.Triple{"B", "status", "cool", "status_graph"}).String(),
				}
				actual := extractTripleFromIterator(ts, and)
				So(actual, ShouldResemble, expected)
			})

			Convey("Can check against other iterators", func() {
				and := iterator.NewAnd()
				// Order is important
				and.AddSubIterator(it)
				and.AddSubIterator(ts.TripleIterator(graph.Subject, ts.ValueOf("B")))

				expected := []string{
					(&graph.Triple{"B", "status", "cool", "status_graph"}).String(),
				}
				actual := extractTripleFromIterator(ts, and)
				So(actual, ShouldResemble, expected)
			})
			Reset(func() {
				it.Reset()
			})

		})

		Reset(func() {
			ts.Close()
		})

	})

}

func TestOptimize(t *testing.T) {
	var ts *TripleStore
	var lto graph.Iterator
	var tmpDir string

	Convey("Given a prepared database", t, func() {
		tmpDir, _ = ioutil.TempDir(os.TempDir(), "cayley_test")
		t.Log(tmpDir)
		defer os.RemoveAll(tmpDir)
		ok := CreateNewLevelDB(tmpDir)
		So(ok, ShouldBeTrue)
		ts = NewTripleStore(tmpDir, nil)
		ts.AddTripleSet(makeTripleSet())

		Convey("With an linksto-fixed pair", func() {
			fixed := ts.FixedIterator()
			fixed.Add(ts.ValueOf("F"))
			fixed.AddTag("internal")
			lto = iterator.NewLinksTo(ts, fixed, graph.Object)

			Convey("Creates an appropriate iterator", func() {
				oldIt := lto.Clone()
				newIt, ok := lto.Optimize()
				So(ok, ShouldBeTrue)
				So(newIt.Type(), ShouldEqual, "leveldb")

				Convey("Containing the right things", func() {
					afterOp := extractTripleFromIterator(ts, newIt)
					beforeOp := extractTripleFromIterator(ts, oldIt)
					sort.Strings(afterOp)
					sort.Strings(beforeOp)
					So(afterOp, ShouldResemble, beforeOp)
				})

				Convey("With the correct tags", func() {
					oldIt.Next()
					newIt.Next()
					oldResults := make(map[string]graph.Value)
					oldIt.TagResults(oldResults)
					newResults := make(map[string]graph.Value)
					oldIt.TagResults(newResults)
					So(newResults, ShouldResemble, oldResults)
				})

			})

		})

	})

}
