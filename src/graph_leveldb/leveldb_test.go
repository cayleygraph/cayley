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

package graph_leveldb

import (
	"github.com/google/cayley/src/graph"
	. "github.com/smartystreets/goconvey/convey"
	"io/ioutil"
	"os"
	"sort"
	"testing"
)

func makeTripleSet() []*graph.Triple {
	tripleSet := []*graph.Triple{
		graph.MakeTriple("A", "follows", "B", ""),
		graph.MakeTriple("C", "follows", "B", ""),
		graph.MakeTriple("C", "follows", "D", ""),
		graph.MakeTriple("D", "follows", "B", ""),
		graph.MakeTriple("B", "follows", "F", ""),
		graph.MakeTriple("F", "follows", "G", ""),
		graph.MakeTriple("D", "follows", "G", ""),
		graph.MakeTriple("E", "follows", "F", ""),
		graph.MakeTriple("B", "status", "cool", "status_graph"),
		graph.MakeTriple("D", "status", "cool", "status_graph"),
		graph.MakeTriple("G", "status", "cool", "status_graph"),
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
		output = append(output, ts.GetTriple(val).ToString())
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
		output = append(output, ts.GetNameFor(val))
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
				ts := NewDefaultLevelDBTripleStore(tmpDir, nil)
				So(ts, ShouldNotBeNil)
				So(ts.Size(), ShouldEqual, 0)
				ts.Close()
			})
		})

		Convey("Fails if it cannot create the database", func() {
			ok := CreateNewLevelDB("/dev/null/some terrible path")
			So(ok, ShouldBeFalse)
			So(func() { NewDefaultLevelDBTripleStore("/dev/null/some terrible path", nil) }, ShouldPanic)
		})

		Reset(func() {
			os.RemoveAll(tmpDir)
		})

	})

}

func TestLoadDatabase(t *testing.T) {
	var ts *LevelDBTripleStore

	Convey("Given a created database path", t, func() {
		tmpDir, _ := ioutil.TempDir(os.TempDir(), "cayley_test")
		t.Log(tmpDir)
		ok := CreateNewLevelDB(tmpDir)
		So(ok, ShouldBeTrue)
		ts = NewDefaultLevelDBTripleStore(tmpDir, nil)

		Convey("Can load a single triple", func() {
			ts.AddTriple(graph.MakeTriple("Something", "points_to", "Something Else", "context"))
			So(ts.GetNameFor(ts.GetIdFor("Something")), ShouldEqual, "Something")
			So(ts.Size(), ShouldEqual, 1)
		})

		Convey("Can load many triples", func() {

			ts.AddTripleSet(makeTripleSet())
			So(ts.Size(), ShouldEqual, 11)
			So(ts.GetSizeFor(ts.GetIdFor("B")), ShouldEqual, 5)

			Convey("Can delete triples", func() {
				ts.RemoveTriple(graph.MakeTriple("A", "follows", "B", ""))
				So(ts.Size(), ShouldEqual, 10)
				So(ts.GetSizeFor(ts.GetIdFor("B")), ShouldEqual, 4)
			})
		})

		Reset(func() {
			ts.Close()
			os.RemoveAll(tmpDir)
		})

	})

}

func TestAllIterator(t *testing.T) {
	var ts *LevelDBTripleStore

	Convey("Given a prepared database", t, func() {
		tmpDir, _ := ioutil.TempDir(os.TempDir(), "cayley_test")
		t.Log(tmpDir)
		defer os.RemoveAll(tmpDir)
		ok := CreateNewLevelDB(tmpDir)
		So(ok, ShouldBeTrue)
		ts = NewDefaultLevelDBTripleStore(tmpDir, nil)
		ts.AddTripleSet(makeTripleSet())
		var it graph.Iterator

		Convey("Can create an all iterator for nodes", func() {
			it = ts.GetNodesAllIterator()
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
				So(it.Check(ts.GetIdFor("A")), ShouldBeTrue)
				So(it.Check(ts.GetIdFor("cool")), ShouldBeTrue)
				//So(it.Check(ts.GetIdFor("baller")), ShouldBeFalse)
			})

			Reset(func() {
				it.Reset()
			})
		})

		Convey("Can create an all iterator for edges", func() {
			it := ts.GetTriplesAllIterator()
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
				triple := ts.GetTriple(edge_val)
				set := makeTripleSet()
				var string_set []string
				for _, t := range set {
					string_set = append(string_set, t.ToString())
				}
				So(triple.ToString(), ShouldBeIn, string_set)
			})

			Reset(func() {
				ts.Close()
			})
		})
	})

}

func TestSetIterator(t *testing.T) {
	var ts *LevelDBTripleStore
	var tmpDir string

	Convey("Given a prepared database", t, func() {
		tmpDir, _ = ioutil.TempDir(os.TempDir(), "cayley_test")
		t.Log(tmpDir)
		defer os.RemoveAll(tmpDir)
		ok := CreateNewLevelDB(tmpDir)
		So(ok, ShouldBeTrue)
		ts = NewDefaultLevelDBTripleStore(tmpDir, nil)
		ts.AddTripleSet(makeTripleSet())
		var it graph.Iterator

		Convey("Can create a subject iterator", func() {
			it = ts.GetTripleIterator("s", ts.GetIdFor("C"))

			Convey("Containing the right things", func() {
				expected := []string{
					graph.MakeTriple("C", "follows", "B", "").ToString(),
					graph.MakeTriple("C", "follows", "D", "").ToString(),
				}
				actual := extractTripleFromIterator(ts, it)
				sort.Strings(actual)
				sort.Strings(expected)
				So(actual, ShouldResemble, expected)
			})

			Convey("And checkable", func() {
				and := graph.NewAndIterator()
				and.AddSubIterator(ts.GetTriplesAllIterator())
				and.AddSubIterator(it)

				expected := []string{
					graph.MakeTriple("C", "follows", "B", "").ToString(),
					graph.MakeTriple("C", "follows", "D", "").ToString(),
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
			it = ts.GetTripleIterator("o", ts.GetIdFor("F"))

			Convey("Containing the right things", func() {
				expected := []string{
					graph.MakeTriple("B", "follows", "F", "").ToString(),
					graph.MakeTriple("E", "follows", "F", "").ToString(),
				}
				actual := extractTripleFromIterator(ts, it)
				sort.Strings(actual)
				sort.Strings(expected)
				So(actual, ShouldResemble, expected)
			})

			Convey("Mutually and-checkable", func() {
				and := graph.NewAndIterator()
				and.AddSubIterator(ts.GetTripleIterator("s", ts.GetIdFor("B")))
				and.AddSubIterator(it)

				expected := []string{
					graph.MakeTriple("B", "follows", "F", "").ToString(),
				}
				actual := extractTripleFromIterator(ts, and)
				sort.Strings(actual)
				sort.Strings(expected)
				So(actual, ShouldResemble, expected)
			})

		})

		Convey("Can create a predicate iterator", func() {
			it = ts.GetTripleIterator("p", ts.GetIdFor("status"))

			Convey("Containing the right things", func() {
				expected := []string{
					graph.MakeTriple("B", "status", "cool", "status_graph").ToString(),
					graph.MakeTriple("D", "status", "cool", "status_graph").ToString(),
					graph.MakeTriple("G", "status", "cool", "status_graph").ToString(),
				}
				actual := extractTripleFromIterator(ts, it)
				sort.Strings(actual)
				sort.Strings(expected)
				So(actual, ShouldResemble, expected)
			})

		})

		Convey("Can create a provenance iterator", func() {
			it = ts.GetTripleIterator("c", ts.GetIdFor("status_graph"))

			Convey("Containing the right things", func() {
				expected := []string{
					graph.MakeTriple("B", "status", "cool", "status_graph").ToString(),
					graph.MakeTriple("D", "status", "cool", "status_graph").ToString(),
					graph.MakeTriple("G", "status", "cool", "status_graph").ToString(),
				}
				actual := extractTripleFromIterator(ts, it)
				sort.Strings(actual)
				sort.Strings(expected)
				So(actual, ShouldResemble, expected)
			})

			Convey("Can be cross-checked", func() {
				and := graph.NewAndIterator()
				// Order is important
				and.AddSubIterator(ts.GetTripleIterator("s", ts.GetIdFor("B")))
				and.AddSubIterator(it)

				expected := []string{
					graph.MakeTriple("B", "status", "cool", "status_graph").ToString(),
				}
				actual := extractTripleFromIterator(ts, and)
				So(actual, ShouldResemble, expected)
			})

			Convey("Can check against other iterators", func() {
				and := graph.NewAndIterator()
				// Order is important
				and.AddSubIterator(it)
				and.AddSubIterator(ts.GetTripleIterator("s", ts.GetIdFor("B")))

				expected := []string{
					graph.MakeTriple("B", "status", "cool", "status_graph").ToString(),
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
	var ts *LevelDBTripleStore
	var lto graph.Iterator
	var tmpDir string

	Convey("Given a prepared database", t, func() {
		tmpDir, _ = ioutil.TempDir(os.TempDir(), "cayley_test")
		t.Log(tmpDir)
		defer os.RemoveAll(tmpDir)
		ok := CreateNewLevelDB(tmpDir)
		So(ok, ShouldBeTrue)
		ts = NewDefaultLevelDBTripleStore(tmpDir, nil)
		ts.AddTripleSet(makeTripleSet())

		Convey("With an linksto-fixed pair", func() {
			fixed := ts.MakeFixed()
			fixed.AddValue(ts.GetIdFor("F"))
			fixed.AddTag("internal")
			lto = graph.NewLinksToIterator(ts, fixed, "o")

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
					oldResults := make(map[string]graph.TSVal)
					oldIt.TagResults(&oldResults)
					newResults := make(map[string]graph.TSVal)
					oldIt.TagResults(&newResults)
					So(newResults, ShouldResemble, oldResults)
				})

			})

		})

	})

}
