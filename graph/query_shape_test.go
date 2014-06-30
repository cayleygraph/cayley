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

package graph

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func buildHasaWithTag(ts TripleStore, tag string, target string) *HasaIterator {
	fixed_obj := ts.MakeFixed()
	fixed_pred := ts.MakeFixed()
	fixed_obj.AddValue(ts.GetIdFor(target))
	fixed_pred.AddValue(ts.GetIdFor("status"))
	fixed_obj.AddTag(tag)
	lto1 := NewLinksToIterator(ts, fixed_obj, Object)
	lto2 := NewLinksToIterator(ts, fixed_pred, Predicate)
	and := NewAndIterator()
	and.AddSubIterator(lto1)
	and.AddSubIterator(lto2)
	hasa := NewHasaIterator(ts, and, Subject)
	return hasa
}

func TestQueryShape(t *testing.T) {
	var queryShape map[string]interface{}
	var ts *TestTripleStore
	ts = new(TestTripleStore)
	ts.On("GetIdFor", "cool").Return(1)
	ts.On("GetNameFor", 1).Return("cool")
	ts.On("GetIdFor", "status").Return(2)
	ts.On("GetNameFor", 2).Return("status")
	ts.On("GetIdFor", "fun").Return(3)
	ts.On("GetNameFor", 3).Return("fun")
	ts.On("GetIdFor", "name").Return(4)
	ts.On("GetNameFor", 4).Return("name")

	Convey("Given a single linkage iterator's shape", t, func() {
		queryShape = make(map[string]interface{})
		hasa := buildHasaWithTag(ts, "tag", "cool")
		hasa.AddTag("top")
		OutputQueryShapeForIterator(hasa, ts, &queryShape)

		Convey("It should have three nodes and one link", func() {
			nodes := queryShape["nodes"].([]Node)
			links := queryShape["links"].([]Link)
			So(len(nodes), ShouldEqual, 3)
			So(len(links), ShouldEqual, 1)
		})

		Convey("These nodes should be correctly tagged", func() {
			nodes := queryShape["nodes"].([]Node)
			So(nodes[0].Tags, ShouldResemble, []string{"tag"})
			So(nodes[1].IsLinkNode, ShouldEqual, true)
			So(nodes[2].Tags, ShouldResemble, []string{"top"})

		})

		Convey("The link should be correctly typed", func() {
			nodes := queryShape["nodes"].([]Node)
			links := queryShape["links"].([]Link)
			So(links[0].Source, ShouldEqual, nodes[2].Id)
			So(links[0].Target, ShouldEqual, nodes[0].Id)
			So(links[0].LinkNode, ShouldEqual, nodes[1].Id)
			So(links[0].Pred, ShouldEqual, 0)

		})

	})

	Convey("Given a name-of-an-and-iterator's shape", t, func() {
		queryShape = make(map[string]interface{})
		hasa1 := buildHasaWithTag(ts, "tag1", "cool")
		hasa1.AddTag("hasa1")
		hasa2 := buildHasaWithTag(ts, "tag2", "fun")
		hasa1.AddTag("hasa2")
		andInternal := NewAndIterator()
		andInternal.AddSubIterator(hasa1)
		andInternal.AddSubIterator(hasa2)
		fixed_pred := ts.MakeFixed()
		fixed_pred.AddValue(ts.GetIdFor("name"))
		lto1 := NewLinksToIterator(ts, andInternal, Subject)
		lto2 := NewLinksToIterator(ts, fixed_pred, Predicate)
		and := NewAndIterator()
		and.AddSubIterator(lto1)
		and.AddSubIterator(lto2)
		hasa := NewHasaIterator(ts, and, Object)
		OutputQueryShapeForIterator(hasa, ts, &queryShape)

		Convey("It should have seven nodes and three links", func() {
			nodes := queryShape["nodes"].([]Node)
			links := queryShape["links"].([]Link)
			So(len(nodes), ShouldEqual, 7)
			So(len(links), ShouldEqual, 3)
		})

		Convey("Three of the nodes are link nodes, four aren't", func() {
			nodes := queryShape["nodes"].([]Node)
			count := 0
			for _, node := range nodes {
				if node.IsLinkNode {
					count++
				}
			}
			So(count, ShouldEqual, 3)
		})

		Convey("These nodes should be correctly tagged", nil)

	})

}
