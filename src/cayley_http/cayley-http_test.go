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

package cayley_http

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestParseJSONOkay(t *testing.T) {
	Convey("Parse JSON", t, func() {
		bytelist := []byte(`[
			{"subject": "foo", "predicate": "bar", "object": "baz"},
			{"subject": "foo", "predicate": "bar", "object": "baz", "provenance": "graph"}
	]`)
		x, err := ParseJsonToTripleList(bytelist)
		So(err, ShouldBeNil)
		So(len(x), ShouldEqual, 2)
		So(x[0].Sub, ShouldEqual, "foo")
		So(x[0].Provenance, ShouldEqual, "")
		So(x[1].Provenance, ShouldEqual, "graph")
	})

	Convey("Parse JSON extra field", t, func() {
		bytelist := []byte(`[
		{"subject": "foo", "predicate": "bar", "object": "foo", "something_else": "extra data"}
	]`)
		_, err := ParseJsonToTripleList(bytelist)
		So(err, ShouldBeNil)
	})
}

func TestParseJSONFail(t *testing.T) {
	Convey("Parse JSON Fail", t, func() {
		bytelist := []byte(`[
			{"subject": "foo", "predicate": "bar"}
	]`)
		_, err := ParseJsonToTripleList(bytelist)
		So(err, ShouldNotBeNil)
	})
}
