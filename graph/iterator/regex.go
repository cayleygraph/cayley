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

package iterator

import (
	"regexp"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/quad"
)

func newRegex(qs graph.Namer, sub graph.Iterator, re *regexp.Regexp, refs bool) graph.Iterator {
	return NewValueFilter(qs, sub, func(v quad.Value) (bool, error) {
		switch v := v.(type) {
		case quad.String:
			return re.MatchString(string(v)), nil
		case quad.LangString:
			return re.MatchString(string(v.Value)), nil
		case quad.TypedString:
			return re.MatchString(string(v.Value)), nil
		default:
			if refs {
				switch v := v.(type) {
				case quad.BNode:
					return re.MatchString(string(v)), nil
				case quad.IRI:
					return re.MatchString(string(v)), nil
				}
			}
		}
		return false, nil
	})
}

// NewRegex returns an unary operator -- a filter across the values in the relevant
// subiterator. It works similarly to gremlin's filter{it.matches('exp')},
// reducing the iterator set to values whose string representation passes a
// regular expression test.
func NewRegex(sub graph.Iterator, re *regexp.Regexp, qs graph.Namer) graph.Iterator {
	return newRegex(qs, sub, re, false)
}

// NewRegexWithRefs is like NewRegex but allows regexp iterator to match IRIs and BNodes.
//
// Consider using it carefully. In most cases it's better to reconsider
// your graph structure instead of relying on slow unoptimizable regexp.
//
// An example of incorrect usage is to match IRIs:
// 	<http://example.org/page>
// 	<http://example.org/page/foo>
// Via regexp like:
//	http://example.org/page.*
//
// The right way is to explicitly link graph nodes and query them by this relation:
// 	<http://example.org/page/foo> <type> <http://example.org/page>
func NewRegexWithRefs(sub graph.Iterator, re *regexp.Regexp, qs graph.Namer) graph.Iterator {
	return newRegex(qs, sub, re, true)
}
