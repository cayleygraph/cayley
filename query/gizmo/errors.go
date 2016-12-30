// Copyright 2017 The Cayley Authors. All rights reserved.
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

package gizmo

import "fmt"

var (
	errNoVia       = fmt.Errorf("expected predicate list")
	errRegexpOnIRI = fmt.Errorf("regexps are not allowed on IRIs")
)

type errArgCount2 struct {
	Expected int
	Got      int
}

func (e errArgCount2) Error() string {
	return fmt.Sprintf("expected %d argument, got %d", e.Expected, e.Got)
}

type errArgCount struct {
	Got int
}

func (e errArgCount) Error() string {
	return fmt.Sprintf("unexpected arguments count: %d", e.Got)
}

type errNotQuadValue struct {
	Val interface{}
}

func (e errNotQuadValue) Error() string {
	return fmt.Sprintf("not a quad.Value: %T", e.Val)
}
