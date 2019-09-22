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

package nquads

import (
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/nquads"
)

// ParseTyped returns a valid quad.Quad or a non-nil error. ParseTyped does
// handle comments except where the comment placement does not prevent
// a complete valid quad.Quad from being defined.
//
// Deprecated: use github.com/cayleygraph/quad/nquads package instead.
func Parse(statement string) (quad.Quad, error) {
	return nquads.Parse(statement)
}
