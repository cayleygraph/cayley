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

package memstore

import "github.com/google/cayley/graph"

//    +---+                        +---+
//    | A |-------               ->| F |<--
//    +---+       \------>+---+-/  +---+   \--+---+
//                 ------>|#B#|      |        | E |
//    +---+-------/      >+---+      |        +---+
//    | C |             /            v
//    +---+           -/           +---+
//      ----    +---+/             |#G#|
//          \-->|#D#|------------->+---+
//              +---+
//

func MakeTestingMemstore() *TripleStore {
	ts := NewTripleStore()
	ts.AddTriple(graph.MakeTriple("A", "follows", "B", ""))
	ts.AddTriple(graph.MakeTriple("C", "follows", "B", ""))
	ts.AddTriple(graph.MakeTriple("C", "follows", "D", ""))
	ts.AddTriple(graph.MakeTriple("D", "follows", "B", ""))
	ts.AddTriple(graph.MakeTriple("B", "follows", "F", ""))
	ts.AddTriple(graph.MakeTriple("F", "follows", "G", ""))
	ts.AddTriple(graph.MakeTriple("D", "follows", "G", ""))
	ts.AddTriple(graph.MakeTriple("E", "follows", "F", ""))
	ts.AddTriple(graph.MakeTriple("B", "status", "cool", "status_graph"))
	ts.AddTriple(graph.MakeTriple("D", "status", "cool", "status_graph"))
	ts.AddTriple(graph.MakeTriple("G", "status", "cool", "status_graph"))
	return ts
}
