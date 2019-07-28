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

// Defines one of the base iterators, the All iterator. Which, logically
// enough, represents all nodes or all links in the graph.
//
// This particular file is actually vestigial. It's up to the QuadStore to give
// us an All iterator that represents all things in the graph. So this is
// really the All iterator for the memstore.QuadStore. That said, it *is* one of
// the base iterators, and it helps just to see it here.

type Int64Node int64

func (v Int64Node) Key() interface{} { return v }

func (Int64Node) IsNode() bool { return true }
