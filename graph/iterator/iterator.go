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

// Define the general iterator interface.

import (
	"context"
	"fmt"

	"github.com/cayleygraph/cayley/graph"
)

var (
	_ graph.Iterator = &Null{}
	_ graph.Iterator = &Error{}
)

type Morphism func(graph.Iterator) graph.Iterator

// Here we define the simplest iterator -- the Null iterator. It contains nothing.
// It is the empty set. Often times, queries that contain one of these match nothing,
// so it's important to give it a special iterator.
type Null struct{}

// Fairly useless New function.
func NewNull() *Null {
	return &Null{}
}

// Fill the map based on the tags assigned to this iterator.
func (it *Null) TagResults(dst map[string]graph.Ref) {}

func (it *Null) Contains(ctx context.Context, v graph.Ref) bool {
	return false
}

// A good iterator will close itself when it returns true.
// Null has nothing it needs to do.
func (it *Null) Optimize() (graph.Iterator, bool) { return it, false }

func (it *Null) String() string {
	return "Null"
}

func (it *Null) Next(ctx context.Context) bool {
	return false
}

func (it *Null) Err() error {
	return nil
}

func (it *Null) Result() graph.Ref {
	return nil
}

func (it *Null) SubIterators() []graph.Iterator {
	return nil
}

func (it *Null) NextPath(ctx context.Context) bool {
	return false
}

func (it *Null) Size() (int64, bool) {
	return 0, true
}

func (it *Null) Reset() {}

func (it *Null) Close() error {
	return nil
}

// A null iterator costs nothing. Use it!
func (it *Null) Stats() graph.IteratorStats {
	return graph.IteratorStats{}
}

// Error iterator always returns a single error with no other results.
type Error struct {
	err error
}

func NewError(err error) *Error {
	return &Error{err: err}
}

// Fill the map based on the tags assigned to this iterator.
func (it *Error) TagResults(dst map[string]graph.Ref) {}

func (it *Error) Contains(ctx context.Context, v graph.Ref) bool {
	return false
}

func (it *Error) Optimize() (graph.Iterator, bool) { return it, false }

func (it *Error) String() string {
	return fmt.Sprintf("Error(%v)", it.err)
}

func (it *Error) Next(ctx context.Context) bool {
	return false
}

func (it *Error) Err() error {
	return it.err
}

func (it *Error) Result() graph.Ref {
	return nil
}

func (it *Error) SubIterators() []graph.Iterator {
	return nil
}

func (it *Error) NextPath(ctx context.Context) bool {
	return false
}

func (it *Error) Size() (int64, bool) {
	return 0, true
}

func (it *Error) Reset() {}

func (it *Error) Close() error {
	return it.err
}

func (it *Error) Stats() graph.IteratorStats {
	return graph.IteratorStats{}
}
