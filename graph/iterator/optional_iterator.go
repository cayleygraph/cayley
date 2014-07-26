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

// "Optional" is kind of odd. It's not an iterator in the strictest sense, but
// it's easier to implement as an iterator.
//
// Consider what it means. It means that we have a subconstraint which we do
// not want to constrain the query -- we just want it to return the matching
// subgraph if one matches at all. By analogy to regular expressions, it is the
// '?' operator.
//
// If it were a proper iterator of its own (and indeed, a reasonable refactor
// of this iterator would be to make it such) it would contain an all iterator
// -- all things in the graph. It matches everything (as does the regex "(a)?")

import (
	"fmt"
	"strings"

	"github.com/barakmich/glog"

	"github.com/google/cayley/graph"
)

// An optional iterator has the subconstraint iterator we wish to be optional
// and whether the last check we received was true or false.
type Optional struct {
	Base
	subIt     graph.Iterator
	lastCheck bool
}

// Creates a new optional iterator.
func NewOptional(it graph.Iterator) *Optional {
	var o Optional
	BaseInit(&o.Base)
	o.canNext = false
	o.subIt = it
	return &o
}

func (it *Optional) Reset() {
	it.subIt.Reset()
	it.lastCheck = false
}

func (it *Optional) Close() {
	it.subIt.Close()
}

func (it *Optional) Clone() graph.Iterator {
	out := NewOptional(it.subIt.Clone())
	out.CopyTagsFrom(it)
	return out
}

// Nexting the iterator is unsupported -- error and return an empty set.
// (As above, a reasonable alternative would be to Next() an all iterator)
func (it *Optional) Next() (graph.Value, bool) {
	glog.Errorln("Nexting an un-nextable iterator")
	return nil, false
}

// An optional iterator only has a next result if, (a) last time we checked
// we had any results whatsoever, and (b) there was another subresult in our
// optional subbranch.
func (it *Optional) NextResult() bool {
	if it.lastCheck {
		return it.subIt.NextResult()
	}
	return false
}

// Check() is the real hack of this iterator. It always returns true, regardless
// of whether the subiterator matched. But we keep track of whether the subiterator
// matched for results purposes.
func (it *Optional) Check(val graph.Value) bool {
	checked := it.subIt.Check(val)
	it.lastCheck = checked
	it.Last = val
	return true
}

// If we failed the check, then the subiterator should not contribute to the result
// set. Otherwise, go ahead and tag it.
func (it *Optional) TagResults(dst map[string]graph.Value) {
	if it.lastCheck == false {
		return
	}
	it.subIt.TagResults(dst)
}

// Registers the optional iterator.
func (it *Optional) Type() graph.Type { return graph.Optional }

// Prints the optional and it's subiterator.
func (it *Optional) DebugString(indent int) string {
	return fmt.Sprintf("%s(%s tags:%s\n%s)",
		strings.Repeat(" ", indent),
		it.Type(),
		it.Tags(),
		it.subIt.DebugString(indent+4))
}

// There's nothing to optimize for an optional. Optimize the subiterator and
// potentially replace it.
func (it *Optional) Optimize() (graph.Iterator, bool) {
	newSub, changed := it.subIt.Optimize()
	if changed {
		it.subIt.Close()
		it.subIt = newSub
	}
	return it, false
}

// We're only as expensive as our subiterator. Except, we can't be nexted.
func (it *Optional) Stats() graph.IteratorStats {
	subStats := it.subIt.Stats()
	return graph.IteratorStats{
		CheckCost: subStats.CheckCost,
		NextCost:  int64(1 << 62),
		Size:      subStats.Size,
	}
}
