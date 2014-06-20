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
	"github.com/barakmich/glog"
	"strings"
)

// An optional iterator has the subconstraint iterator we wish to be optional
// and whether the last check we received was true or false.
type OptionalIterator struct {
	BaseIterator
	subIt     Iterator
	lastCheck bool
}

// Creates a new optional iterator.
func NewOptionalIterator(it Iterator) *OptionalIterator {
	var o OptionalIterator
	BaseIteratorInit(&o.BaseIterator)
	o.nextable = false
	o.subIt = it
	return &o
}

func (o *OptionalIterator) Reset() {
	o.subIt.Reset()
	o.lastCheck = false
}

func (o *OptionalIterator) Close() {
	o.subIt.Close()
}

func (o *OptionalIterator) Clone() Iterator {
	out := NewOptionalIterator(o.subIt.Clone())
	out.CopyTagsFrom(o)
	return out
}

// Nexting the iterator is unsupported -- error and return an empty set.
// (As above, a reasonable alternative would be to Next() an all iterator)
func (o *OptionalIterator) Next() (TSVal, bool) {
	glog.Errorln("Nexting an un-nextable iterator")
	return nil, false
}

// An optional iterator only has a next result if, (a) last time we checked
// we had any results whatsoever, and (b) there was another subresult in our
// optional subbranch.
func (o *OptionalIterator) NextResult() bool {
	if o.lastCheck {
		return o.subIt.NextResult()
	}
	return false
}

// Check() is the real hack of this iterator. It always returns true, regardless
// of whether the subiterator matched. But we keep track of whether the subiterator
// matched for results purposes.
func (o *OptionalIterator) Check(val TSVal) bool {
	checked := o.subIt.Check(val)
	o.lastCheck = checked
	o.Last = val
	return true
}

// If we failed the check, then the subiterator should not contribute to the result
// set. Otherwise, go ahead and tag it.
func (o *OptionalIterator) TagResults(out *map[string]TSVal) {
	if o.lastCheck == false {
		return
	}
	o.subIt.TagResults(out)
}

// Registers the optional iterator.
func (o *OptionalIterator) Type() string { return "optional" }

// Prints the optional and it's subiterator.
func (o *OptionalIterator) DebugString(indent int) string {
	return fmt.Sprintf("%s(%s tags:%s\n%s)",
		strings.Repeat(" ", indent),
		o.Type(),
		o.Tags(),
		o.subIt.DebugString(indent+4))
}

// There's nothing to optimize for an optional. Optimize the subiterator and
// potentially replace it.
func (o *OptionalIterator) Optimize() (Iterator, bool) {
	newSub, changed := o.subIt.Optimize()
	if changed {
		o.subIt.Close()
		o.subIt = newSub
	}
	return o, false
}

// We're only as expensive as our subiterator. Except, we can't be nexted.
func (o *OptionalIterator) GetStats() *IteratorStats {
	subStats := o.subIt.GetStats()
	return &IteratorStats{
		CheckCost: subStats.CheckCost,
		NextCost:  int64(1 << 62),
		Size:      subStats.Size,
	}
}
