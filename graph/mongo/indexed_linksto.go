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

package mongo

import (
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"fmt"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/quad"
)

var _ graph.Iterator = &LinksTo{}

// LinksTo is a MongoDB-dependent version of a LinksTo iterator. Like the normal
// LinksTo, it represents a set of links to a set of nodes, represented by its
// subiterator. However, this iterator may often be faster than the generic
// LinksTo, as it can use the secondary indices in Mongo as features within the
// Mongo query, reducing the size of the result set and speeding up iteration.
type LinksTo struct {
	uid        uint64
	collection string
	tags       graph.Tagger
	qs         *QuadStore
	primaryIt  graph.Iterator
	dir        quad.Direction
	nextIt     *mgo.Iter
	result     graph.Value
	runstats   graph.IteratorStats
	lset       []graph.Linkage
	err        error
}

// NewLinksTo constructs a new indexed LinksTo iterator for Mongo around a direction
// and a subiterator of nodes.
func NewLinksTo(qs *QuadStore, it graph.Iterator, collection string, d quad.Direction, lset []graph.Linkage) *LinksTo {
	return &LinksTo{
		uid:        iterator.NextUID(),
		qs:         qs,
		primaryIt:  it,
		dir:        d,
		nextIt:     nil,
		lset:       lset,
		collection: collection,
	}
}

func (it *LinksTo) buildConstraint() bson.M {
	constraint := bson.M{}
	// TODO(barakmich): Currently this only works for an individual linkage in any direction.
	for _, link := range it.lset {
		constraint[link.Dir.String()] = link.Value
	}
	return constraint
}

func (it *LinksTo) buildIteratorFor(d quad.Direction, val graph.Value) *mgo.Iter {
	constraint := it.buildConstraint()
	constraint[d.String()] = string(val.(NodeHash))
	return it.qs.db.C(it.collection).Find(constraint).Iter()
}

func (it *LinksTo) UID() uint64 {
	return it.uid
}

func (it *LinksTo) Tagger() *graph.Tagger {
	return &it.tags
}

// Return the direction under consideration.
func (it *LinksTo) Direction() quad.Direction { return it.dir }

// Tag these results, and our subiterator's results.
func (it *LinksTo) TagResults(dst map[string]graph.Value) {
	it.tags.TagResult(dst, it.Result())

	it.primaryIt.TagResults(dst)
}

// Optimize the LinksTo, by replacing it if it can be.
func (it *LinksTo) Optimize() (graph.Iterator, bool) {
	return it, false
}

func (it *LinksTo) Next() bool {
	var result struct {
		ID      string     `bson:"_id"`
		Added   []bson.Raw `bson:"Added"`
		Deleted []bson.Raw `bson:"Deleted"`
	}
	graph.NextLogIn(it)
next:
	for {
		it.runstats.Next += 1
		if it.nextIt != nil && it.nextIt.Next(&result) {
			it.runstats.ContainsNext += 1
			if it.collection == "quads" && len(result.Added) <= len(result.Deleted) {
				continue next
			}
			it.result = QuadHash(result.ID)
			return graph.NextLogOut(it, true)
		}

		if it.nextIt != nil {
			// If there's an error in the 'next' iterator, we save it and we're done.
			it.err = it.nextIt.Err()
			if it.err != nil {
				return false
			}

		}
		// Subiterator is empty, get another one
		if !it.primaryIt.Next() {
			// Possibly save error
			it.err = it.primaryIt.Err()

			// We're out of nodes in our subiterator, so we're done as well.
			return graph.NextLogOut(it, false)
		}
		if it.nextIt != nil {
			it.nextIt.Close()
		}
		it.nextIt = it.buildIteratorFor(it.dir, it.primaryIt.Result())

		// Recurse -- return the first in the next set.
	}
}

func (it *LinksTo) Err() error {
	return it.err
}

func (it *LinksTo) Result() graph.Value {
	return it.result
}

func (it *LinksTo) Close() error {
	var err error
	if it.nextIt != nil {
		err = it.nextIt.Close()
	}

	_err := it.primaryIt.Close()
	if _err != nil && err == nil {
		err = _err
	}

	return err
}

func (it *LinksTo) NextPath() bool {
	ok := it.primaryIt.NextPath()
	if !ok {
		it.err = it.primaryIt.Err()
	}
	return ok
}

func (it *LinksTo) Type() graph.Type {
	return "mongo-linksto"
}

func (it *LinksTo) Clone() graph.Iterator {
	m := NewLinksTo(it.qs, it.primaryIt.Clone(), it.collection, it.dir, it.lset)
	m.tags.CopyFrom(it)
	return m
}

func (it *LinksTo) Contains(val graph.Value) bool {
	graph.ContainsLogIn(it, val)
	it.runstats.Contains += 1

	for _, link := range it.lset {
		dval := it.qs.QuadDirection(val, link.Dir)
		if dval != link.Value {
			return graph.ContainsLogOut(it, val, false)
		}
	}

	node := it.qs.QuadDirection(val, it.dir)
	if it.primaryIt.Contains(node) {
		it.result = val
		return graph.ContainsLogOut(it, val, true)
	}
	it.err = it.primaryIt.Err()
	return graph.ContainsLogOut(it, val, false)
}

func (it *LinksTo) String() string {
	return fmt.Sprintf("MongoLinks(%v)", it.lset)
}

func (it *LinksTo) Reset() {
	it.primaryIt.Reset()
	if it.nextIt != nil {
		it.nextIt.Close()
	}
	it.nextIt = nil
}

// Return a guess as to how big or costly it is to next the iterator.
func (it *LinksTo) Stats() graph.IteratorStats {
	subitStats := it.primaryIt.Stats()
	// TODO(barakmich): These should really come from the quadstore itself
	fanoutFactor := int64(20)
	checkConstant := int64(1)
	nextConstant := int64(2)

	size := fanoutFactor * subitStats.Size
	csize, _ := it.qs.getSize(it.collection, it.buildConstraint())
	if size > csize {
		size = csize
	}

	return graph.IteratorStats{
		NextCost:     nextConstant + subitStats.NextCost,
		ContainsCost: checkConstant + subitStats.ContainsCost,
		Size:         size,
		ExactSize:    false,
		Next:         it.runstats.Next,
		Contains:     it.runstats.Contains,
		ContainsNext: it.runstats.ContainsNext,
	}
}

func (it *LinksTo) Size() (int64, bool) {
	st := it.Stats()
	return st.Size, st.ExactSize
}

// Return a list containing only our subiterator.
func (it *LinksTo) SubIterators() []graph.Iterator {
	return []graph.Iterator{it.primaryIt}
}
