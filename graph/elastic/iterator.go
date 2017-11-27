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

package elastic

import (
	"context"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/quad"
	elastic "gopkg.in/olivere/elastic.v5"
)

// Iterator struct used for elastic backend
type Iterator struct {
	uid         uint64
	tags        graph.Tagger
	qs          *QuadStore
	dir         quad.Direction
	resultSet   *elastic.SearchResult
	resultIndex int64
	scrollId    string
	hash        NodeHash
	size        int64
	isAll       bool
	resultType  string
	types       string
	query       elastic.Query
	result      graph.Value
	err         error
}

// SearchResultsIterator contains the results of the search
type SearchResultsIterator struct {
	index         uint64
	searchResults *elastic.SearchResult
}

// NewIterator returns a new iterator
func NewIterator(qs *QuadStore, resultType string, d quad.Direction, val graph.Value) *Iterator {
	h := val.(NodeHash)
	query := elastic.NewTermQuery(d.String(), string(h))
	return &Iterator{
		uid:         iterator.NextUID(),
		qs:          qs,
		dir:         d,
		resultSet:   nil,
		resultType:  resultType,
		resultIndex: 0,
		query:       query,
		size:        -1,
		hash:        h,
		isAll:       false,
	}
}

// if iterator is empty make elastic query and return results set
func (it *Iterator) makeElasticResultSet(ctx context.Context) (*elastic.SearchResult, error) {
	if it.isAll {
		resultSet, error := it.qs.client.Scroll("cayley").Type(it.resultType).Do(ctx)
		it.resultSet = resultSet
		return resultSet, error
	}
	resultSet, error := it.qs.client.Scroll("cayley").Type(it.resultType).Query(it.query).Do(ctx)
	it.resultSet = resultSet
	return resultSet, error
}

// NewAllIterator returns an iterator over all nodes
func NewAllIterator(qs *QuadStore, resultType string) *Iterator {
	query := elastic.NewTypeQuery(resultType)
	return &Iterator{
		uid:         iterator.NextUID(),
		qs:          qs,
		dir:         quad.Any,
		resultSet:   nil,
		resultType:  resultType,
		resultIndex: 0,
		size:        -1,
		query:       query,
		hash:        "",
		isAll:       true,
	}
}

// Tagger returns the iterator tags
func (it *Iterator) Tagger() *graph.Tagger {
	return &it.tags
}

// TagResults tags the iterator results
func (it *Iterator) TagResults(dst map[string]graph.Value) {
	for _, tag := range it.tags.Tags() {
		dst[tag] = it.Result()
	}

	for tag, value := range it.tags.Fixed() {
		dst[tag] = value
	}
}

// Result returns the iterator results
func (it *Iterator) Result() graph.Value {
	return it.result
}

// Next returns true and increments resultIndex if there is another result in the elastic results page, else returns false.
func (it *Iterator) Next() bool {
	ctx := context.Background()
	if it.resultSet == nil {
		results, err := it.makeElasticResultSet(ctx)
		if err != nil {
			return false
		}
		it.resultSet = results
	}

	var resultID string
	if it.resultIndex < int64(len(it.resultSet.Hits.Hits)) {
		resultID = it.resultSet.Hits.Hits[it.resultIndex].Id
		it.resultIndex++
	} else {
		newResults, err := it.qs.client.Scroll("cayley").ScrollId(it.resultSet.ScrollId).Do(ctx)
		if err != nil || newResults.Hits.TotalHits == 0 {
			return false
		}
		it.resultSet = newResults
		resultID = it.resultSet.Hits.Hits[0].Id
		it.resultIndex = 1
	}

	if it.resultType == "quads" {
		it.result = QuadHash(resultID)
	} else {
		it.result = NodeHash(resultID)
	}

	return true
}

// NextPath gives another path in the tree that gives us the desired result
func (it *Iterator) NextPath() bool {
	return false
}

// Contains checks if the graph contains a given value
func (it *Iterator) Contains(v graph.Value) bool {
	graph.ContainsLogIn(it, v)
	if it.isAll {
		it.result = v
		return graph.ContainsLogOut(it, v, true)
	}
	val := NodeHash(v.(QuadHash).Get(it.dir))
	if val == it.hash {
		it.result = v
		return graph.ContainsLogOut(it, v, true)
	}
	return graph.ContainsLogOut(it, v, false)
}

// Err returns an error
func (it *Iterator) Err() error {
	return it.err
}

// Reset makes a result set
func (it *Iterator) Reset() {
	it.resultSet = nil
	it.resultIndex = 0
}

// Clone copies the iterator that is passed in
func (it *Iterator) Clone() graph.Iterator {
	var m *Iterator
	if it.isAll {
		m = NewAllIterator(it.qs, it.resultType)
	} else {
		m = NewIterator(it.qs, it.resultType, it.dir, NodeHash(it.hash))
	}
	m.tags.CopyFrom(it)
	return m
}

// Stats returns the stats of the Iterator
func (it *Iterator) Stats() graph.IteratorStats {
	size, exact := it.Size()
	return graph.IteratorStats{
		ContainsCost: 1,
		NextCost:     5,
		Size:         size,
		ExactSize:    exact,
	}
}

// Size gives the number of results returned
func (it *Iterator) Size() (int64, bool) {
	if it.size == -1 {
		var err error
		it.size, err = it.qs.getSize(it.resultType, it.query)
		if err != nil {
			it.err = err
		}
	}
	return it.size, true
}

// Type returns the kind of iterator (All, And, etc.)
func (it *Iterator) Type() graph.Type {
	if it.isAll {
		return graph.All
	}
	return elasticType
}

// Optimize makes the iterator more efficient
func (it *Iterator) Optimize() (graph.Iterator, bool) { return it, false }

// SubIterators returns the subiterators
func (it *Iterator) SubIterators() []graph.Iterator {
	return nil
}

// Describe gives the graph description
func (it *Iterator) Describe() graph.Description {
	size, _ := it.Size()
	return graph.Description{
		UID:  it.UID(),
		Name: string(it.hash),
		Type: it.Type(),
		Size: size,
	}
}

// Close closes the iterator
func (it *Iterator) Close() error {
	return nil
}

// UID returns the iterator ID
func (it *Iterator) UID() uint64 {
	return it.uid
}

var elasticType graph.Type

// Type returns the type of graph (elastic in this case)
func Type() graph.Type { return elasticType }

// Sorted sorts the iterator results
func (it *Iterator) Sorted() bool { return true }

var _ graph.Iterator = &Iterator{}
