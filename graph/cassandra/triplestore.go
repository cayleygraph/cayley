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

package cassandra

import (
	"fmt"
	"strings"

	"github.com/barakmich/glog"
	"github.com/gocql/gocql"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
)

const DefaultKeyspace = "cayley"

type TripleStore struct {
	sess *gocql.Session
	size int64
}

func getAllAddresses(addr string, options graph.Options) []string {
	var out []string
	out = append(out, strings.Split(addr, ",")...)
	if val, ok := options.StringKey("address_list"); ok {
		out = append(out, strings.Split(val, ",")...)
	}
	return out
}

func clusterWithOptions(addr string, options graph.Options) *gocql.ClusterConfig {
	allAddrs := getAllAddresses(addr, options)
	cluster := gocql.NewCluster(allAddrs...)
	keyspace := DefaultKeyspace
	if val, ok := options.StringKey("keyspace"); ok {
		keyspace = val
	}
	cluster.Keyspace = keyspace
	cluster.Consistency = gocql.One
	return cluster
}

func CreateNewCassandraGraph(addr string, options graph.Options) bool {
	cluster := clusterWithOptions(addr, options)
	cluster.Consistency = gocql.All
	session, err := cluster.CreateSession()
	if err != nil {
		glog.Fatalln("Could not create a Cassandra graph:", err)
		return false
	}
	err = session.Query(`
	CREATE TABLE triples_by_s (
		subject text,
		predicate text,
		object text,
		provenance text,
		PRIMARY KEY (subject, predicate, object, provenance)
	)
	`).Exec()
	if err != nil {
		glog.Fatalln("Could not create table triples_by_s:", err)
	}
	err = session.Query(`
	CREATE TABLE triples_by_p (
		subject text,
		predicate text,
		object text,
		provenance text,
		PRIMARY KEY (predicate, object, subject, provenance)
	)
	`).Exec()
	if err != nil {
		glog.Fatalln("Could not create table triples_by_p:", err)
	}
	err = session.Query(`
	CREATE TABLE triples_by_o (
		subject text,
		predicate text,
		object text,
		provenance text,
		PRIMARY KEY (object, subject, predicate, provenance)
	)
	`).Exec()
	if err != nil {
		glog.Fatalln("Could not create table triples_by_o:", err)
	}
	err = session.Query(`
	CREATE TABLE triples_by_c (
		subject text,
		predicate text,
		object text,
		provenance text,
		PRIMARY KEY (provenance, subject, predicate, object)
	)
	`).Exec()
	if err != nil {
		glog.Fatalln("Could not create table triples_by_c:", err)
	}
	err = session.Query(`
	CREATE TABLE nodes (
		node text,
		subject_count counter,
		predicate_count counter,
		object_count counter,
		provenance_count counter,
		PRIMARY KEY (node)
	)
	`).Exec()
	if err != nil {
		glog.Fatalln("Could not create table nodes:", err)
	}
	return true
}

func NewTripleStore(addr string, options graph.Options) graph.TripleStore {
	cluster := clusterWithOptions(addr, options)
	session, err := cluster.CreateSession()
	if err != nil {
		glog.Fatalln("Could not connect to Cassandra graph:", err)
	}

	ts := &TripleStore{}
	ts.sess = session
	session.Query("SELECT COUNT(*) FROM triples_by_s").Scan(&ts.size)
	return ts
}

func (ts *TripleStore) Close() {
	ts.sess.Close()
}

var tables = []string{"triples_by_s", "triples_by_p", "triples_by_o", "triples_by_c"}

func (ts *TripleStore) addTripleToBatch(t *graph.Triple, data *gocql.Batch, count *gocql.Batch) {
	data.Cons = gocql.Quorum
	for _, table := range tables {
		if t.Provenance == "" && table == "triples_by_c" {
			continue
		}
		query := fmt.Sprint("INSERT INTO ", table, " (subject, predicate, object, provenance) VALUES (?, ?, ?, ?)")
		data.Query(query, t.Subject, t.Predicate, t.Object, t.Provenance)
	}
	count.Cons = gocql.Quorum
	for _, dir := range []graph.Direction{graph.Subject, graph.Predicate, graph.Object, graph.Provenance} {
		if t.Get(dir) == "" {
			continue
		}
		query := fmt.Sprint("UPDATE nodes SET ", dir, "_count = ", dir, "_count + 1 WHERE node = ?")
		count.Query(query, t.Get(dir))
	}
}

func (ts *TripleStore) AddTriple(t *graph.Triple) {
	batch := ts.sess.NewBatch(gocql.LoggedBatch)
	counter_batch := ts.sess.NewBatch(gocql.CounterBatch)
	ts.addTripleToBatch(t, batch, counter_batch)
	err := ts.sess.ExecuteBatch(batch)
	if err != nil {
		glog.Errorln("Couldn't write triple:", t, ", ", err)
	}
	err = ts.sess.ExecuteBatch(counter_batch)
	if err != nil {
		glog.Errorln("Couldn't write triple:", t, ", ", err)
	}
	ts.size += 1
}

func (ts *TripleStore) RemoveTriple(t *graph.Triple) {
}

func (ts *TripleStore) AddTripleSet(set []*graph.Triple) {
	batch := ts.sess.NewBatch(gocql.LoggedBatch)
	counter_batch := ts.sess.NewBatch(gocql.CounterBatch)
	for _, t := range set {
		ts.addTripleToBatch(t, batch, counter_batch)
	}
	err := ts.sess.ExecuteBatch(batch)
	if err != nil {
		glog.Errorln("Couldn't write tripleset:", err)
	}
	err = ts.sess.ExecuteBatch(counter_batch)
	if err != nil {
		glog.Errorln("Couldn't write tripleset:", err)
	}
	ts.size += int64(len(set))
}

func (ts *TripleStore) TripleIterator(d graph.Direction, val graph.Value) graph.Iterator {
	return NewIterator(ts, d, val)
}

func (ts *TripleStore) NodesAllIterator() graph.Iterator {
	return NewNodeIterator(ts)
}

func (ts *TripleStore) TriplesAllIterator() graph.Iterator {
	return NewIterator(ts, graph.Any, "")
}

func compareStrings(a, b graph.Value) bool {
	return a.(string) == b.(string)
}

func (ts *TripleStore) FixedIterator() graph.FixedIterator {
	return iterator.NewFixedIteratorWithCompare(compareStrings)
}

func (ts *TripleStore) Triple(val graph.Value) *graph.Triple {
	return val.(*graph.Triple)
}

func (ts *TripleStore) ValueOf(node string) graph.Value {
	return node
}

func (ts *TripleStore) NameOf(val graph.Value) string {
	return val.(string)
}

func (ts *TripleStore) TripleDirection(triple_id graph.Value, d graph.Direction) graph.Value {
	return ts.ValueOf(ts.Triple(triple_id).Get(d))
}

func (ts *TripleStore) Size() int64 {
	return ts.size
}

func (ts *TripleStore) OptimizeIterator(it graph.Iterator) (graph.Iterator, bool) {
	switch it.Type() {
	case graph.LinksTo:
		return ts.optimizeLinksTo(it.(*iterator.LinksTo))
	}
	return it, false
}

func (ts *TripleStore) optimizeLinksTo(it *iterator.LinksTo) (graph.Iterator, bool) {
	subs := it.SubIterators()
	if len(subs) != 1 {
		return it, false
	}
	primary := subs[0]
	if primary.Type() == graph.Fixed {
		size, _ := primary.Size()
		if size == 1 {
			val, ok := primary.Next()
			if !ok {
				panic("Sizes lie")
			}
			newIt := ts.TripleIterator(it.Direction(), val)
			newIt.CopyTagsFrom(it)
			for _, tag := range primary.Tags() {
				newIt.AddFixedTag(tag, val)
			}
			it.Close()
			return newIt, true
		}
	}
	return it, false
}
