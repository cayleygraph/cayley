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
	"github.com/google/cayley/quad"
)

const DefaultKeyspace = "cayley"

type QuadStore struct {
	sess    *gocql.Session
	size    int64
	horizon int64
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

func NewQuadStore(addr string, options graph.Options) graph.TripleStore {
	cluster := clusterWithOptions(addr, options)
	session, err := cluster.CreateSession()
	if err != nil {
		glog.Fatalln("Could not connect to Cassandra graph:", err)
	}

	qs := &QuadStore{}
	qs.sess = session
	session.Query("SELECT COUNT(*) FROM quads_by_s").Scan(&qs.size)
	return qs
}

func (qs *QuadStore) Close() {
	qs.sess.Close()
}

var tables = []string{"quads_by_s", "quads_by_p", "quads_by_o", "quads_by_c"}

func (qs *QuadStore) addDeltaToBatch(d graph.Delta, data *gocql.Batch, count *gocql.Batch) {
	data.Cons = gocql.Quorum
	q := &d.Quad
	for _, table := range tables {
		if q.Label == "" && table == "quads_by_c" {
			continue
		}
		query := fmt.Sprint("INSERT INTO ", table, " (subject, predicate, object, label, created) VALUES (?, ?, ?, ?)")
		data.Query(query, q.Subject, q.Predicate, q.Object, q.Label, d.ID)
	}
	count.Cons = gocql.Quorum
	for _, dir := range []quad.Direction{quad.Subject, quad.Predicate, quad.Object, quad.Label} {
		if q.Get(dir) == "" {
			continue
		}
		query := fmt.Sprint("UPDATE nodes SET ", dir, "_count = ", dir, "_count + 1 WHERE node = ?")
		count.Query(query, q.Get(dir))
	}
}

func (qs *QuadStore) ApplyDeltas(deltas []graph.Delta) error {
	batch := qs.sess.NewBatch(gocql.LoggedBatch)
	counter_batch := qs.sess.NewBatch(gocql.CounterBatch)
	new_size := qs.size
	new_horizon := qs.horizon
	for _, d := range deltas {
		if d.Action == graph.Add {
			qs.addTripleToBatch(t, batch, counter_batch)
		}

	}

}

func (qs *QuadStore) AddTriple(t *quad.Quad) {
	err := qs.sess.ExecuteBatch(batch)
	if err != nil {
		glog.Errorln("Couldn't write triple:", t, ", ", err)
	}
	err = qs.sess.ExecuteBatch(counter_batch)
	if err != nil {
		glog.Errorln("Couldn't write triple:", t, ", ", err)
	}
	qs.size += 1
}

func (qs *QuadStore) RemoveTriple(t *quad.Quad) {
}

func (qs *QuadStore) AddTripleSet(set []*quad.Quad) {
	batch := qs.sess.NewBatch(gocql.LoggedBatch)
	counter_batch := qs.sess.NewBatch(gocql.CounterBatch)
	for _, t := range set {
		qs.addTripleToBatch(t, batch, counter_batch)
	}
	err := qs.sess.ExecuteBatch(batch)
	if err != nil {
		glog.Errorln("Couldn't write tripleset:", err)
	}
	err = qs.sess.ExecuteBatch(counter_batch)
	if err != nil {
		glog.Errorln("Couldn't write tripleset:", err)
	}
	qs.size += int64(len(set))
}

func (qs *QuadStore) TripleIterator(d quad.Direction, val graph.Value) graph.Iterator {
	return NewIterator(qs, d, val)
}

func (qs *QuadStore) NodesAllIterator() graph.Iterator {
	return NewNodeIterator(qs)
}

func (qs *QuadStore) TriplesAllIterator() graph.Iterator {
	return NewIterator(qs, quad.Any, "")
}

func compareStrings(a, b graph.Value) bool {
	return a.(string) == b.(string)
}

func (qs *QuadStore) FixedIterator() graph.FixedIterator {
	return iterator.NewFixedIteratorWithCompare(compareStrings)
}

func (qs *QuadStore) Quad(val graph.Value) quad.Quad {
	return val.(quad.Quad)
}

func (qs *QuadStore) ValueOf(node string) graph.Value {
	return node
}

func (qs *QuadStore) NameOf(val graph.Value) string {
	return val.(string)
}

func (qs *QuadStore) TripleDirection(triple_id graph.Value, d quad.Direction) graph.Value {
	return qs.ValueOf(qs.Quad(triple_id).Get(d))
}

func (qs *QuadStore) Size() int64 {
	return qs.size
}

func (qs *QuadStore) OptimizeIterator(it graph.Iterator) (graph.Iterator, bool) {
	switch it.Type() {
	case graph.LinksTo:
		return qs.optimizeLinksTo(it.(*iterator.LinksTo))
	}
	return it, false
}

func (qs *QuadStore) optimizeLinksTo(it *iterator.LinksTo) (graph.Iterator, bool) {
	subs := it.SubIterators()
	if len(subs) != 1 {
		return it, false
	}
	primary := subs[0]
	if primary.Type() == graph.Fixed {
		size, _ := primary.Size()
		if size == 1 {
			if !graph.Next(primary) {
				panic("unexpected size during optimize")
			}
			val := primary.Result()
			newIt := qs.TripleIterator(it.Direction(), val)
			nt := newIt.Tagger()
			nt.CopyFrom(it)
			for _, tag := range primary.Tagger().Tags() {
				nt.AddFixed(tag, val)
			}
			it.Close()
			return newIt, true
		}
	}
	return it, false
}
