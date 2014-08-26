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
	"github.com/barakmich/glog"
	"github.com/gocql/gocql"

	"github.com/google/cayley/graph"
)

func createNewCassandraGraph(addr string, options graph.Options) error {
	cluster := clusterWithOptions(addr, options)
	cluster.Consistency = gocql.All
	session, err := cluster.CreateSession()
	if err != nil {
		glog.Errorln("Could not create a Cassandra graph:", err)
		return err
	}
	err = session.Query(`
	CREATE TABLE log (
		id bigint PRIMARY KEY,
		size bigint,
		timestamp timestamp,
		action text,
		subject text,
		predicate text,
		object text,
		label text,
	)
	`).Exec()
	if err != nil {
		glog.Errorln("Could not create table log:", err)
		return err
	}
	err = session.Query(`
	CREATE TABLE quads_by_s (
		subject text,
		predicate text,
		object text,
		label text,
		created bigint,
		deleted bigint,
		PRIMARY KEY (subject, predicate, object, label)
	)
	`).Exec()
	if err != nil {
		glog.Errorln("Could not create table quads_by_s:", err)
		return err
	}
	err = session.Query(`
	CREATE TABLE quads_by_p (
		subject text,
		predicate text,
		object text,
		label text,
		created bigint,
		deleted bigint,
		PRIMARY KEY (predicate, object, subject, label)
	)
	`).Exec()
	if err != nil {
		glog.Errorln("Could not create table quads_by_p:", err)
		return err
	}
	err = session.Query(`
	CREATE TABLE quads_by_o (
		subject text,
		predicate text,
		object text,
		label text,
		created bigint,
		deleted bigint,
		PRIMARY KEY (object, subject, predicate, label)
	)
	`).Exec()
	if err != nil {
		glog.Errorln("Could not create table quads_by_o:", err)
		return err
	}
	err = session.Query(`
	CREATE TABLE quads_by_c (
		subject text,
		predicate text,
		object text,
		label text,
		created bigint,
		deleted bigint,
		PRIMARY KEY (label, subject, predicate, object)
	)
	`).Exec()
	if err != nil {
		glog.Errorln("Could not create table triples_by_c:", err)
		return err
	}
	err = session.Query(`
	CREATE TABLE nodes (
		node text,
		subject_count counter,
		predicate_count counter,
		object_count counter,
		label_count counter,
		PRIMARY KEY (node)
	)
	`).Exec()
	if err != nil {
		glog.Errorln("Could not create table nodes:", err)
		return err
	}
	err = session.Query(`
	CREATE TABLE metadata (
		meta_key text PRIMARY KEY,
		value bigint,
	)
	`).Exec()
	if err != nil {
		glog.Errorln("Could not create table metadata:", err)
		return err
	}
	session.Close()
	return nil
}
