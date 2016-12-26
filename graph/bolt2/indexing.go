// Copyright 2016 The Cayley Authors. All rights reserved.
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

package bolt2

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/boltdb/bolt"
	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/quad"
	"github.com/cayleygraph/cayley/quad/pquads"
)

var (
	metaBucket     = []byte("meta")
	valueIndex     = []byte("value")
	subjectIndex   = []byte{quad.Subject.Prefix()}
	objectIndex    = []byte{quad.Object.Prefix()}
	sameAsIndex    = []byte("sameas")
	sameNodesIndex = []byte("samenodes")
	logIndex       = []byte("log")

	// List of all buckets in the current version of the database.
	buckets = [][]byte{
		metaBucket,
		valueIndex,
		subjectIndex,
		objectIndex,
		sameAsIndex,
		sameNodesIndex,
		logIndex,
	}
)

func (qs *QuadStore) createBuckets() error {
	return qs.db.Update(func(tx *bolt.Tx) error {
		var err error
		for _, index := range buckets {
			_, err = tx.CreateBucket(index)
			if err != nil {
				return fmt.Errorf("could not create bucket %s: %s", string(index), err)
			}
		}
		return nil
	})
}

func (qs *QuadStore) writeHorizonAndSize(tx *bolt.Tx) error {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, qs.size)
	if err != nil {
		clog.Errorf("Couldn't convert size!")
		return err
	}
	b := tx.Bucket(metaBucket)
	b.FillPercent = localFillPercent
	werr := b.Put([]byte("size"), buf.Bytes())
	if werr != nil {
		clog.Errorf("Couldn't write size!")
		return werr
	}
	buf.Reset()
	err = binary.Write(buf, binary.LittleEndian, qs.horizon)

	if err != nil {
		clog.Errorf("Couldn't convert horizon!")
	}

	werr = b.Put([]byte("horizon"), buf.Bytes())

	if werr != nil {
		clog.Errorf("Couldn't write horizon!")
		return werr
	}
	return err
}

func (qs *QuadStore) ApplyDeltas(deltas []graph.Delta, ignoreOpts graph.IgnoreOpts) error {
	tx, err := qs.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

nextDelta:
	for _, d := range deltas {
		link := &graph.Primitive{}
		for _, dir := range quad.Directions {
			val := d.Quad.Get(dir)
			if val == nil {
				continue
			}
			v := qs.resolveQuadValue(tx, val)
			if v == 0 {
				// Not found
				if d.Action == graph.Delete {
					if ignoreOpts.IgnoreMissing {
						continue nextDelta
					}
					return fmt.Errorf("Deleting unknown quad: %s", d.Quad)
				}
				node, err := qs.createNodePrimitive(val)
				if err != nil {
					return err
				}
				qs.horizon++
				node.ID = uint64(qs.horizon)
				err = qs.index(tx, node)
				if err != nil {
					return err
				}
				v = node.ID
			}
			link.SetDirection(dir, v)
		}
		qs.horizon++
		link.ID = uint64(qs.horizon)
		t := time.Now()
		link.Timestamp = &t
		if d.Action == graph.Delete {
			// TODO(barakmich):
			// * Lookup existing link
			// * Add link.Replaces = existing
		}
		err = qs.index(tx, link)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (qs *QuadStore) index(tx *bolt.Tx, p *graph.Primitive) error {
	if p.IsNode() {
		return qs.indexNode(tx, p)
	}
	return qs.indexLink(tx, p)
}

func (qs *QuadStore) indexNode(tx *bolt.Tx, p *graph.Primitive) error {
	v, err := pquads.UnmarshalValue(p.Value)
	if err != nil {
		return err
	}
	bucket := tx.Bucket(valueIndex)
	err = bucket.Put(quad.HashOf(v), uint64toBytes(p.ID))
	if err != nil {
		return err
	}
	return qs.addToLog(tx, p)
}

func (qs *QuadStore) indexLink(tx *bolt.Tx, p *graph.Primitive) error {
	var err error
	// Subject
	err = qs.addToMapBucket(tx, subjectIndex, p.Subject, p.ID)
	if err != nil {
		return err
	}
	// Object
	err = qs.addToMapBucket(tx, objectIndex, p.Object, p.ID)
	if err != nil {
		return err
	}
	err = qs.indexSchema(tx, p)
	if err != nil {
		return err
	}
	return qs.addToLog(tx, p)
}

func (qs *QuadStore) addToMapBucket(tx *bolt.Tx, bucket []byte, key, value uint64) error {
	if key == 0 {
		return fmt.Errorf("trying to add to map bucket %s with key 0", bucket)
	}
	b := tx.Bucket(bucket)
	k := uint64toBytes(key)
	bytelist := b.Get(k)
	add := uint64toBytes(value)
	n := make([]byte, len(bytelist)+len(add))
	copy(n[:len(bytelist)], bytelist)
	copy(n[len(bytelist):], add)
	return b.Put(k, n)
}

func (qs *QuadStore) indexSchema(tx *bolt.Tx, p *graph.Primitive) error {
	return nil
}

func (qs *QuadStore) addToLog(tx *bolt.Tx, p *graph.Primitive) error {
	b, err := p.Marshal()
	if err != nil {
		return err
	}
	return tx.Bucket(logIndex).Put(uint64toBytes(p.ID), b)
}

func (qs *QuadStore) createNodePrimitive(v quad.Value) (*graph.Primitive, error) {
	p := &graph.Primitive{}
	b, err := pquads.MarshalValue(v)
	if err != nil {
		return p, err
	}
	p.Value = b
	t := time.Now()
	p.Timestamp = &t
	return p, nil
}

func (qs *QuadStore) resolveQuadValue(tx *bolt.Tx, v quad.Value) uint64 {
	b := quad.HashOf(v)
	val := tx.Bucket(valueIndex).Get(b)
	if val == nil {
		return 0
	}
	out, read := binary.Uvarint(val)
	if read <= 0 {
		clog.Errorf("Error reading value from Bolt")
		return 0
	}
	return out
}

func uint64toBytes(x uint64) []byte {
	b := make([]byte, binary.MaxVarintLen64)
	return uint64toBytesAt(x, b)
}

func uint64toBytesAt(x uint64, bytes []byte) []byte {
	n := binary.PutUvarint(bytes, x)
	return bytes[:n]
}
