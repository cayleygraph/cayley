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

	"github.com/boltdb/bolt"
	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/quad"
)

var (
	metaBucket     = []byte("meta")
	valueIndex     = []byte("value")
	subjectIndex   = []byte{quad.Subject.Prefix()}
	objectIndex    = []byte{quad.Object.Prefix()}
	sameAsIndex    = []byte("sameas")
	sameNodesIndex = []byte("samenodes")

	// List of all buckets in the current version of the database.
	buckets = [][]byte{
		metaBucket,
		valueIndex,
		subjectIndex,
		objectIndex,
		sameAsIndex,
		sameNodesIndex,
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
	panic("todo")
}
