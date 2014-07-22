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

package db

import (
	"fmt"
	"io"
	"os"

	"github.com/google/cayley/config"
	"github.com/google/cayley/graph"
	"github.com/google/cayley/nquads"
)

func Load(ts graph.TripleStore, cfg *config.Config, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("could not open file %q: %v", path, err)
	}
	defer f.Close()

	dec := nquads.NewDecoder(f)

	bulker, canBulk := ts.(graph.BulkLoader)
	if canBulk {
		err = bulker.BulkLoad(dec)
		if err == nil {
			return nil
		}
		if err == graph.ErrCannotBulkLoad {
			err = nil
		}
	}
	if err != nil {
		return err
	}

	block := make([]*graph.Triple, 0, cfg.LoadSize)
	for {
		t, err := dec.Unmarshal()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		block = append(block, t)
		if len(block) == cap(block) {
			ts.AddTripleSet(block)
			block = block[:0]
		}
	}
	ts.AddTripleSet(block)

	return nil
}
