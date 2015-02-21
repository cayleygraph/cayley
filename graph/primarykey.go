// Copyright 2015 The Cayley Authors. All rights reserved.
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

import (
	"encoding/json"
	"strconv"
	"sync"
)

// Defines the PrimaryKey interface, this abstracts the generation of IDs

type primaryKeyType uint8

const (
	none primaryKeyType = iota
	sequential
)

type PrimaryKey struct {
	keyType      primaryKeyType
	mut          sync.Mutex
	sequentialID int64
}

func NewSequentialKey(horizon int64) PrimaryKey {
	return PrimaryKey{
		keyType:      sequential,
		sequentialID: horizon,
	}
}

func (p *PrimaryKey) Next() PrimaryKey {
	switch p.keyType {
	case sequential:
		p.mut.Lock()
		defer p.mut.Unlock()
		p.sequentialID++
		if p.sequentialID <= 0 {
			p.sequentialID = 1
		}
		return PrimaryKey{
			keyType:      sequential,
			sequentialID: p.sequentialID,
		}
	case none:
		panic("Calling next() on a none PrimaryKey")
	}
	return PrimaryKey{}
}

func (p *PrimaryKey) Int() int64 {
	switch p.keyType {
	case sequential:
		return p.sequentialID
	}
	return -1
}

func (p *PrimaryKey) String() string {
	// More options for more keyTypes
	return strconv.FormatInt(p.sequentialID, 10)
}

func (p *PrimaryKey) MarshalJSON() ([]byte, error) {
	return json.Marshal(p.sequentialID)
}

func (p *PrimaryKey) UnmarshalJSON(bytes []byte) error {
	/* Careful special casing here. For example, string-related things might begin
	if bytes[0] == '"' {
	}
	*/
	p.keyType = sequential
	return json.Unmarshal(bytes, &p.sequentialID)
}
