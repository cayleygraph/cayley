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
	"errors"
	"strconv"
	"sync"

	"github.com/cayleygraph/cayley/clog"
	"github.com/pborman/uuid"
)

type primaryKeyType uint8

const (
	none primaryKeyType = iota
	sequential
	unique
)

type PrimaryKey struct {
	keyType      primaryKeyType
	mut          sync.Mutex
	sequentialID int64
	uniqueID     string
}

func NewSequentialKey(horizon int64) PrimaryKey {
	return PrimaryKey{
		keyType:      sequential,
		sequentialID: horizon,
	}
}

func NewUniqueKey(horizon string) PrimaryKey {
	id := uuid.Parse(horizon)
	if id == nil {
		id = uuid.NewUUID()
	}
	return PrimaryKey{
		keyType:  unique,
		uniqueID: id.String(),
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
	case unique:
		id := uuid.NewUUID()
		p.uniqueID = id.String()
		return *p
	case none:
		panic("Calling next() on a none PrimaryKey")
	}
	return PrimaryKey{}
}

func (p *PrimaryKey) Int() int64 {
	switch p.keyType {
	case sequential:
		return p.sequentialID
	case unique:
		msg := "UUID cannot be converted to an int64"
		clog.Errorf(msg)
		panic(msg)
	}
	return -1
}

func (p *PrimaryKey) String() string {
	switch p.keyType {
	case sequential:
		return strconv.FormatInt(p.sequentialID, 10)
	case unique:
		return p.uniqueID
	case none:
		panic("Calling String() on a none PrimaryKey")
	}
	return ""
}

func (p PrimaryKey) MarshalJSON() ([]byte, error) {
	switch p.keyType {
	case none:
		return nil, errors.New("Cannot marshal PrimaryKey with KeyType of 'none'")
	case sequential:
		return json.Marshal(p.sequentialID)
	case unique:
		return []byte("\"u" + p.uniqueID + "\""), nil
	default:
		return nil, errors.New("Unknown PrimaryKey type")
	}
}

func (p *PrimaryKey) UnmarshalJSON(bytes []byte) error {
	if bytes[0] == '"' {
		switch bytes[1] {
		case 'u':
			p.keyType = unique
			p.uniqueID = string(bytes[2 : len(bytes)-1])
			return nil
		default:
			return errors.New("Unknown string-like PrimaryKey type")
		}
	}
	p.keyType = sequential
	return json.Unmarshal(bytes, &p.sequentialID)
}
