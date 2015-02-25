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
	"code.google.com/p/go-uuid/uuid"
	"encoding/json"
	"errors"
	"github.com/barakmich/glog"
	"strconv"
	"sync"
)

type primaryKeyType uint8

const (
	none primaryKeyType = iota
	sequential
	unique
)

type PrimaryKey struct {
	KeyType      primaryKeyType
	mut          sync.Mutex
	SequentialID int64
	UniqueID     string
}

func NewSequentialKey(horizon int64) PrimaryKey {
	return PrimaryKey{
		KeyType:      sequential,
		SequentialID: horizon,
	}
}

func NewUniqueKey(horizon string) PrimaryKey {
	id := uuid.Parse(horizon)
	if id == nil {
		id = uuid.NewUUID()
	}
	return PrimaryKey{
		KeyType:  unique,
		UniqueID: id.String(),
	}
}

func (p *PrimaryKey) Next() PrimaryKey {
	switch p.KeyType {
	case sequential:
		p.mut.Lock()
		defer p.mut.Unlock()
		p.SequentialID++
		if p.SequentialID <= 0 {
			p.SequentialID = 1
		}
		return PrimaryKey{
			KeyType:      sequential,
			SequentialID: p.SequentialID,
		}
	case unique:
		id := uuid.NewUUID()
		p.UniqueID = id.String()
		return *p
	case none:
		panic("Calling next() on a none PrimaryKey")
	}
	return PrimaryKey{}
}

func (p *PrimaryKey) Int() int64 {
	switch p.KeyType {
	case sequential:
		return p.SequentialID
	case unique:
		glog.Fatal("UUID cannot be cast to an int64")
		return -1
	}
	return -1
}

func (p *PrimaryKey) String() string {
	switch p.KeyType {
	case sequential:
		return strconv.FormatInt(p.SequentialID, 10)
	case unique:
		return p.UniqueID
	case none:
		panic("Calling String() on a none PrimaryKey")
	}
	return ""
}

func (p *PrimaryKey) MarshalJSON() ([]byte, error) {
	if p.KeyType == none {
		return nil, errors.New("Cannot marshal PrimaryKey with KeyType of 'none'")
	}
	return json.Marshal(*p)
}

//To avoid recursion in the implmentation of the UnmarshalJSON interface below
type primaryKey PrimaryKey

func (p *PrimaryKey) UnmarshalJSON(bytes []byte) error {
	/* Careful special casing here. For example, string-related things might begin
	if bytes[0] == '"' {
	}
	*/
	temp := primaryKey{}
	if err := json.Unmarshal(bytes, &temp); err != nil {
		return err
	}
	*p = (PrimaryKey)(temp)
	if p.KeyType == none {
		return errors.New("Could not properly unmarshal primary key, 'none' keytype detected")
	}
	return nil
}
