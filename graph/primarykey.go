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
	"bytes"
	"encoding/json"
	"io"

	"github.com/pborman/uuid"
)

var _ Value = PrimaryKey{}

type PrimaryKey struct {
	sequentialID int64
	uniqueID     string
}

func (p PrimaryKey) Key() interface{} {
	return p
}

func NewSequentialKey(val int64) PrimaryKey {
	return PrimaryKey{
		sequentialID: val,
	}
}

func NewUniqueKey(s string) PrimaryKey {
	if s == "" {
		s = uuid.NewUUID().String()
	}
	return PrimaryKey{
		uniqueID: s,
	}
}

func (p PrimaryKey) Valid() bool {
	return p.sequentialID != 0 || p.uniqueID != ""
}

func (p PrimaryKey) Int() (int64, bool) {
	return p.sequentialID, p.uniqueID == ""
}

func (p *PrimaryKey) Unique() (string, bool) {
	return p.uniqueID, p.uniqueID != ""
}

func (p PrimaryKey) MarshalJSON() ([]byte, error) {
	if p.sequentialID != 0 {
		return json.Marshal(p.sequentialID)
	} else if p.uniqueID != "" {
		return json.Marshal(p.uniqueID)
	}
	return []byte("null"), nil
}

func (p *PrimaryKey) UnmarshalJSON(b []byte) error {
	b = bytes.TrimSpace(b)
	if len(b) == 0 {
		return io.ErrUnexpectedEOF
	} else if b[0] == '"' {
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
		*p = PrimaryKey{uniqueID: s}
		return nil
	}
	var n int64
	if err := json.Unmarshal(b, &n); err != nil {
		return err
	}
	*p = PrimaryKey{sequentialID: n}
	return nil
}
