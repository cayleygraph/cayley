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
	"github.com/google/cayley/quad"
)

type Transaction struct {
	Deltas []Delta
}

func NewTransaction() *Transaction {
	return &Transaction{make([]Delta, 0, 5)}
}

func (t *Transaction) AddQuad(q quad.Quad) {
	t.Deltas = append(t.Deltas,
		Delta{
			Quad:   q,
			Action: Add,
		})
}

func (t *Transaction) RemoveQuad(q quad.Quad) {
	t.Deltas = append(t.Deltas,
		Delta{
			Quad:   q,
			Action: Delete,
		})
}
