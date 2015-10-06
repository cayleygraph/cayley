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

import "github.com/google/cayley/quad"

// Transaction stores a bunch of Deltas to apply atomatically on the database.
type Transaction struct {
	Deltas map[Delta]struct{}
}

// NewTransaction initialize a new transaction.
func NewTransaction() *Transaction {
	return &Transaction{Deltas: make(map[Delta]struct{}, 100)}
}

// AddQuad adds a new quad to the transaction if it is not already present in it.
// If there is a 'remove' delta for that quad, it will remove that delta from
// the transaction instead of actually addind the quad.
func (t *Transaction) AddQuad(q quad.Quad) {
	ad := Delta{
		Quad:   q,
		Action: Add,
	}
	rd := Delta{
		Quad:   q,
		Action: Delete,
	}

	if _, adExists := t.Deltas[ad]; !adExists {
		if _, rdExists := t.Deltas[rd]; rdExists {
			delete(t.Deltas, rd)
		} else {
			t.Deltas[ad] = struct{}{}
		}
	}
}

// RemoveQuad adds a quad to remove to the transaction.
// The quad will be removed from the database if it is not present in the
// transaction, otherwise it simply remove it from the transaction.
func (t *Transaction) RemoveQuad(q quad.Quad) {
	ad := Delta{
		Quad:   q,
		Action: Add,
	}

	if _, adExists := t.Deltas[ad]; adExists {
		delete(t.Deltas, ad)
	} else {
		t.Deltas[Delta{Quad: q, Action: Delete}] = struct{}{}
	}
}
