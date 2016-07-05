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

import "github.com/cayleygraph/cayley/quad"

// Transaction stores a bunch of Deltas to apply atomatically on the database.
type Transaction struct {
	// Deltas stores the deltas in the right order
	Deltas []Delta
	// deltas stores the deltas in a map to avoid duplications
	deltas map[Delta]struct{}
}

// NewTransaction initialize a new transaction.
func NewTransaction() *Transaction {
	return &Transaction{Deltas: make([]Delta, 0, 10), deltas: make(map[Delta]struct{}, 10)}
}

// AddQuad adds a new quad to the transaction if it is not already present in it.
// If there is a 'remove' delta for that quad, it will remove that delta from
// the transaction instead of actually addind the quad.
func (t *Transaction) AddQuad(q quad.Quad) {
	ad, rd := createDeltas(q)

	if _, adExists := t.deltas[ad]; !adExists {
		if _, rdExists := t.deltas[rd]; rdExists {
			t.deleteDelta(rd)
		} else {
			t.addDelta(ad)
		}
	}
}

// RemoveQuad adds a quad to remove to the transaction.
// The quad will be removed from the database if it is not present in the
// transaction, otherwise it simply remove it from the transaction.
func (t *Transaction) RemoveQuad(q quad.Quad) {
	ad, rd := createDeltas(q)

	if _, adExists := t.deltas[ad]; adExists {
		t.deleteDelta(ad)
	} else {
		if _, rdExists := t.deltas[rd]; !rdExists {
			t.addDelta(rd)
		}
	}
}

func createDeltas(q quad.Quad) (ad, rd Delta) {
	ad = Delta{
		Quad:   q,
		Action: Add,
	}
	rd = Delta{
		Quad:   q,
		Action: Delete,
	}
	return
}

func (t *Transaction) addDelta(d Delta) {
	t.Deltas = append(t.Deltas, d)
	t.deltas[d] = struct{}{}
}

func (t *Transaction) deleteDelta(d Delta) {
	delete(t.deltas, d)

	for i, id := range t.Deltas {
		if id == d {
			t.Deltas = append(t.Deltas[:i], t.Deltas[i+1:]...)
			break
		}
	}
}
