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

package graph

import (
	"bytes"
	"fmt"
)

type ResultTree struct {
	result   Value
	subtrees []*ResultTree
}

func NewResultTree(result Value) *ResultTree {
	return &ResultTree{result: result}
}

func (t *ResultTree) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "(%d", t.result)
	for _, sub := range t.subtrees {
		fmt.Fprintf(&buf, " %s", sub)
	}
	buf.WriteByte(')')
	return buf.String()
}

func (t *ResultTree) AddSubtree(sub *ResultTree) {
	t.subtrees = append(t.subtrees, sub)
}

func StringResultTreeEvaluator(it Nexter) string {
	var buf bytes.Buffer
	for it.Next() {
		fmt.Fprintln(&buf, it.ResultTree())
		for it.NextPath() {
			buf.WriteByte(' ')
			fmt.Fprintln(&buf, it.ResultTree())
		}
	}
	return buf.String()
}

func PrintResultTreeEvaluator(it Nexter) {
	fmt.Print(StringResultTreeEvaluator(it))
}
