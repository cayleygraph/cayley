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

import "fmt"

type ResultTree struct {
	result   Value
	subtrees []*ResultTree
}

func NewResultTree(result Value) *ResultTree {
	return &ResultTree{result: result}
}

func (t *ResultTree) String() string {
	base := fmt.Sprintf("(%d", t.result)
	if len(t.subtrees) != 0 {
		for _, sub := range t.subtrees {
			base += fmt.Sprintf(" %s", sub)
		}
	}
	base += ")"
	return base
}

func (t *ResultTree) AddSubtree(sub *ResultTree) {
	t.subtrees = append(t.subtrees, sub)
}

func StringResultTreeEvaluator(it Nexter) string {
	ok := true
	out := ""
	for {
		_, ok = it.Next()
		if !ok {
			break
		}
		out += it.ResultTree().String()
		out += "\n"
		for it.NextResult() == true {
			out += " "
			out += it.ResultTree().String()
			out += "\n"
		}
	}
	return out
}

func PrintResultTreeEvaluator(it Nexter) {
	fmt.Print(StringResultTreeEvaluator(it))
}
