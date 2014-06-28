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
	"container/list"
	"fmt"
)

type ResultTree struct {
	result   TSVal
	subtrees *list.List
}

func NewResultTree(result TSVal) *ResultTree {
	var tree ResultTree
	tree.subtrees = list.New()
	tree.result = result
	return &tree
}

func (tree *ResultTree) ToString() string {
	base := fmt.Sprintf("(%d", tree.result)
	if tree.subtrees.Len() != 0 {
		for e := tree.subtrees.Front(); e != nil; e = e.Next() {
			base += fmt.Sprintf(" %s", (e.Value.(*ResultTree)).ToString())
		}
	}
	base += ")"
	return base
}

func (tree *ResultTree) AddSubtree(sub *ResultTree) {
	tree.subtrees.PushBack(sub)
}

func StringResultTreeEvaluator(it Iterator) string {
	ok := true
	out := ""
	for {
		_, ok = it.Next()
		if !ok {
			break
		}
		out += it.GetResultTree().ToString()
		out += "\n"
		for it.NextResult() == true {
			out += " "
			out += it.GetResultTree().ToString()
			out += "\n"
		}
	}
	return out
}

func PrintResultTreeEvaluator(it Iterator) {
	fmt.Print(StringResultTreeEvaluator(it))
}
