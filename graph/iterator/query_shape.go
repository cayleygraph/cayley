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

package iterator

import (
	"github.com/google/cayley/graph"
)

type Node struct {
	Id         int      `json:"id"`
	Tags       []string `json:"tags,omitempty"`
	Values     []string `json:"values,omitempty"`
	IsLinkNode bool     `json:"is_link_node"`
	IsFixed    bool     `json:"is_fixed"`
}

type Link struct {
	Source   int `json:"source"`
	Target   int `json:"target"`
	Pred     int `json:"type"`
	LinkNode int `json:"link_node"`
}

type queryShape struct {
	nodes    []Node
	links    []Link
	ts       graph.TripleStore
	nodeId   int
	hasaIds  []int
	hasaDirs []graph.Direction
}

func OutputQueryShapeForIterator(it graph.Iterator, ts graph.TripleStore, outputMap *map[string]interface{}) {
	qs := &queryShape{
		ts:     ts,
		nodeId: 1,
	}

	node := qs.MakeNode(it.Clone())
	qs.AddNode(node)
	(*outputMap)["nodes"] = qs.nodes
	(*outputMap)["links"] = qs.links
}

func (qs *queryShape) AddNode(n *Node) {
	qs.nodes = append(qs.nodes, *n)
}

func (qs *queryShape) AddLink(l *Link) {
	qs.links = append(qs.links, *l)
}

func (qs *queryShape) LastHasa() (int, graph.Direction) {
	return qs.hasaIds[len(qs.hasaIds)-1], qs.hasaDirs[len(qs.hasaDirs)-1]
}

func (qs *queryShape) PushHasa(i int, d graph.Direction) {
	qs.hasaIds = append(qs.hasaIds, i)
	qs.hasaDirs = append(qs.hasaDirs, d)
}

func (qs *queryShape) RemoveHasa() {
	qs.hasaIds = qs.hasaIds[:len(qs.hasaIds)-1]
	qs.hasaDirs = qs.hasaDirs[:len(qs.hasaDirs)-1]
}

func (qs *queryShape) StealNode(left *Node, right *Node) {
	for _, v := range right.Values {
		left.Values = append(left.Values, v)
	}
	for _, v := range right.Tags {
		left.Tags = append(left.Tags, v)
	}
	left.IsLinkNode = left.IsLinkNode || right.IsLinkNode
	left.IsFixed = left.IsFixed || right.IsFixed
	for i, link := range qs.links {
		rewrite := false
		if link.LinkNode == right.Id {
			link.LinkNode = left.Id
			rewrite = true
		}
		if link.Source == right.Id {
			link.Source = left.Id
			rewrite = true
		}
		if link.Target == right.Id {
			link.Target = left.Id
			rewrite = true
		}
		if rewrite {
			qs.links = append(append(qs.links[:i], qs.links[i+1:]...), link)
		}
	}
}

func (qs *queryShape) MakeNode(it graph.Iterator) *Node {
	n := Node{Id: qs.nodeId}
	for _, tag := range it.Tags() {
		n.Tags = append(n.Tags, tag)
	}
	for k, _ := range it.FixedTags() {
		n.Tags = append(n.Tags, k)
	}

	switch it.Type() {
	case graph.And:
		for _, sub := range it.SubIterators() {
			qs.nodeId++
			newNode := qs.MakeNode(sub)
			if sub.Type() != graph.Or {
				qs.StealNode(&n, newNode)
			} else {
				qs.AddNode(newNode)
				qs.AddLink(&Link{n.Id, newNode.Id, 0, 0})
			}
		}
	case graph.Fixed:
		n.IsFixed = true
		for {
			val, more := it.Next()
			if !more {
				break
			}
			n.Values = append(n.Values, qs.ts.NameOf(val))
		}
	case graph.HasA:
		hasa := it.(*HasA)
		qs.PushHasa(n.Id, hasa.dir)
		qs.nodeId++
		newNode := qs.MakeNode(hasa.primaryIt)
		qs.AddNode(newNode)
		qs.RemoveHasa()
	case graph.Or:
		for _, sub := range it.SubIterators() {
			qs.nodeId++
			newNode := qs.MakeNode(sub)
			if sub.Type() == graph.Or {
				qs.StealNode(&n, newNode)
			} else {
				qs.AddNode(newNode)
				qs.AddLink(&Link{n.Id, newNode.Id, 0, 0})
			}
		}
	case graph.LinksTo:
		n.IsLinkNode = true
		lto := it.(*LinksTo)
		qs.nodeId++
		newNode := qs.MakeNode(lto.primaryIt)
		hasaID, hasaDir := qs.LastHasa()
		if (hasaDir == graph.Subject && lto.dir == graph.Object) ||
			(hasaDir == graph.Object && lto.dir == graph.Subject) {
			qs.AddNode(newNode)
			if hasaDir == graph.Subject {
				qs.AddLink(&Link{hasaID, newNode.Id, 0, n.Id})
			} else {
				qs.AddLink(&Link{newNode.Id, hasaID, 0, n.Id})
			}
		} else if lto.primaryIt.Type() == graph.Fixed {
			qs.StealNode(&n, newNode)
		} else {
			qs.AddNode(newNode)
		}
	case graph.Optional:
		// Unsupported, for the moment
		fallthrough
	case graph.All:
	}
	return &n
}
