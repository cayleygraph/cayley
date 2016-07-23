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
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/quad"
)

type Node struct {
	ID         int      `json:"id"`
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
	qs       graph.QuadStore
	nodeID   int
	hasaIDs  []int
	hasaDirs []quad.Direction
}

func OutputQueryShapeForIterator(it graph.Iterator, qs graph.QuadStore, outputMap map[string]interface{}) {
	s := &queryShape{
		qs:     qs,
		nodeID: 1,
	}

	node := s.MakeNode(it.Clone())
	s.AddNode(node)
	outputMap["nodes"] = s.nodes
	outputMap["links"] = s.links
}

func (s *queryShape) AddNode(n *Node) {
	s.nodes = append(s.nodes, *n)
}

func (s *queryShape) AddLink(l *Link) {
	s.links = append(s.links, *l)
}

func (s *queryShape) LastHasa() (int, quad.Direction) {
	return s.hasaIDs[len(s.hasaIDs)-1], s.hasaDirs[len(s.hasaDirs)-1]
}

func (s *queryShape) PushHasa(i int, d quad.Direction) {
	s.hasaIDs = append(s.hasaIDs, i)
	s.hasaDirs = append(s.hasaDirs, d)
}

func (s *queryShape) RemoveHasa() {
	s.hasaIDs = s.hasaIDs[:len(s.hasaIDs)-1]
	s.hasaDirs = s.hasaDirs[:len(s.hasaDirs)-1]
}

func (s *queryShape) StealNode(left *Node, right *Node) {
	for _, v := range right.Values {
		left.Values = append(left.Values, v)
	}
	for _, v := range right.Tags {
		left.Tags = append(left.Tags, v)
	}
	left.IsLinkNode = left.IsLinkNode || right.IsLinkNode
	left.IsFixed = left.IsFixed || right.IsFixed
	for i, link := range s.links {
		rewrite := false
		if link.LinkNode == right.ID {
			link.LinkNode = left.ID
			rewrite = true
		}
		if link.Source == right.ID {
			link.Source = left.ID
			rewrite = true
		}
		if link.Target == right.ID {
			link.Target = left.ID
			rewrite = true
		}
		if rewrite {
			s.links = append(append(s.links[:i], s.links[i+1:]...), link)
		}
	}
}

func (s *queryShape) MakeNode(it graph.Iterator) *Node {
	n := Node{ID: s.nodeID}
	for _, tag := range it.Tagger().Tags() {
		n.Tags = append(n.Tags, tag)
	}
	for k := range it.Tagger().Fixed() {
		n.Tags = append(n.Tags, k)
	}

	switch it.Type() {
	case graph.And:
		for _, sub := range it.SubIterators() {
			s.nodeID++
			newNode := s.MakeNode(sub)
			if sub.Type() != graph.Or {
				s.StealNode(&n, newNode)
			} else {
				s.AddNode(newNode)
				s.AddLink(&Link{n.ID, newNode.ID, 0, 0})
			}
		}
	case graph.Fixed:
		n.IsFixed = true
		for it.Next() {
			n.Values = append(n.Values, s.qs.NameOf(it.Result()).String())
		}
	case graph.HasA:
		hasa := it.(*HasA)
		s.PushHasa(n.ID, hasa.dir)
		s.nodeID++
		newNode := s.MakeNode(hasa.primaryIt)
		s.AddNode(newNode)
		s.RemoveHasa()
	case graph.Or:
		for _, sub := range it.SubIterators() {
			s.nodeID++
			newNode := s.MakeNode(sub)
			if sub.Type() == graph.Or {
				s.StealNode(&n, newNode)
			} else {
				s.AddNode(newNode)
				s.AddLink(&Link{n.ID, newNode.ID, 0, 0})
			}
		}
	case graph.LinksTo:
		n.IsLinkNode = true
		lto := it.(*LinksTo)
		s.nodeID++
		newNode := s.MakeNode(lto.primaryIt)
		hasaID, hasaDir := s.LastHasa()
		if (hasaDir == quad.Subject && lto.dir == quad.Object) ||
			(hasaDir == quad.Object && lto.dir == quad.Subject) {
			s.AddNode(newNode)
			if hasaDir == quad.Subject {
				s.AddLink(&Link{hasaID, newNode.ID, 0, n.ID})
			} else {
				s.AddLink(&Link{newNode.ID, hasaID, 0, n.ID})
			}
		} else if lto.primaryIt.Type() == graph.Fixed {
			s.StealNode(&n, newNode)
		} else {
			s.AddNode(newNode)
		}
	case graph.Optional:
		// Unsupported, for the moment
		fallthrough
	case graph.All:
	}
	return &n
}
