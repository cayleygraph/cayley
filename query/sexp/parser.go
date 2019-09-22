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

package sexp

import (
	"github.com/badgerodon/peg"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/graph/shape"
	"github.com/cayleygraph/quad"
)

func BuildIteratorTreeForQuery(qs graph.QuadStore, query string) graph.Iterator {
	s, err := BuildShape(query)
	if err != nil {
		return iterator.NewError(err)
	}
	return shape.BuildIterator(qs, s)
}

func BuildShape(query string) (shape.Shape, error) {
	tree := parseQuery(query)
	s, _ := buildShape(tree)
	s, _ = shape.Optimize(s, nil)
	return s, nil
}

func ParseString(input string) string {
	return parseQuery(input).String()
}

func newParser() *peg.Parser {
	parser := peg.NewParser()

	start := parser.NonTerminal("Start")
	whitespace := parser.NonTerminal("Whitespace")
	quotedString := parser.NonTerminal("QuotedString")
	rootConstraint := parser.NonTerminal("RootConstraint")

	constraint := parser.NonTerminal("Constraint")
	colonIdentifier := parser.NonTerminal("ColonIdentifier")
	variable := parser.NonTerminal("Variable")
	identifier := parser.NonTerminal("Identifier")
	fixedNode := parser.NonTerminal("FixedNode")
	nodeIdent := parser.NonTerminal("NodeIdentifier")
	predIdent := parser.NonTerminal("PredIdentifier")
	reverse := parser.NonTerminal("Reverse")
	predKeyword := parser.NonTerminal("PredicateKeyword")
	optional := parser.NonTerminal("OptionalKeyword")

	start.Expression = rootConstraint

	whitespace.Expression = parser.OneOrMore(
		parser.OrderedChoice(
			parser.Terminal(' '),
			parser.Terminal('\t'),
			parser.Terminal('\n'),
			parser.Terminal('\r'),
		),
	)

	quotedString.Expression = parser.Sequence(
		parser.Terminal('"'),
		parser.OneOrMore(
			parser.OrderedChoice(
				parser.Range('0', '9'),
				parser.Range('a', 'z'),
				parser.Range('A', 'Z'),
				parser.Terminal('_'),
				parser.Terminal('/'),
				parser.Terminal(':'),
				parser.Terminal(' '),
				parser.Terminal('\''),
			),
		),
		parser.Terminal('"'),
	)

	predKeyword.Expression = parser.OrderedChoice(
		optional,
	)

	optional.Expression = parser.Sequence(
		parser.Terminal('o'),
		parser.Terminal('p'),
		parser.Terminal('t'),
		parser.Terminal('i'),
		parser.Terminal('o'),
		parser.Terminal('n'),
		parser.Terminal('a'),
		parser.Terminal('l'),
	)

	identifier.Expression = parser.OneOrMore(
		parser.OrderedChoice(
			parser.Range('0', '9'),
			parser.Range('a', 'z'),
			parser.Range('A', 'Z'),
			parser.Terminal('_'),
			parser.Terminal('.'),
			parser.Terminal('/'),
			parser.Terminal(':'),
			parser.Terminal('#'),
		),
	)

	reverse.Expression = parser.Terminal('!')

	variable.Expression = parser.Sequence(
		parser.Terminal('$'),
		identifier,
	)

	colonIdentifier.Expression = parser.Sequence(
		parser.Terminal(':'),
		identifier,
	)

	fixedNode.Expression = parser.OrderedChoice(
		colonIdentifier,
		quotedString,
	)

	nodeIdent.Expression = parser.OrderedChoice(
		variable,
		fixedNode,
	)

	predIdent.Expression = parser.Sequence(
		parser.Optional(reverse),
		parser.OrderedChoice(
			nodeIdent,
			constraint,
		),
	)

	constraint.Expression = parser.Sequence(
		parser.Terminal('('),
		parser.Optional(whitespace),
		predIdent,
		parser.Optional(whitespace),
		parser.Optional(predKeyword),
		parser.Optional(whitespace),
		parser.OrderedChoice(
			nodeIdent,
			rootConstraint,
		),
		parser.Optional(whitespace),
		parser.Terminal(')'),
	)

	rootConstraint.Expression = parser.Sequence(
		parser.Terminal('('),
		parser.Optional(whitespace),
		nodeIdent,
		parser.Optional(whitespace),
		parser.ZeroOrMore(parser.Sequence(
			constraint,
			parser.Optional(whitespace),
		)),
		parser.Terminal(')'),
	)
	return parser
}

func parseQuery(input string) *peg.ExpressionTree {
	return newParser().Parse(input)
}

func getIdentString(tree *peg.ExpressionTree) string {
	out := ""
	if len(tree.Children) > 0 {
		for _, child := range tree.Children {
			out += getIdentString(child)
		}
	} else {
		if tree.Value != '"' {
			out += string(tree.Value)
		}
	}
	return out
}

func lookup(s string) shape.Shape {
	return shape.Lookup{quad.StringToValue(s)}
}

func buildShape(tree *peg.ExpressionTree) (_ shape.Shape, opt bool) {
	switch tree.Name {
	case "Start":
		return buildShape(tree.Children[0])
	case "NodeIdentifier":
		var out shape.Shape
		nodeID := getIdentString(tree)
		if tree.Children[0].Name == "Variable" {
			out = shape.Save{
				From: shape.AllNodes{},
				Tags: []string{nodeID},
			}
		} else {
			n := nodeID
			if tree.Children[0].Children[0].Name == "ColonIdentifier" {
				n = nodeID[1:]
			}
			out = lookup(n)
		}
		return out, false
	case "PredIdentifier":
		i := 0
		if tree.Children[0].Name == "Reverse" {
			//Taken care of below
			i++
		}
		it, _ := buildShape(tree.Children[i])
		return shape.Quads{
			{Dir: quad.Predicate, Values: it},
		}, false
	case "RootConstraint":
		var and shape.IntersectOpt
		for _, c := range tree.Children {
			switch c.Name {
			case "NodeIdentifier":
				fallthrough
			case "Constraint":
				it, opt := buildShape(c)
				if opt {
					and.AddOptional(it)
				} else {
					and.Add(it)
				}
				continue
			default:
				continue
			}
		}
		return and, false
	case "Constraint":
		topLevelDir := quad.Subject
		subItDir := quad.Object
		var subAnd shape.IntersectOpt
		isOptional := false
		for _, c := range tree.Children {
			switch c.Name {
			case "PredIdentifier":
				if c.Children[0].Name == "Reverse" {
					topLevelDir = quad.Object
					subItDir = quad.Subject
				}
				it, opt := buildShape(c)
				if opt {
					subAnd.AddOptional(it)
				} else {
					subAnd.Add(it)
				}
				continue
			case "PredicateKeyword":
				switch c.Children[0].Name {
				case "OptionalKeyword":
					isOptional = true
				}
			case "NodeIdentifier":
				fallthrough
			case "RootConstraint":
				it, opt := buildShape(c)
				l := shape.Quads{
					{Dir: subItDir, Values: it},
				}
				if opt {
					subAnd.AddOptional(l)
				} else {
					subAnd.Add(l)
				}
				continue
			default:
				continue
			}
		}
		return shape.NodesFrom{
			Dir:   topLevelDir,
			Quads: subAnd,
		}, isOptional
	default:
		return shape.Null{}, false
	}
}
