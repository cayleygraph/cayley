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

	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/quad"
)

func BuildIteratorTreeForQuery(qs graph.QuadStore, query string) graph.Iterator {
	tree := parseQuery(query)
	return buildIteratorTree(tree, qs)
}

func ParseString(input string) string {
	return parseQuery(input).String()
}

func parseQuery(input string) *peg.ExpressionTree {
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

	tree := parser.Parse(input)
	return tree
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

func buildIteratorTree(tree *peg.ExpressionTree, qs graph.QuadStore) graph.Iterator {
	switch tree.Name {
	case "Start":
		return buildIteratorTree(tree.Children[0], qs)
	case "NodeIdentifier":
		var out graph.Iterator
		nodeID := getIdentString(tree)
		if tree.Children[0].Name == "Variable" {
			allIt := qs.NodesAllIterator()
			allIt.Tagger().Add(nodeID)
			out = allIt
		} else {
			n := nodeID
			if tree.Children[0].Children[0].Name == "ColonIdentifier" {
				n = nodeID[1:]
			}
			fixed := qs.FixedIterator()
			fixed.Add(qs.ValueOf(n))
			out = fixed
		}
		return out
	case "PredIdentifier":
		i := 0
		if tree.Children[0].Name == "Reverse" {
			//Taken care of below
			i++
		}
		it := buildIteratorTree(tree.Children[i], qs)
		lto := iterator.NewLinksTo(qs, it, quad.Predicate)
		return lto
	case "RootConstraint":
		constraintCount := 0
		and := iterator.NewAnd()
		for _, c := range tree.Children {
			switch c.Name {
			case "NodeIdentifier":
				fallthrough
			case "Constraint":
				it := buildIteratorTree(c, qs)
				and.AddSubIterator(it)
				constraintCount++
				continue
			default:
				continue
			}
		}
		return and
	case "Constraint":
		var hasa *iterator.HasA
		topLevelDir := quad.Subject
		subItDir := quad.Object
		subAnd := iterator.NewAnd()
		isOptional := false
		for _, c := range tree.Children {
			switch c.Name {
			case "PredIdentifier":
				if c.Children[0].Name == "Reverse" {
					topLevelDir = quad.Object
					subItDir = quad.Subject
				}
				it := buildIteratorTree(c, qs)
				subAnd.AddSubIterator(it)
				continue
			case "PredicateKeyword":
				switch c.Children[0].Name {
				case "OptionalKeyword":
					isOptional = true
				}
			case "NodeIdentifier":
				fallthrough
			case "RootConstraint":
				it := buildIteratorTree(c, qs)
				l := iterator.NewLinksTo(qs, it, subItDir)
				subAnd.AddSubIterator(l)
				continue
			default:
				continue
			}
		}
		hasa = iterator.NewHasA(qs, subAnd, topLevelDir)
		if isOptional {
			optional := iterator.NewOptional(hasa)
			return optional
		}
		return hasa
	default:
		return &iterator.Null{}
	}
}
