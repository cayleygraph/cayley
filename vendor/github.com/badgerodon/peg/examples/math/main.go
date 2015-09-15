package main

import (
	"github.com/badgerodon/peg"
	"fmt"
	"strconv"
)

type (
	op struct {
		val int
		op int
		next *op
	}
)

var (
	prec = []int{'*','/','%','+','-'}
	ops = map[int]func(int,int)int{
		'*': func(a,b int) int {
			return a * b
		},
		'/': func(a,b int) int {
			return a / b
		},
		'%': func(a,b int) int {
			return a % b
		},
		'+': func(a,b int) int {
			return a + b
		},
		'-': func(a,b int) int {
			return a - b
		},
	}
)

func main() {
	parser := peg.NewParser()
	
	start := parser.NonTerminal("Start")
	expr := parser.NonTerminal("Expression")
	paren := parser.NonTerminal("Parentheses")
	number := parser.NonTerminal("Number")
	
	start.Expression = expr
	expr.Expression = parser.Sequence(
		parser.OrderedChoice(
			paren,
			number,
		),
		parser.Optional(
			parser.Sequence(
				parser.OrderedChoice(
					parser.Terminal('-'),
					parser.Terminal('+'),
					parser.Terminal('*'),
					parser.Terminal('/'),
				),
				expr,
			),
		),
	)
	paren.Expression = parser.Sequence(
		parser.Terminal('('),
		expr,
		parser.Terminal(')'),
	)
	number.Expression = parser.ZeroOrMore(
		parser.Range('0','9'),
	)
	
	tree := parser.Parse("1+1+3*11-5+(2+3)*2/20")
	fmt.Println(tree)
	fmt.Println(reduce(tree))
}
func reduce(tree *peg.ExpressionTree) int {
	// If we're at a number just parse it
	if tree.Name == "Number" {
		str := ""
		for _, c := range tree.Children {
			str += string(c.Value)
		}
		i, _ := strconv.Atoi(str)
		return i
	}
	
	// We have to collapse all sub expressions into a flattened linked list
	//   of expressions each of which has an operator. We will then execute
	//   each of the operators in order of precedence.
	fst := &op{0,'+',nil}
	lst := fst
	var visit func(*peg.ExpressionTree)
	visit = func(t *peg.ExpressionTree) {
		switch t.Name {
		case "Expression":
			if len(t.Children) > 1 {
				nxt := &op{reduce(t.Children[0]),t.Children[1].Value, nil}
				lst.next = nxt
				lst = nxt
				visit(t.Children[2])
				return
			}
		case "Parentheses":
			nxt := &op{reduce(t.Children[1]),0,nil}
			lst.next = nxt
			lst = nxt
			return
		}
		
		if len(t.Children) > 0 {
			nxt := &op{reduce(t.Children[0]),0,nil}
			lst.next = nxt
			lst = nxt
		}
	}
	visit(tree)
	
	// Foreach operator in order of precedence
	for _, o := range prec {
		cur := fst
		for cur.next != nil {
			if cur.op == o {
				cur.val = ops[o](cur.val, cur.next.val)
				cur.op = cur.next.op
				cur.next = cur.next.next
			} else {
				cur = cur.next
			}
		}
	}
	return fst.val
}
func (this *op) String() string {
	str := ""
	if this.op == 0 {
		str = "(" + fmt.Sprint(this.val) + ") "
	} else {	
		str = "(" + fmt.Sprint(this.val) + " " + string(this.op) + ") "
	}
	if this.next != nil {
		str += this.next.String()
	}
	return str
}
