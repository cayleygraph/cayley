package peg

import(
	//"fmt"
)
type (
	ExpressionTree struct {
		Name string
		Children []*ExpressionTree
		Value int
	}
)

// collapse the expression tree (removing anything that isn't a NonTerminal or
//   a Terminal)
func (this *ExpressionTree) collapse() {
	ncs := make([]*ExpressionTree, 0)
	for _, c := range this.Children {
		c.collapse()
		if c.Name == "" && c.Value == -1 {
			for _, gc := range c.Children {
				ncs = append(ncs, gc)
			}
		} else {
			ncs = append(ncs, c)
		}
	}
	this.Children = ncs
}
func (this *ExpressionTree) toString(indent int) string {
	str := ""
	for i := 0; i < indent; i++ {
		str += " "
	}
	if this.Value > -1 {
		str += "<" + string(this.Value) + ">"
	} else {
		str += this.Name
	}
	for _, t := range this.Children {
		str += "\n" + t.toString(indent + 2)
	}
	return str
}
func (this *ExpressionTree) String() string {
	return this.toString(0)
}
