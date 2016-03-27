package peg

import(
	//"fmt"
)

type (
	Parser struct {
		working string
		rules map[string]*NonTerminal
	}
	Result struct {
		*Parser
		Valid bool
		Offset int
		Length int
		Expression Exp
		Children []*Result
	}
)

// Result
func (this *Result) toTree() *ExpressionTree {
	children := make([]*ExpressionTree, 0)
	if this.Valid {
		for _, r := range this.Children {
			children = append(children, r.toTree())
		}
		switch exp := this.Expression.(type) {
		case *Terminal:
			return &ExpressionTree{"",children,exp.Character}
		case *NonTerminal:
			return &ExpressionTree{exp.Name,children,-1}
		}
	}
	return &ExpressionTree{"",children,-1}
}


// Create a new PEG Parser
func NewParser() *Parser {
	return &Parser{"",make(map[string]*NonTerminal)}
}
func (this *Parser) pass(offset, length int, exp Exp, children []*Result) *Result {
	if children == nil {
		children = make([]*Result, 0)
	}
	return &Result{this,true,offset,length,exp,children}
}
func (this *Parser) fail(offset, length int, exp Exp, children []*Result) *Result {
	if children == nil {
		children = make([]*Result, 0)
	}
	return &Result{this,false,offset,length,exp,children}
}
func (this *Parser) Terminal(ch int) *Terminal {
	return &Terminal{this,ch}
}
func (this *Parser) NonTerminal(name string) *NonTerminal {
	e := &NonTerminal{this,name,nil}
	this.rules[name] = e
	return e
}
func (this *Parser) Sequence(exps ... Exp) *Sequence {
	return &Sequence{this,exps}
}
func (this *Parser) OrderedChoice(exps ... Exp) *OrderedChoice {
	return &OrderedChoice{this,exps}
}
func (this *Parser) ZeroOrMore(exp Exp) *ZeroOrMore {
	return &ZeroOrMore{this,exp}
}
func (this *Parser) OneOrMore(exp Exp) *OneOrMore {
	return &OneOrMore{this,exp}
}
func (this *Parser) Optional(exp Exp) *Optional {
	return &Optional{this,exp}
}
func (this *Parser) AndPredicate(exp Exp) *AndPredicate {
	return &AndPredicate{this,exp}
}
func (this *Parser) NotPredicate(exp Exp) *NotPredicate {
	return &NotPredicate{this,exp}
}

// Extensions for easier construction
func (this *Parser) Range(start, end int) *OrderedChoice {
	exps := make([]Exp, (end - start)+1)
	for i := start; i <= end; i++ {
		exps[i-start] = &Terminal{this,i}
	}
	return &OrderedChoice{this,exps}
}

func (this *Parser) Parse(text string) *ExpressionTree {
	this.working = text
	if start, ok := this.rules["Start"]; ok {
		_, r := start.Match(0)
		t := r.toTree()
		t.collapse()
		return t
	} 
	panic("No starting rule defined. Create a non terminal named \"Start\"")
}
