package peg

import (
	"fmt"
)

type (
	// Expressions
	Exp interface {
		Match(int) (int, *Result)
	}	
	Terminal struct {
		*Parser
		Character int
	}
	NonTerminal struct {
		*Parser
		Name string
		Expression Exp
	}
	Sequence struct {
		*Parser
		Expressions []Exp
	}
	OrderedChoice struct {
		*Parser
		Expressions []Exp
	}
	ZeroOrMore struct {
		*Parser
		Expression Exp
	}
	OneOrMore struct {
		*Parser
		Expression Exp
	}
	Optional struct {
		*Parser
		Expression Exp
	}
	AndPredicate struct {
		*Parser
		Expression Exp
	}
	NotPredicate struct {
		*Parser
		Expression Exp
	}
)

// Terminal
func (this *Terminal) Match(offset int) (int, *Result) {
	if offset < len(this.Parser.working) && int(this.Parser.working[offset]) == this.Character {
		return offset + 1, this.Parser.pass(offset, 1, this, nil)
	}
	return offset, this.Parser.fail(offset, 0, this, nil)
}
func (this *Terminal) String() string {
	return "<" + string(this.Character) + ">"
}
// Non Terminal
func (this *NonTerminal) Match(offset int) (int, *Result) {
	o, r := this.Expression.Match(offset)
	if r.Valid {
		return o, this.Parser.pass(offset, o-offset, this, []*Result{r})
	}
	return offset, this.Parser.fail(offset, o, this, []*Result{r})
}
func (this *NonTerminal) String() string {
	return "(" + this.Name + ":" + fmt.Sprint(this.Expression) + ")"
}
// Sequence
func (this *Sequence) Match(offset int) (int, *Result) {
	o := offset
	var r *Result
	children := make([]*Result, 0)
	for _, exp := range this.Expressions {
		o, r = exp.Match(o)
		children = append(children, r)
		if !r.Valid {
			return offset, this.Parser.fail(o, o-offset, this, children)
		}
	}
	return o, this.Parser.pass(offset, o-offset, this, children)
}
func (this *Sequence) String() string {
	str := ""
	for i, e := range this.Expressions {
		if i > 0 {
			str += " "
		}
		str += fmt.Sprint(e)
	}
	return "(" + str + ")"
}
// Ordered Choice
func (this *OrderedChoice) Match(start int) (int, *Result) {
	children := make([]*Result, 0)
	for _, exp := range this.Expressions {
		i, r := exp.Match(start)
		
		if r.Valid {
			return i, this.Parser.pass(i, i-start, this, []*Result{r})
		}
		
		children = append(children, r)
	}
	return start, this.Parser.fail(start, 0, this, children)
}
func (this *OrderedChoice) String() string {
	str := ""
	for i, e := range this.Expressions {
		if i > 0 {
			str += " / "
		}
		str += fmt.Sprint(e)
	}
	return str
}
// Zero or More
func (this *ZeroOrMore) Match(offset int) (int, *Result) {
	o := offset
	children := make([]*Result, 0)
	for {
		i, r := this.Expression.Match(o)
		if !r.Valid {
			break
		}
		children = append(children, r)
		o = i
	}
	return o, this.Parser.pass(offset, o-offset, this, children)
}
func (this *ZeroOrMore) String() string {
	return "(" + fmt.Sprint(this.Expression) + ")*"
}
// One or More
func (this *OneOrMore) Match(offset int) (int, *Result) {
	o := offset
	children := make([]*Result, 0)
	for {
		i, r := this.Expression.Match(o)
		if !r.Valid {
			if len(children) == 0 {
				return offset, this.Parser.fail(i, i-offset, this, []*Result{r})
			}
			break
		}
		children = append(children, r)
		o = i
	}
	return o, this.Parser.pass(offset, o-offset, this, children)
}
func (this *OneOrMore) String() string {
	return "(" + fmt.Sprint(this.Expression) + ")+"
}
// Optional
func (this *Optional) Match(start int) (int, *Result) {
	current, result := this.Expression.Match(start)
	if result.Valid {
		return current, this.Parser.pass(current, current-start, this, []*Result{result})
	}
	return start, this.Parser.pass(start, 0, this, nil)
}
func (this *Optional) String() string {
	return "(" + fmt.Sprint(this.Expression) + ")?"
}
// And Predicate
func (this *AndPredicate) Match(start int) (int, *Result) {
	// Ignore the new position
	_, result := this.Expression.Match(start)
	if result.Valid {
		return start, this.Parser.pass(start, 0, this, []*Result{result})
	}	
	return start, this.Parser.fail(start, 0, this, []*Result{result})
}
func (this *AndPredicate) String() string {
	return "&(" + fmt.Sprint(this.Expression) + ")"
}
// Not Predicate
func (this *NotPredicate) Match(start int) (int, *Result) {
	// Ignore the new position
	_, result := this.Expression.Match(start)
	if !result.Valid {
		return start, this.Parser.pass(start, 0, this, []*Result{result})
	}
	return start, this.Parser.fail(start, 0, this, []*Result{result})
}
func (this *NotPredicate) String() string {
	return "!(" + fmt.Sprint(this.Expression) + ")"
}
