package graph

type variable struct {
	CurrentValue Value
	isBound      bool
}

type VarBinder struct {
	variable *variable
}

type VarUser struct {
	variable *variable
}

// NewVariable creates a variable that can be used in place of a graph.Value
// in a
func NewVariable() variable {
	return variable{}
}

func (v *variable) Use() *VarUser {
	return &VarUser{
		variable: v,
	}
}

func (v *variable) Bind() *VarBinder {
	if !v.isBound {
		v.isBound = true
		return &VarBinder{
			variable: v,
		}
	}
	return nil
	// return nil, errors.New("You can't bind a variable twice")
}

func (u *VarUser) GetCurrentValue() Value {
	return u.variable.CurrentValue
}

func (b *VarBinder) GetCurrentValue() Value {
	return b.variable.CurrentValue
}

func (b *VarBinder) SetNewValue(val Value) {
	b.variable.CurrentValue = val
}

func (b *VarBinder) ToUser() *VarUser {
	return &VarUser{
		variable: b.variable,
	}
}
