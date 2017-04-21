package graph

type Var struct {
	name string
}

// NewVar creates a variable that can be used in place of a graph.Value
func NewVar(name string) Var {
	return Var{
		// name: string(uuid.NewV4()),
		name: name,
	}
}

func (v Var) String() string {
	return v.name
}

func (v Var) Native() interface{} {
	return v.name
}
