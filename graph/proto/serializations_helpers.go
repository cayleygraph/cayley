package proto

import (
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/pquads"
)

//go:generate curl -LO https://github.com/cayleygraph/quad/raw/v1.3.0/pquads/quads.proto
//go:generate protoc --go_opt=paths=source_relative --proto_path=. --go_out=. serializations.proto

// GetNativeValue returns the value stored in Node.
func (m *NodeData) GetNativeValue() quad.Value {
	if m == nil {
		return nil
	} else if m.Value == nil {
		if m.Name == "" {
			return nil
		}
		return quad.Raw(m.Name)
	}
	return m.Value.ToNative()
}

func (m *NodeData) Upgrade() {
	if m.Value == nil {
		m.Value = pquads.MakeValue(quad.Raw(m.Name))
		m.Name = ""
	}
}
