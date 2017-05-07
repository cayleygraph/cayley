package proto

import "github.com/cayleygraph/cayley/quad"

//go:generate protoc --proto_path=$GOPATH/src:. --gogo_out=. primitive.proto

func (p Primitive) GetDirection(d quad.Direction) uint64 {
	switch d {
	case quad.Subject:
		return p.Subject
	case quad.Predicate:
		return p.Predicate
	case quad.Object:
		return p.Object
	case quad.Label:
		return p.Label
	}
	panic("unknown direction")
}

func (p *Primitive) SetDirection(d quad.Direction, v uint64) {
	switch d {
	case quad.Subject:
		p.Subject = v
	case quad.Predicate:
		p.Predicate = v
	case quad.Object:
		p.Object = v
	case quad.Label:
		p.Label = v
	}
}

func (p Primitive) IsNode() bool {
	return len(p.Value) != 0
}

func (p Primitive) Key() interface{} {
	return p.ID
}

func (p *Primitive) IsSameLink(q *Primitive) bool {
	return p.Subject == q.Subject && p.Predicate == q.Predicate && p.Object == q.Object && p.Label == q.Label
}
