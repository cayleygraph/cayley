package pquads

import (
	"fmt"
	"time"

	"github.com/cayleygraph/cayley/quad"
)

//go:generate protoc --proto_path=$GOPATH/src:. --gogo_out=. quads.proto

// MakeValue converts quad.Value to its protobuf representation.
func MakeValue(qv quad.Value) *Value {
	if qv == nil {
		return nil
	}
	switch v := qv.(type) {
	case quad.Raw:
		return &Value{&Value_Raw{[]byte(v)}}
	case quad.String:
		return &Value{&Value_Str{string(v)}}
	case quad.IRI:
		return &Value{&Value_Iri{string(v)}}
	case quad.BNode:
		return &Value{&Value_Bnode{string(v)}}
	case quad.TypedString:
		return &Value{&Value_TypedStr{&Value_TypedString{
			Value: string(v.Value),
			Type:  string(v.Type),
		}}}
	case quad.LangString:
		return &Value{&Value_LangStr{&Value_LangString{
			Value: string(v.Value),
			Lang:  v.Lang,
		}}}
	case quad.Int:
		return &Value{&Value_Int{int64(v)}}
	case quad.Float:
		return &Value{&Value_Float{float64(v)}}
	case quad.Bool:
		return &Value{&Value_Boolean{bool(v)}}
	case quad.Time:
		t := time.Time(v)
		seconds := t.Unix()
		nanos := int32(t.Sub(time.Unix(seconds, 0)))
		return &Value{&Value_Time{&Value_Timestamp{
			Seconds: seconds,
			Nanos:   nanos,
		}}}
	default:
		panic(fmt.Errorf("unsupported type: %T", qv))
	}
}

// MarshalValue is a helper for serialization of quad.Value.
func MarshalValue(v quad.Value) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	return MakeValue(v).Marshal()
}

// UnmarshalValue is a helper for deserialization of quad.Value.
func UnmarshalValue(data []byte) (quad.Value, error) {
	if len(data) == 0 {
		return nil, nil
	}
	var v Value
	if err := v.Unmarshal(data); err != nil {
		return nil, err
	}
	return v.ToNative(), nil
}

// ToNative converts protobuf Value to quad.Value.
func (m *Value) ToNative() (qv quad.Value) {
	if m == nil {
		return nil
	}
	switch v := m.Value.(type) {
	case *Value_Raw:
		return quad.Raw(v.Raw)
	case *Value_Str:
		return quad.String(v.Str)
	case *Value_Iri:
		return quad.IRI(v.Iri)
	case *Value_Bnode:
		return quad.BNode(v.Bnode)
	case *Value_TypedStr:
		return quad.TypedString{
			Value: quad.String(v.TypedStr.Value),
			Type:  quad.IRI(v.TypedStr.Type),
		}
	case *Value_LangStr:
		return quad.LangString{
			Value: quad.String(v.LangStr.Value),
			Lang:  v.LangStr.Lang,
		}
	case *Value_Int:
		return quad.Int(v.Int)
	case *Value_Float:
		return quad.Float(v.Float)
	case *Value_Boolean:
		return quad.Bool(v.Boolean)
	case *Value_Time:
		var t time.Time
		if v.Time == nil {
			t = time.Unix(0, 0).UTC()
		} else {
			t = time.Unix(v.Time.Seconds, int64(v.Time.Nanos)).UTC()
		}
		return quad.Time(t)
	default:
		panic(fmt.Errorf("unsupported type: %T", m.Value))
	}
}

// ToNative converts protobuf Value to quad.Value.
func (m *StrictQuad_Ref) ToNative() (qv quad.Value) {
	if m == nil {
		return nil
	}
	switch v := m.Value.(type) {
	case *StrictQuad_Ref_Iri:
		return quad.IRI(v.Iri)
	case *StrictQuad_Ref_BnodeLabel:
		return quad.BNode(v.BnodeLabel)
	default:
		panic(fmt.Errorf("unsupported type: %T", m.Value))
	}
}

// MakeQuad converts quad.Quad to its protobuf representation.
func MakeQuad(q quad.Quad) *Quad {
	return &Quad{
		SubjectValue:   MakeValue(q.Subject),
		PredicateValue: MakeValue(q.Predicate),
		ObjectValue:    MakeValue(q.Object),
		LabelValue:     MakeValue(q.Label),
	}
}

func makeRef(v quad.Value) (*StrictQuad_Ref, error) {
	if v == nil {
		return nil, nil
	}
	var sv isStrictQuad_Ref_Value
	switch v := v.(type) {
	case quad.BNode:
		sv = &StrictQuad_Ref_BnodeLabel{BnodeLabel: string(v)}
	case quad.IRI:
		sv = &StrictQuad_Ref_Iri{Iri: string(v)}
	default:
		return nil, fmt.Errorf("unexpected type for ref: %T", v)
	}
	return &StrictQuad_Ref{Value: sv}, nil
}

func makeWireQuad(q quad.Quad) *WireQuad {
	return &WireQuad{
		Subject:   MakeValue(q.Subject),
		Predicate: MakeValue(q.Predicate),
		Object:    MakeValue(q.Object),
		Label:     MakeValue(q.Label),
	}
}

func makeStrictQuad(q quad.Quad) (sq *StrictQuad, err error) {
	sq = new(StrictQuad)
	if sq.Subject, err = makeRef(q.Subject); err != nil {
		return nil, err
	}
	if sq.Predicate, err = makeRef(q.Predicate); err != nil {
		return nil, err
	}
	sq.Object = MakeValue(q.Object)
	if sq.Label, err = makeRef(q.Label); err != nil {
		return nil, err
	}
	return sq, nil
}

// ToNative converts protobuf Quad to quad.Quad.
func (m *Quad) ToNative() (q quad.Quad) {
	if m == nil {
		return
	}
	if m.SubjectValue != nil {
		q.Subject = m.SubjectValue.ToNative()
	} else if m.Subject != "" {
		q.Subject = quad.Raw(m.Subject)
	}
	if m.PredicateValue != nil {
		q.Predicate = m.PredicateValue.ToNative()
	} else if m.Predicate != "" {
		q.Predicate = quad.Raw(m.Predicate)
	}
	if m.ObjectValue != nil {
		q.Object = m.ObjectValue.ToNative()
	} else if m.Object != "" {
		q.Object = quad.Raw(m.Object)
	}
	if m.LabelValue != nil {
		q.Label = m.LabelValue.ToNative()
	} else if m.Label != "" {
		q.Label = quad.Raw(m.Label)
	}
	return
}

// ToNative converts protobuf StrictQuad to quad.Quad.
func (m *StrictQuad) ToNative() (q quad.Quad) {
	if m == nil {
		return
	}
	if m.Subject != nil {
		q.Subject = m.Subject.ToNative()
	}
	if m.Predicate != nil {
		q.Predicate = m.Predicate.ToNative()
	}
	if m.Object != nil {
		q.Object = m.Object.ToNative()
	}
	if m.Label != nil {
		q.Label = m.Label.ToNative()
	}
	return
}

// ToNative converts protobuf WireQuad to quad.Quad.
func (m *WireQuad) ToNative() (q quad.Quad) {
	if m == nil {
		return
	}
	if m.Subject != nil {
		q.Subject = m.Subject.ToNative()
	}
	if m.Predicate != nil {
		q.Predicate = m.Predicate.ToNative()
	}
	if m.Object != nil {
		q.Object = m.Object.ToNative()
	}
	if m.Label != nil {
		q.Label = m.Label.ToNative()
	}
	return
}

func (m *Quad) Upgrade() {
	if m.SubjectValue == nil {
		m.SubjectValue = MakeValue(quad.Raw(m.Subject))
		m.Subject = ""
	}
	if m.PredicateValue == nil {
		m.PredicateValue = MakeValue(quad.Raw(m.Predicate))
		m.Predicate = ""
	}
	if m.ObjectValue == nil {
		m.ObjectValue = MakeValue(quad.Raw(m.Object))
		m.Object = ""
	}
	if m.LabelValue == nil && m.Label != "" {
		m.LabelValue = MakeValue(quad.Raw(m.Label))
		m.Label = ""
	}
}
