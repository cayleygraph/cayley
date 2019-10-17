package linkedql

import (
	"errors"
	"strings"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/voc"
)

const namespace = "http://cayley.io/linkedql#"
const prefix = "linkedql:"

func init() {
	voc.Register(voc.Namespace{Full: namespace, Prefix: prefix})
	Register(&Vertex{})
	Register(&Out{})
	Register(&As{})
	Register(&TagArray{})
	Register(&Value{})
	Register(&Intersect{})
}

// Vertex corresponds to g.V()
type Vertex struct {
	Values []quad.Value `json:"values"`
}

// Type implements Step
func (s *Vertex) Type() quad.IRI {
	return prefix + "Vertex"
}

func parseValue(a interface{}) (quad.Value, error) {
	switch a := a.(type) {
	case string:
		return quad.String(a), nil
	case map[string]interface{}:
		id, ok := a["@id"].(string)
		if ok {
			if strings.HasPrefix(id, "_:") {
				return quad.BNode(id[2:]), nil
			}
			return quad.IRI(id), nil
		}
		_, ok = a["@value"].(string)
		if ok {
			panic("Doesn't support special literals yet")
		}
	}
	return nil, errors.New("Couldn't parse rawValue to a quad.Value")
}

// BuildIterator implements Step
func (s *Vertex) BuildIterator(qs graph.QuadStore) query.Iterator {
	path := path.StartPath(qs, s.Values...)
	return NewValueIterator(path, qs)
}

// Out corresponds to .out()
type Out struct {
	From Step     `json:"from"`
	Via  Step     `json:"via"`
	Tags []string `json:"tags"`
}

// Type implements Step
func (s *Out) Type() quad.IRI {
	return prefix + "Out"
}

// BuildIterator implements Step
func (s *Out) BuildIterator(qs graph.QuadStore) query.Iterator {
	fromIt, ok := s.From.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("Out must be called from ValueIterator")
	}
	viaIt, ok := s.Via.BuildIterator(qs).(*ValueIterator)
	path := fromIt.path.OutWithTags(s.Tags, viaIt.path)
	return NewValueIterator(path, qs)
}

// As corresponds to .tag()
type As struct {
	From Step     `json:"from"`
	Tags []string `json:"tags"`
}

// Type implements Step
func (s *As) Type() quad.IRI {
	return prefix + "As"
}

// BuildIterator implements Step
func (s *As) BuildIterator(qs graph.QuadStore) query.Iterator {
	fromIt, ok := s.From.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("As must be called from ValueIterator")
	}
	path := fromIt.path.Tag(s.Tags...)
	return NewValueIterator(path, qs)
}

// TagArray corresponds to .tagArray()
type TagArray struct {
	From Step `json:"from"`
}

// Type implements Step
func (s *TagArray) Type() quad.IRI {
	return prefix + "TagArray"
}

// BuildIterator implements Step
func (s *TagArray) BuildIterator(qs graph.QuadStore) query.Iterator {
	fromIt, ok := s.From.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("TagArray must be called from ValueIterator")
	}
	return &TagArrayIterator{fromIt}
}

// Value corresponds to .value()
type Value struct {
	From Step `json:"from"`
}

// Type implements Step
func (s *Value) Type() quad.IRI {
	return prefix + "Value"
}

// BuildIterator implements Step
func (s *Value) BuildIterator(qs graph.QuadStore) query.Iterator {
	fromIt, ok := s.From.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("Value must be called from ValueIterator")
	}
	// TODO(@iddan): support non iterators for query result
	return fromIt
}

// Intersect represents .intersect() and .and()
type Intersect struct {
	From        Step `json:"from"`
	Intersectee Step `json:"intersectee"`
}

// Type implements Step
func (s *Intersect) Type() quad.IRI {
	return prefix + "Intersect"
}

// BuildIterator implements Step
func (s *Intersect) BuildIterator(qs graph.QuadStore) query.Iterator {
	fromIt, ok := s.From.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("Intersect must be called from ValueIterator")
	}
	intersecteeIt, ok := s.Intersectee.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("Intersect must be called with ValueIterator")
	}
	return NewValueIterator(fromIt.path.And(intersecteeIt.path), qs)
}

type Is struct {
	Values []quad.Value `json:"values"`
}
