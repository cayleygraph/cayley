package linkedql

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/cayleygraph/cayley/graph"
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
}

// Vertex corresponds to g.V()
type Vertex struct {
	Values []json.RawMessage `json:"values"`
}

// Type implements Step
func (s *Vertex) Type() quad.IRI {
	return prefix + "Vertex"
}

func parseValue(rawValue []byte) (quad.Value, error) {
	var a interface{}
	err := json.Unmarshal(rawValue, &a)
	if err != nil {
		return nil, err
	}
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

// BuildPath implements Step
func (s *Vertex) BuildPath(qs graph.QuadStore) *path.Path {
	var values []quad.Value
	for _, rawValue := range s.Values {
		value, err := parseValue(rawValue)
		if err != nil {
			panic(err)
		}
		values = append(values, value)
	}
	return path.StartPath(qs, values...)
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

// BuildPath implements Step
func (s *Out) BuildPath(qs graph.QuadStore) *path.Path {
	return s.From.BuildPath(qs).OutWithTags(s.Tags, s.Via.BuildPath(qs))
}
