package linkedql

import (
	"testing"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad/voc"

	"github.com/stretchr/testify/require"
)

func init() {
	Register(&TestStep{})
}

var unmarshalCases = []struct {
	name string
	data string
	exp  Step
}{
	{
		name: "simple",
		data: `{
	"@type": "linkedql:TestStep",
	"linkedql:limit": 10
}`,
		exp: &TestStep{Limit: 10},
	},
	{
		name: "simple",
		data: `{
	"@type": "linkedql:TestStep",
	"linkedql:tags": ["a", "b"]
}`,
		exp: &TestStep{Tags: []string{"a", "b"}},
	},
	{
		name: "nested",
		data: `{
	"@type": "linkedql:TestStep",
	"linkedql:limit": 10,
	"linkedql:from": {
		"@type": "linkedql:TestStep",
		"linkedql:limit": 15,
		"linkedql:from": {
			"@type": "linkedql:TestStep",
			"linkedql:limit": 20
		}
	}
}`,
		exp: &TestStep{
			Limit: 10,
			From: &TestStep{
				Limit: 15,
				From: &TestStep{
					Limit: 20,
				},
			},
		},
	},
	{
		name: "nested slice",
		data: `{
	"@type": "linkedql:TestStep",
	"linkedql:limit": 10,
	"linkedql:sub": [
		{
			"@type": "linkedql:TestStep",
			"linkedql:limit": 15
		},
		{
			"@type": "linkedql:TestStep",
			"linkedql:limit": 20
		}
	]
}`,
		exp: &TestStep{
			Limit: 10,
			Sub: []PathStep{
				&TestStep{
					Limit: 15,
				},
				&TestStep{
					Limit: 20,
				},
			},
		},
	},
}

type TestStep struct {
	Limit int        `json:"limit"`
	Tags  []string   `json:"tags"`
	From  PathStep   `json:"from"`
	Sub   []PathStep `json:"sub"`
}

func (s *TestStep) Description() string {
	return "A TestStep for checking the registry"
}

func (s *TestStep) BuildIterator(qs graph.QuadStore, ns *voc.Namespaces) (query.Iterator, error) {
	panic("Can't build iterator for TestStep")
}

func (s *TestStep) BuildPath(qs graph.QuadStore, ns *voc.Namespaces) (*path.Path, error) {
	panic("Can't build path for TestStep")
}

func TestUnmarshalStep(t *testing.T) {
	for _, c := range unmarshalCases {
		t.Run(c.name, func(t *testing.T) {
			s, err := Unmarshal([]byte(c.data))
			require.NoError(t, err)
			require.Equal(t, c.exp, s)
		})
	}
}
