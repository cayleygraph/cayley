package search

import (
	"context"
	"testing"

	"github.com/blevesearch/bleve"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/graph/refs"
	"github.com/cayleygraph/quad"
	"github.com/stretchr/testify/require"
)

type testCase struct {
	Name     string
	Data     []quad.Quad
	Config   DocumentConfig
	Expected []document
	Entities []quad.Value
}

var testCases = []testCase{
	{
		Name: "Simple",
		Data: []quad.Quad{
			quad.Quad{
				Subject:   quad.IRI("alice"),
				Predicate: quad.IRI("name"),
				Object:    quad.String("Alice Liddell"),
				Label:     quad.IRI(""),
			},
		},
		Config: DocumentConfig{
			Name: "people",
			Properties: []PropertyConfig{
				{
					Name: quad.IRI("name"),
				},
			},
		},
		Expected: []document{
			{
				id: "alice",
				data: map[string]interface{}{
					"_type": "people",
					"name":  []interface{}{"Alice Liddell"},
				},
			},
		},
	},
	{
		Name: "With Match",
		Data: []quad.Quad{
			quad.Quad{
				Subject:   quad.IRI("alice"),
				Predicate: quad.IRI("name"),
				Object:    quad.String("Alice Liddell"),
				Label:     quad.IRI(""),
			},
			quad.Quad{
				Subject:   quad.IRI("alice"),
				Predicate: quad.IRI("rdf:type"),
				Object:    quad.IRI("Person"),
				Label:     quad.IRI(""),
			},
			quad.Quad{
				Subject:   quad.IRI("bob"),
				Predicate: quad.IRI("name"),
				Object:    quad.String("Bob Cool"),
				Label:     quad.IRI(""),
			},
		},
		Config: DocumentConfig{
			Name: "people",
			Match: map[quad.IRI][]quad.Value{
				quad.IRI("rdf:type"): {
					quad.IRI("Person"),
				},
			},
			Properties: []PropertyConfig{
				{
					Name: quad.IRI("name"),
				},
			},
		},
		Expected: []document{
			{
				id: "alice",
				data: map[string]interface{}{
					"_type": "people",
					"name":  []interface{}{"Alice Liddell"},
				},
			},
		},
	},
	{
		Name: "With Entities",
		Data: []quad.Quad{
			quad.Quad{
				Subject:   quad.IRI("alice"),
				Predicate: quad.IRI("name"),
				Object:    quad.String("Alice Liddell"),
				Label:     quad.IRI(""),
			},
			quad.Quad{
				Subject:   quad.IRI("alice"),
				Predicate: quad.IRI("rdf:type"),
				Object:    quad.IRI("Person"),
				Label:     quad.IRI(""),
			},
			quad.Quad{
				Subject:   quad.IRI("bob"),
				Predicate: quad.IRI("name"),
				Object:    quad.String("Bob Cool"),
				Label:     quad.IRI(""),
			},
		},
		Config: DocumentConfig{
			Name: "people",
			Properties: []PropertyConfig{
				{
					Name: quad.IRI("name"),
				},
			},
		},
		Expected: []document{
			{
				id: "alice",
				data: map[string]interface{}{
					"_type": "people",
					"name":  []interface{}{"Alice Liddell"},
				},
			},
		},
		Entities: []quad.Value{
			quad.IRI("alice"),
		},
	},
}

func TestGetDocuments(t *testing.T) {
	for _, c := range testCases {
		t.Run(c.Name, func(t *testing.T) {
			qs := NewTestQuadStore(c.Data...)
			documents, err := getDocuments(context.TODO(), qs, c.Config, c.Entities)
			require.NoError(t, err)
			require.Equal(t, c.Expected, documents)
		})
	}
}

func TestSearch(t *testing.T) {
	data := []quad.Quad{
		quad.Quad{
			Subject:   quad.IRI("alice"),
			Predicate: quad.IRI("name"),
			Object:    quad.String("Alice Liddell"),
			Label:     quad.IRI(""),
		},
	}
	configs := []DocumentConfig{
		{
			Name: "people",
			Properties: []PropertyConfig{
				{
					Name: quad.IRI("name"),
				},
			},
		},
	}
	qs := NewTestQuadStore(data...)
	mapping := NewIndexMapping(configs)
	index, err := bleve.NewMemOnly(mapping)
	require.NoError(t, err)
	err = InitIndex(context.TODO(), index, qs, configs)
	require.NoError(t, err)

	results, err := Search(index, "Alice Liddell")
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, quad.IRI("alice"), results[0])

	results, err = Search(index, "Liddell")
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, quad.IRI("alice"), results[0])

	results, err = Search(index, "Alice")
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, quad.IRI("alice"), results[0])
}

var _ graph.QuadStore = &TestQuadStore{}

type ValueToRef map[quad.Value]graph.Ref
type RefToValue map[graph.Ref]quad.Value
type RefToQuad map[graph.Ref]quad.Quad

type TestQuadStore struct {
	valueToRef ValueToRef
	refToValue RefToValue
	refToQuad  RefToQuad
	quads      []quad.Quad
}

func NewTestQuadStore(data ...quad.Quad) TestQuadStore {
	return TestQuadStore{
		valueToRef: make(ValueToRef),
		refToValue: make(RefToValue),
		refToQuad:  make(RefToQuad),
		quads:      data,
	}
}

func (qs TestQuadStore) Quad(ref graph.Ref) quad.Quad {
	return qs.refToQuad[ref]
}
func (qs TestQuadStore) QuadIterator(dir quad.Direction, ref graph.Ref) iterator.Shape {
	var r []graph.Ref
	for _, q := range qs.quads {
		if dir == quad.Subject && qs.ValueOf(q.Subject) == ref ||
			dir == quad.Predicate && qs.ValueOf(q.Predicate) == ref ||
			dir == quad.Object && qs.ValueOf(q.Object) == ref ||
			dir == quad.Label && qs.ValueOf(q.Label) == ref {
			hash := refs.QuadHash{
				Subject:   refs.HashOf(q.Subject),
				Predicate: refs.HashOf(q.Predicate),
				Object:    refs.HashOf(q.Object),
				Label:     refs.HashOf(q.Label),
			}
			qs.refToQuad[hash] = q
			r = append(r, hash)
		}
	}
	return iterator.NewFixed(r...)
}
func (TestQuadStore) QuadIteratorSize(ctx context.Context, d quad.Direction, v graph.Ref) (refs.Size, error) {
	panic("QuadIteratorSize is not implemented for TestQuadStore")
}
func (qs TestQuadStore) QuadDirection(id graph.Ref, dir quad.Direction) graph.Ref {
	return qs.ValueOf(qs.Quad(id).Get(dir))
}
func (TestQuadStore) Stats(ctx context.Context, exact bool) (graph.Stats, error) {
	panic("Stats is not implemented for TestQuadStore")
}
func (qs TestQuadStore) ValueOf(value quad.Value) graph.Ref {
	ref, ok := qs.valueToRef[value]
	if ok {
		return ref
	}
	ref = refs.HashOf(value)
	qs.valueToRef[value] = ref
	qs.refToValue[ref] = value
	return ref
}
func (qs TestQuadStore) NameOf(ref graph.Ref) quad.Value {
	return qs.refToValue[ref]
}
func (TestQuadStore) ApplyDeltas(in []graph.Delta, opts graph.IgnoreOpts) error {
	panic("ApplyDeltas is not implemented for TestQuadStore")
}
func (TestQuadStore) NewQuadWriter() (quad.WriteCloser, error) {
	panic("NewQuadWriter is not implemented for TestQuadStore")
}
func (qs TestQuadStore) NodesAllIterator() iterator.Shape {
	var r []graph.Ref
	for _, q := range qs.quads {
		r = append(r, qs.ValueOf(q.Subject), qs.ValueOf(q.Object))
	}
	return iterator.NewFixed(r...)
}
func (TestQuadStore) QuadsAllIterator() iterator.Shape {
	panic("QuadsAllIterator is not implemented for TestQuadStore")
}
func (TestQuadStore) Close() error {
	panic("Close is not implemented for TestQuadStore")
}
