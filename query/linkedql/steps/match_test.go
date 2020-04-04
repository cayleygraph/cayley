package steps

import (
	"testing"

	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/voc"
	"github.com/stretchr/testify/require"
)

var (
	ns      = "http://example.com/"
	alice   = quad.IRI(ns + "alice")
	likes   = quad.IRI(ns + "likes")
	name    = quad.IRI(ns + "name")
	bob     = quad.IRI(ns + "bob")
	address = quad.IRI(ns + "address")
	city    = quad.IRI(ns + "city")
	street  = quad.IRI(ns + "street")
	country = quad.IRI(ns + "country")
)

var patternTestCases = []struct {
	name     string
	pattern  map[string]interface{}
	expected *path.Path
}{
	{
		name:     "Empty Pattern",
		pattern:  map[string]interface{}{},
		expected: path.StartMorphism(),
	},
	{
		name: "Single Entity",
		pattern: map[string]interface{}{
			"@id": string(alice),
		},
		expected: path.StartMorphism().Is(alice),
	},
	{
		name: "Single Property Value",
		pattern: map[string]interface{}{
			string(likes): map[string]interface{}{"@id": string(bob)},
		},
		expected: path.StartMorphism().Has(likes, bob),
	},
	{
		name: "Nested Structure",
		pattern: map[string]interface{}{
			string(address): map[string]interface{}{
				string(street): "Lafayette",
			},
		},
		expected: path.
			StartMorphism().
			Out(address).
			Follow(
				path.StartMorphism().
					Has(street, quad.String("Lafayette")),
			).
			Back(""),
	},
	{
		name: "Two Level Nested Structure",
		pattern: map[string]interface{}{
			string(address): map[string]interface{}{
				string(country): map[string]interface{}{
					string(name): "The United States of America",
				},
			},
		},
		expected: path.
			StartMorphism().
			Out(address).
			Follow(
				path.StartMorphism().
					Out(country).
					Follow(
						path.StartMorphism().
							Has(name, quad.String("The United States of America")),
					).
					Back(""),
			).
			Back(""),
	},
}

func TestBuildPath(t *testing.T) {
	for _, c := range patternTestCases {
		t.Run(c.name, func(t *testing.T) {
			ns := voc.Namespaces{}
			quads, err := parsePattern(c.pattern, &ns)
			require.NoError(t, err)
			p := buildPatternPath(quads, &ns)
			expectedShape := c.expected.Shape()
			shape := p.Shape()
			// TODO(iddan): replace with stable comparison. Currently, it breaks
			// because order of properties is not guaranteed
			require.Equal(t, expectedShape, shape)
		})
	}
}
