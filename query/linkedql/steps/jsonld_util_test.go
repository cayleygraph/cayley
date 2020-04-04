package steps

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

var testCases = []struct {
	name     string
	source   interface{}
	target   interface{}
	expected error
}{
	{
		name:     "Single matching IDs",
		source:   map[string]interface{}{"@id": "a"},
		target:   map[string]interface{}{"@id": "a"},
		expected: nil,
	},
	{
		name:     "Single non matching IDs",
		source:   map[string]interface{}{"@id": "a"},
		target:   map[string]interface{}{"@id": "b"},
		expected: fmt.Errorf("Expected \"a\" but instead received \"b\""),
	},
	{
		name:     "Single matching properties",
		source:   map[string]interface{}{"http://example.com/name": "Alice"},
		target:   map[string]interface{}{"http://example.com/name": "Alice"},
		expected: nil,
	},
	{
		name:     "Single non matching properties",
		source:   map[string]interface{}{"http://example.com/name": "Alice"},
		target:   map[string]interface{}{"http://example.com/name": "Bob"},
		expected: fmt.Errorf("Expected \"Alice\" but instead received \"Bob\""),
	},
	{
		name:     "Single matching property with multiple values ordered",
		source:   map[string]interface{}{"http://example.com/name": []interface{}{"Alice", "Bob"}},
		target:   map[string]interface{}{"http://example.com/name": []interface{}{"Alice", "Bob"}},
		expected: nil,
	},
	{
		name:     "Single matching property with multiple values unordered",
		source:   map[string]interface{}{"http://example.com/name": []interface{}{"Alice", "Bob"}},
		target:   map[string]interface{}{"http://example.com/name": []interface{}{"Bob", "Alice"}},
		expected: nil,
	},
	{
		name:     "Single non matching property with multiple values",
		source:   map[string]interface{}{"http://example.com/name": []interface{}{"Alice", "Bob"}},
		target:   map[string]interface{}{"http://example.com/name": []interface{}{"Dan", "Alice"}},
		expected: fmt.Errorf("No matching values for the item \"Bob\" in []interface {}{\"Dan\", \"Alice\"}"),
	},
	{
		name:     "Single non matching property with multiple values non matching length",
		source:   map[string]interface{}{"http://example.com/name": []interface{}{"Alice", "Bob"}},
		target:   map[string]interface{}{"http://example.com/name": []interface{}{"Alice"}},
		expected: fmt.Errorf("Expected multiple values but instead received the single value: \"Alice\""),
	},
	{
		name: "Single matching nested",
		source: map[string]interface{}{
			"http://example.com/friend": map[string]interface{}{
				"@id": "alice",
			},
		},
		target: map[string]interface{}{
			"http://example.com/friend": map[string]interface{}{
				"@id": "alice",
			},
		},
		expected: nil,
	},
	{
		name: "Single non matching nested",
		source: map[string]interface{}{
			"http://example.com/friend": map[string]interface{}{
				"@id": "alice",
			},
		},
		target: map[string]interface{}{
			"http://example.com/friend": map[string]interface{}{
				"@id": "bob",
			},
		},
		expected: fmt.Errorf("Expected \"alice\" but instead received \"bob\""),
	},
	{
		name:     "Single matching properties with @value string",
		source:   map[string]interface{}{"http://example.com/name": map[string]interface{}{"@value": "Alice"}},
		target:   map[string]interface{}{"http://example.com/name": map[string]interface{}{"@value": "Alice"}},
		expected: nil,
	},
	{
		name:     "Single non matching properties with @value string",
		source:   map[string]interface{}{"http://example.com/name": map[string]interface{}{"@value": "Alice"}},
		target:   map[string]interface{}{"http://example.com/name": map[string]interface{}{"@value": "Bob"}},
		expected: fmt.Errorf("Expected \"Alice\" but instead received \"Bob\""),
	},
	{
		name:     "Single matching properties with @value string and string",
		source:   map[string]interface{}{"http://example.com/name": map[string]interface{}{"@value": "Alice"}},
		target:   map[string]interface{}{"http://example.com/name": "Alice"},
		expected: nil,
	},
	{
		name:     "Single matching properties with string and @value string",
		source:   map[string]interface{}{"http://example.com/name": "Alice"},
		target:   map[string]interface{}{"http://example.com/name": map[string]interface{}{"@value": "Alice"}},
		expected: nil,
	},
	{
		name:     "Single matching properties with @value string array string",
		source:   map[string]interface{}{"http://example.com/name": []interface{}{map[string]interface{}{"@value": "Alice"}}},
		target:   map[string]interface{}{"http://example.com/name": "Alice"},
		expected: nil,
	},
	{
		name:     "Single matching properties with string and @value string array",
		source:   map[string]interface{}{"http://example.com/name": "Alice"},
		target:   map[string]interface{}{"http://example.com/name": []interface{}{map[string]interface{}{"@value": "Alice"}}},
		expected: nil,
	},
}

func TestIsomorphic(t *testing.T) {
	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.expected, isomorphic(c.source, c.target))
		})
	}
}
