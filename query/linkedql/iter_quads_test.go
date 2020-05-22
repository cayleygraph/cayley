package linkedql

import (
	"fmt"
	"testing"

	"github.com/cayleygraph/cayley/graph/memstore"
	"github.com/cayleygraph/quad"
	"github.com/stretchr/testify/require"
)

var (
	namespace       = "http://example.com/"
	alice           = quad.IRI(namespace + "alice")
	likes           = quad.IRI(namespace + "likes")
	blank           = quad.RandomBlankNode()
	name            = quad.IRI(namespace + "name")
	aliceName       = quad.String("Alice")
	aliceLikesBlank = quad.Quad{
		Subject:   alice,
		Predicate: likes,
		Object:    blank,
	}
	aliceNameAlice = quad.Quad{
		Subject:   alice,
		Predicate: name,
		Object:    aliceName,
	}
)

var testCases = []struct {
	name     string
	data     quad.Quad
	value    quad.Value
	expected quad.Value
	err      error
}{
	{
		name:     "Success for IRI",
		data:     aliceLikesBlank,
		value:    aliceLikesBlank.Subject,
		expected: alice,
		err:      nil,
	},
	{
		name:     "Success for Blank Node",
		data:     aliceLikesBlank,
		value:    aliceLikesBlank.Object,
		expected: blank,
		err:      nil,
	},
	{
		name:     "Failure for String",
		data:     aliceNameAlice,
		value:    aliceNameAlice.Object,
		expected: nil,
		err:      fmt.Errorf("Expected subject to be an entity identifier but instead received: %v", aliceName),
	},
}

func TestToSubject(t *testing.T) {
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			store := memstore.New(testCase.data)
			r := store.ValueOf(testCase.value)
			s, err := toSubject(store, r)
			if testCase.err == nil {
				require.NoError(t, err)
				require.Equal(t, testCase.expected, s)
			} else {
				require.Equal(t, testCase.err, err)
			}
		})
	}
}
