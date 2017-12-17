package nosql

import (
	"github.com/stretchr/testify/require"
	"testing"
)

var filterMatch = []struct {
	f   FieldFilter
	d   Document
	exp bool
}{
	{
		f:   FieldFilter{Path: []string{"value", "str"}, Filter: GT, Value: String("f")},
		d:   Document{"value": Document{"str": String("bob")}},
		exp: false,
	},
	{
		f:   FieldFilter{Path: []string{"value", "str"}, Filter: Equal, Value: String("f")},
		d:   Document{"value": Document{"str": String("bob")}},
		exp: false,
	},
	{
		f:   FieldFilter{Path: []string{"value", "str"}, Filter: Equal, Value: String("bob")},
		d:   Document{"value": Document{"str": String("bob")}},
		exp: true,
	},
	{
		f:   FieldFilter{Path: []string{"value", "str"}, Filter: NotEqual, Value: String("bob")},
		d:   Document{"value1": Document{"str": String("bob")}},
		exp: true,
	},
}

func TestFilterMatch(t *testing.T) {
	for _, c := range filterMatch {
		require.Equal(t, c.exp, c.f.Matches(c.d))
	}
}
