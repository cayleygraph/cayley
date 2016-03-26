package path

import (
	"github.com/google/cayley/quad"
	"reflect"
	"testing"
)

var casesOptimize = []struct {
	message  string
	simplify bool
	path     PathObj
	expect   PathObj
}{
	{
		"unique fixed",
		false,
		Unique{Fixed{"c", "b", "a", "b", "c", "b", "b"}},
		Fixed{"a", "b", "c"},
	},
	{
		"hasA with all",
		false,
		HasA{Links: AllLinks{}, Dir: quad.Object},
		AllNodes{},
	},
	{
		"linksTo with all",
		false,
		LinksTo{Nodes: AllNodes{}, Dir: quad.Object},
		AllLinks{},
	},
	{
		"linksTo over hasA",
		false,
		HasA{
			Links: LinksTo{
				Nodes: Fixed{"bob"},
				Dir:   quad.Object,
			},
			Dir: quad.Object,
		},
		Fixed{"bob"},
	},
	{
		"intersect fixed",
		false,
		IntersectNodes{
			Fixed{"a", "c"},
			Fixed{"b", "a", "d"},
			Fixed{"a", "b"},
		},
		Fixed{"a"},
	},
	//	{
	//		"intersect fixed and not",
	//		false,
	//		IntersectNodes{
	//			Fixed{"a","c","d"},
	//			NotNodes{Fixed{"d"}},
	//			Fixed{"a","d"},
	//		},
	//		Fixed{"a"},
	//	},
	{
		"nested intersect nodes",
		false,
		IntersectNodes{
			IntersectNodes{
				Fixed{"bob", "fred"},
			},
			IntersectNodes{
				NotNodes{Fixed{"dani"}},
				Fixed{"bob", "john"},
			},
		},
		IntersectNodes{ // TODO: fixed-intersects-not optimization
			Fixed{"bob"},
			NotNodes{Fixed{"dani"}},
		},
	},
	{
		"intersect all and optional",
		false,
		IntersectNodes{
			AllNodes{},
			Optional{Fixed{"b", "a", "d"}},
		},
		IntersectNodes{
			Optional{Fixed{"b", "a", "d"}},
			AllNodes{},
		},
	},
	{
		"out with no labels",
		false,
		Out{
			From: AllNodes{},
			Via:  Fixed{"follows"},
		},
		nil,
	},
	{
		"out with all labels",
		false,
		Out{
			From:   AllNodes{},
			Via:    Fixed{"follows"},
			Labels: AllNodes{},
		},
		Out{
			From:   AllNodes{},
			Via:    Fixed{"follows"},
			Labels: AllNodes{},
		},
	},
	{
		"out with all labels (simplify)",
		true,
		Out{
			From:   AllNodes{},
			Via:    Fixed{"follows"},
			Labels: AllNodes{},
		},
		HasA{
			Links: LinksTo{
				Nodes: Fixed{"follows"},
				Dir:   quad.Predicate,
			},
			Dir: quad.Object,
		},
	},
}

func TestOptimize(t *testing.T) {
	for _, test := range casesOptimize {
		got := test.path
		if test.simplify {
			switch si := got.(type) {
			case NodesSimplifier:
				got = si.Simplify()
			case LinksSimplifier:
				got = si.Simplify()
			default:
				t.Errorf("Failed to optimize %s: not simplifiable", test.message)
			}
		}
		got, _ = Optimize(got)
		if !reflect.DeepEqual(got, test.expect) {
			t.Errorf("Failed to optimize %s, got: %#v expected: %#v", test.message, got, test.expect)
		}
	}
}
