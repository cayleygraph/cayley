package steps

import (
	"fmt"
)

type ldArray = []interface{}
type ldMap = map[string]interface{}

func unwrapValue(i interface{}) interface{} {
	m, ok := i.(ldMap)
	if ok && len(m) == 1 {
		v, ok := m["@value"]
		if ok {
			return v
		}
	}
	return i
}

func unwrapSingle(i interface{}) interface{} {
	a, ok := i.(ldArray)
	if ok && len(a) == 1 {
		return a[0]
	}
	return i
}

// isomorphic checks whether source and target JSON-LD structures are the same
// semantically. This function is not complete and is maintained for testing
// purposes. Hopefully in the future it can be proven sufficient for general
// purpose use.
func isomorphic(source interface{}, target interface{}) error {
	source = unwrapValue(unwrapSingle(source))
	target = unwrapValue(unwrapSingle(target))
	switch s := source.(type) {
	case string:
		t, ok := target.(string)
		if !ok {
			return fmt.Errorf("Expected %v to be a string but instead received %T", target, target)
		}
		if s != t {
			return fmt.Errorf("Expected \"%v\" but instead received \"%v\"", s, t)
		}
		return nil
	case ldArray:
		t, ok := target.(ldArray)
		if !ok {
			return fmt.Errorf("Expected multiple values but instead received the single value: %#v", target)
		}
		if len(s) != len(t) {
			return fmt.Errorf("Expected %#v and %#v to have the same length", s, t)
		}
	items:
		for _, i := range s {
			for _, tI := range t {
				if isomorphic(i, tI) == nil {
					continue items
				}
			}
			return fmt.Errorf("No matching values for the item %#v in %#v", i, t)
		}
		return nil
	case ldMap:
		t, ok := target.(ldMap)
		if !ok {
			return fmt.Errorf("Expected %#v to be a map but instead received %T", target, target)
		}
		for k, v := range s {
			tV, _ := t[k]
			err := isomorphic(v, tV)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
