package linkedql

import "github.com/cayleygraph/quad"

import "encoding/json"

// Values holds multiple values and can be constructed from single or multiple values
type Values []quad.Value

// UnmarshalJSON implements RawMessage
func (v *Values) UnmarshalJSON(data []byte) (err error) {
	var errors []error

	err = json.Unmarshal(data, &v)
	if err == nil {
		return
	}
	errors = append(errors, err)

	var value quad.Value
	err = json.Unmarshal(data, &value)
	if err == nil {
		values := append(*v, value)
		v = &values
		return
	}
	errors = append(errors, err)

	return formatMultiError(errors)
}
