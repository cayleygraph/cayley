package schema_test

import (
	"encoding/json"
	"testing"

	"github.com/cayleygraph/cayley/query/linkedql/schema"
)

func TestMarshalSchema(t *testing.T) {
	out := schema.Generate()
	var o interface{}
	err := json.Unmarshal(out, &o)
	if err != nil {
		t.Fatal(err)
	}
}
