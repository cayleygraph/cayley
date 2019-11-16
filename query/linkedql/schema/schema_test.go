package schema

import (
	"encoding/json"
	"testing"
)

func TestGenerate(t *testing.T) {
	schema := Generate()
	_, err := json.Marshal(schema)
	if err != nil {
		t.Fatal(err)
	}

}
