package linkedql

import (
	"encoding/json"
	"testing"
)

func TestGenerateSchema(t *testing.T) {
	schema := GenerateSchema()
	_, err := json.Marshal(schema)
	if err != nil {
		t.Fatal(err)
	}

}
