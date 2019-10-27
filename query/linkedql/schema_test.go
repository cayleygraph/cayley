package linkedql

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestGenerateSchema(t *testing.T) {
	bytes, err := json.MarshalIndent(GenerateSchema(), "", "    ")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(bytes))
}
