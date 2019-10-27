package linkedql

import (
	"fmt"
	"testing"
)

func TestGenerateSchema(t *testing.T) {
	fmt.Println(string(serializeSchema()))
}
