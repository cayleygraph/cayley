package client

import "testing"

import "fmt"

func TestGenerateClient(t *testing.T) {
	code, err := GenerateClient()
	if err != nil {
		panic(err)
	}
	fmt.Println(code)
}
