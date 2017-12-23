package quad

import (
	"encoding/hex"
	"testing"
)

var hashCases = []struct {
	val  Value
	hash string
}{
	{String(`abc`), "b87f4bf9b7b07f594430548b653b4998e4b40402"},
	{Raw(`"abc"`), "b87f4bf9b7b07f594430548b653b4998e4b40402"},
	{BNode(`abc`), "3603f98d3203a037ffa6b8780b97ef8bc964fd94"},
	{Raw(`_:abc`), "3603f98d3203a037ffa6b8780b97ef8bc964fd94"},
	{IRI(`abc`), "b301db80a006fb0c667f3feffbf8c68a7b38fe7e"},
	{Raw(`<abc>`), "b301db80a006fb0c667f3feffbf8c68a7b38fe7e"},
}

func TestHashOf(t *testing.T) {
	for i, c := range hashCases {
		h := hex.EncodeToString(HashOf(c.val))
		if h != c.hash {
			t.Errorf("unexpected hash for case %d: %v vs %v", i+1, h, c.hash)
		}
	}
}
