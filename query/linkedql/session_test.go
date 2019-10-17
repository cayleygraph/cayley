package linkedql

import (
	"context"
	"testing"

	"github.com/cayleygraph/cayley/graph/memstore"
	"github.com/cayleygraph/cayley/query"
)

func TestNodeQuery(t *testing.T) {
	q := `
{
	"@type": "linkedql:NewVertex"
}
	`
	store := memstore.New()
	session := NewSession(store)
	iterator, err := session.Execute(context.TODO(), q, query.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if iterator.Result() != nil {
		t.Error("Returned result for an empty store")
	}
}
