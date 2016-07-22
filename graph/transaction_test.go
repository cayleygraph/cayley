package graph

import (
	"testing"

	"github.com/cayleygraph/cayley/quad"
)

func TestTransaction(t *testing.T) {
	var tx *Transaction

	// simples adds / removes
	tx = NewTransaction()

	tx.AddQuad(quad.MakeRaw("E", "follows", "F", ""))
	tx.AddQuad(quad.MakeRaw("F", "follows", "G", ""))
	tx.RemoveQuad(quad.MakeRaw("A", "follows", "Z", ""))
	if len(tx.Deltas) != 3 {
		t.Errorf("Expected 3 Deltas, have %d delta(s)", len(tx.Deltas))
	}

	// add, remove -> nothing
	tx = NewTransaction()
	tx.AddQuad(quad.MakeRaw("E", "follows", "G", ""))
	tx.RemoveQuad(quad.MakeRaw("E", "follows", "G", ""))
	if len(tx.Deltas) != 0 {
		t.Errorf("Expected [add, remove]->[], have %d Deltas", len(tx.Deltas))
	}

	// remove, add -> nothing
	tx = NewTransaction()
	tx.RemoveQuad(quad.MakeRaw("E", "follows", "G", ""))
	tx.AddQuad(quad.MakeRaw("E", "follows", "G", ""))
	if len(tx.Deltas) != 0 {
		t.Errorf("Expected [add, remove]->[], have %d delta(s)", len(tx.Deltas))
	}

	// add x2 -> add x1
	tx = NewTransaction()
	tx.AddQuad(quad.MakeRaw("E", "follows", "G", ""))
	tx.AddQuad(quad.MakeRaw("E", "follows", "G", ""))
	if len(tx.Deltas) != 1 {
		t.Errorf("Expected [add, add]->[add], have %d delta(s)", len(tx.Deltas))
	}

	// remove x2 -> remove x1
	tx = NewTransaction()
	tx.RemoveQuad(quad.MakeRaw("E", "follows", "G", ""))
	tx.RemoveQuad(quad.MakeRaw("E", "follows", "G", ""))
	if len(tx.Deltas) != 1 {
		t.Errorf("Expected [remove, remove]->[remove], have %d delta(s)", len(tx.Deltas))
	}

	// add, remove x2 -> remove x1
	tx = NewTransaction()
	tx.AddQuad(quad.MakeRaw("E", "follows", "G", ""))
	tx.RemoveQuad(quad.MakeRaw("E", "follows", "G", ""))
	tx.RemoveQuad(quad.MakeRaw("E", "follows", "G", ""))
	if len(tx.Deltas) != 1 {
		t.Errorf("Expected [add, remove, remove]->[remove], have %d delta(s)", len(tx.Deltas))
	}
}
