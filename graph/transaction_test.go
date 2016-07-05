package graph

import (
	"testing"

	"github.com/cayleygraph/cayley/quad"
)

func TestTransaction(t *testing.T) {
	var tx *Transaction

	// simples adds / removes
	tx = NewTransaction()

	tx.AddQuad(quad.Quad{Subject: "E", Predicate: "follows", Object: "F", Label: ""})
	tx.AddQuad(quad.Quad{Subject: "F", Predicate: "follows", Object: "G", Label: ""})
	tx.RemoveQuad(quad.Quad{Subject: "A", Predicate: "follows", Object: "Z", Label: ""})
	if len(tx.Deltas) != 3 {
		t.Errorf("Expected 3 Deltas, have %d delta(s)", len(tx.Deltas))
	}

	// add, remove -> nothing
	tx = NewTransaction()
	tx.AddQuad(quad.Quad{Subject: "E", Predicate: "follows", Object: "G", Label: ""})
	tx.RemoveQuad(quad.Quad{Subject: "E", Predicate: "follows", Object: "G", Label: ""})
	if len(tx.Deltas) != 0 {
		t.Errorf("Expected [add, remove]->[], have %d Deltas", len(tx.Deltas))
	}

	// remove, add -> nothing
	tx = NewTransaction()
	tx.RemoveQuad(quad.Quad{Subject: "E", Predicate: "follows", Object: "G", Label: ""})
	tx.AddQuad(quad.Quad{Subject: "E", Predicate: "follows", Object: "G", Label: ""})
	if len(tx.Deltas) != 0 {
		t.Errorf("Expected [add, remove]->[], have %d delta(s)", len(tx.Deltas))
	}

	// add x2 -> add x1
	tx = NewTransaction()
	tx.AddQuad(quad.Quad{Subject: "E", Predicate: "follows", Object: "G", Label: ""})
	tx.AddQuad(quad.Quad{Subject: "E", Predicate: "follows", Object: "G", Label: ""})
	if len(tx.Deltas) != 1 {
		t.Errorf("Expected [add, add]->[add], have %d delta(s)", len(tx.Deltas))
	}

	// remove x2 -> remove x1
	tx = NewTransaction()
	tx.RemoveQuad(quad.Quad{Subject: "E", Predicate: "follows", Object: "G", Label: ""})
	tx.RemoveQuad(quad.Quad{Subject: "E", Predicate: "follows", Object: "G", Label: ""})
	if len(tx.Deltas) != 1 {
		t.Errorf("Expected [remove, remove]->[remove], have %d delta(s)", len(tx.Deltas))
	}

	// add, remove x2 -> remove x1
	tx = NewTransaction()
	tx.AddQuad(quad.Quad{Subject: "E", Predicate: "follows", Object: "G", Label: ""})
	tx.RemoveQuad(quad.Quad{Subject: "E", Predicate: "follows", Object: "G", Label: ""})
	tx.RemoveQuad(quad.Quad{Subject: "E", Predicate: "follows", Object: "G", Label: ""})
	if len(tx.Deltas) != 1 {
		t.Errorf("Expected [add, remove, remove]->[remove], have %d delta(s)", len(tx.Deltas))
	}
}
