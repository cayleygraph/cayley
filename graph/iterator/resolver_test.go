package iterator_test

import (
	"context"
	"testing"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/graphmock"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/quad"
)

func TestResolverIteratorIterate(t *testing.T) {
	var ctx context.Context
	nodes := []quad.Value{
		quad.String("1"),
		quad.String("2"),
		quad.String("3"),
		quad.String("4"),
		quad.String("5"),
		quad.String("3"), // Assert iterator can handle duplicate values
	}
	data := make([]quad.Quad, 0, len(nodes))
	for _, node := range nodes {
		data = append(data, quad.Make(quad.String("0"), "has", node, nil))
	}
	qs := &graphmock.Store{
		Data: data,
	}
	expected := make(map[quad.Value]graph.Ref)
	for _, node := range nodes {
		expected[node] = qs.ValueOf(node)
	}
	it := iterator.NewResolver(qs, nodes...)
	for _, node := range nodes {
		if it.Next(ctx) != true {
			t.Fatal("unexpected end of iterator")
		}
		if err := it.Err(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if value := it.Result(); value != expected[node] {
			t.Fatalf("unexpected quad value: expected %v, got %v", expected[node], value)
		}
	}
	if it.Next(ctx) != false {
		t.Fatal("expected end of iterator")
	}
	if it.Result() != nil {
		t.Fatal("expected nil result")
	}
}

func TestResolverIteratorNotFoundError(t *testing.T) {
	var ctx context.Context
	nodes := []quad.Value{
		quad.String("1"),
		quad.String("2"),
		quad.String("3"),
		quad.String("4"),
		quad.String("5"),
	}
	data := make([]quad.Quad, 0)
	skip := 3
	for i, node := range nodes {
		// Simulate a missing subject
		if i == skip {
			continue
		}
		data = append(data, quad.Make(quad.String("0"), "has", node, nil))
	}
	qs := &graphmock.Store{
		Data: data,
	}
	count := 0
	it := iterator.NewResolver(qs, nodes...)
	for it.Next(ctx) {
		count++
	}
	if count != 0 {
		t.Fatal("expected end of iterator")
	}
	if it.Err() == nil {
		t.Fatal("expected not found error")
	}
	if it.Result() != nil {
		t.Fatal("expected nil result")
	}
}

func TestResolverIteratorContains(t *testing.T) {
	tests := []struct {
		name     string
		nodes    []quad.Value
		subject  quad.Value
		contains bool
	}{
		{
			"contains",
			[]quad.Value{
				quad.String("1"),
				quad.String("2"),
				quad.String("3"),
			},
			quad.String("2"),
			true,
		},
		{
			"not contains",
			[]quad.Value{
				quad.String("1"),
				quad.String("3"),
			},
			quad.String("2"),
			false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var ctx context.Context
			data := make([]quad.Quad, 0, len(test.nodes))
			for _, node := range test.nodes {
				data = append(data, quad.Make(quad.String("0"), "has", node, nil))
			}
			qs := &graphmock.Store{
				Data: data,
			}
			it := iterator.NewResolver(qs, test.nodes...)
			if it.Contains(ctx, graph.PreFetched(test.subject)) != test.contains {
				t.Fatal("unexpected result")
			}
		})
	}
}
