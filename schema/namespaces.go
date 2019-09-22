package schema

import (
	"context"
	"fmt"
	"reflect"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/voc"
)

type namespace struct {
	_      struct{} `quad:"@type > cayley:namespace"`
	Full   quad.IRI `quad:"@id"`
	Prefix quad.IRI `quad:"cayley:prefix"`
}

// WriteNamespaces will writes namespaces list into graph.
func (c *Config) WriteNamespaces(w quad.Writer, n *voc.Namespaces) error {
	rules, err := c.rulesFor(reflect.TypeOf(namespace{}))
	if err != nil {
		return fmt.Errorf("can't load rules: %v", err)
	}
	wr := c.newWriter(w)
	for _, ns := range n.List() {
		obj := namespace{
			Full:   quad.IRI(ns.Full),
			Prefix: quad.IRI(ns.Prefix),
		}
		rv := reflect.ValueOf(obj)
		if err = wr.writeValueAs(obj.Full, rv, "", rules); err != nil {
			return err
		}
	}
	return nil
}

// LoadNamespaces will load namespaces stored in graph to a specified list.
// If destination list is empty, global namespace registry will be used.
func (c *Config) LoadNamespaces(ctx context.Context, qs graph.QuadStore, dest *voc.Namespaces) error {
	var list []namespace
	if err := c.LoadTo(ctx, qs, &list); err != nil {
		return err
	}
	register := dest.Register
	if dest == nil {
		register = voc.Register
	}
	for _, ns := range list {
		register(voc.Namespace{
			Prefix: string(ns.Prefix),
			Full:   string(ns.Full),
		})
	}
	return nil
}
