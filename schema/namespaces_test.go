package schema_test

import (
	"context"
	"reflect"
	"sort"
	"testing"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/memstore"
	"github.com/cayleygraph/cayley/schema"
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/voc"
)

func TestSaveNamespaces(t *testing.T) {
	sch := schema.NewConfig()
	save := []voc.Namespace{
		{Full: "http://example.org/", Prefix: "ex:"},
		{Full: "http://cayley.io/", Prefix: "c:"},
	}
	var ns voc.Namespaces
	for _, n := range save {
		ns.Register(n)
	}
	qs := memstore.New()
	err := sch.WriteNamespaces(qs, &ns)
	if err != nil {
		t.Fatal(err)
	}
	var ns2 voc.Namespaces
	err = sch.LoadNamespaces(context.TODO(), qs, &ns2)
	if err != nil {
		t.Fatal(err)
	}
	got := ns2.List()
	sort.Sort(voc.ByFullName(save))
	sort.Sort(voc.ByFullName(got))
	if !reflect.DeepEqual(save, got) {
		t.Fatalf("wrong namespaces returned: got: %v, expect: %v", got, save)
	}
	qr := graph.NewQuadStoreReader(qs)
	q, err := quad.ReadAll(qr)
	qr.Close()
	if err != nil {
		t.Fatal(err)
	}
	expect := []quad.Quad{
		quad.MakeIRI("http://cayley.io/", "cayley:prefix", "c:", ""),
		quad.MakeIRI("http://cayley.io/", "rdf:type", "cayley:namespace", ""),

		quad.MakeIRI("http://example.org/", "cayley:prefix", "ex:", ""),
		quad.MakeIRI("http://example.org/", "rdf:type", "cayley:namespace", ""),
	}
	sort.Sort(quad.ByQuadString(expect))
	sort.Sort(quad.ByQuadString(q))
	if !reflect.DeepEqual(expect, q) {
		t.Fatalf("wrong quads returned: got: %v, expect: %v", q, expect)
	}
}
