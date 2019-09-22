package schema_test

import (
	"sort"
	"time"

	"github.com/cayleygraph/cayley/schema"
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/voc"
	"github.com/cayleygraph/quad/voc/rdf"
)

func init() {
	voc.RegisterPrefix("ex:", "http://example.org/")
	schema.RegisterType(quad.IRI("ex:Coords"), Coords{})
}

type item struct {
	rdfType struct{} `quad:"rdf:type > some:item"`
	ID      quad.IRI `quad:"@id"`
	Name    string   `quad:"name"`
	Spec    string   `quad:"spec,optional"`
}

type item2 struct {
	Name string `quad:"name"`
	Spec string `quad:"spec"`
}

type timeItem struct {
	ID   quad.IRI  `quad:"@id"`
	Name string    `quad:"name"`
	TS   time.Time `quad:"ts"`
}

type treeItemByIRI []treeItem

func (o treeItemByIRI) Len() int           { return len(o) }
func (o treeItemByIRI) Less(i, j int) bool { return o[i].ID < o[j].ID }
func (o treeItemByIRI) Swap(i, j int)      { o[i], o[j] = o[j], o[i] }

type treeItem struct {
	ID       quad.IRI   `quad:"@id"`
	Name     string     `quad:"name"`
	Children []treeItem `quad:"child"`
}

func (t *treeItem) Sort() {
	for _, c := range t.Children {
		c.Sort()
	}
	sort.Sort(treeItemByIRI(t.Children))
}

type treeItemOptByIRI []treeItemOpt

func (o treeItemOptByIRI) Len() int           { return len(o) }
func (o treeItemOptByIRI) Less(i, j int) bool { return o[i].ID < o[j].ID }
func (o treeItemOptByIRI) Swap(i, j int)      { o[i], o[j] = o[j], o[i] }

type treeItemOpt struct {
	ID       quad.IRI      `quad:"@id"`
	Name     string        `quad:"name"`
	Children []treeItemOpt `quad:"child,optional"`
}

func (t *treeItemOpt) Sort() {
	for _, c := range t.Children {
		c.Sort()
	}
	sort.Sort(treeItemOptByIRI(t.Children))
}

type treeItemReqByIRI []treeItemReq

func (o treeItemReqByIRI) Len() int           { return len(o) }
func (o treeItemReqByIRI) Less(i, j int) bool { return o[i].ID < o[j].ID }
func (o treeItemReqByIRI) Swap(i, j int)      { o[i], o[j] = o[j], o[i] }

type treeItemReq struct {
	ID       quad.IRI      `quad:"@id"`
	Name     string        `quad:"name"`
	Children []treeItemReq `quad:"child,required"`
}

func (t *treeItemReq) Sort() {
	for _, c := range t.Children {
		c.Sort()
	}
	sort.Sort(treeItemReqByIRI(t.Children))
}

type genObject struct {
	ID   quad.IRI `quad:"@id"`
	Name string   `quad:"name"`
}

type MyString string

type genObjectTypedef struct {
	ID   quad.IRI `quad:"@id"`
	Name MyString `quad:"name"`
}

type subObject struct {
	genObject
	Num int `quad:"num"`
}

type subSubObject struct {
	subObject
	Num2 int `quad:"num2"`
}

type Coords struct {
	Lat float64 `json:"ex:lat"`
	Lng float64 `json:"ex:lng"`
}

type NodeLoop struct {
	ID   quad.IRI  `quad:"@id"`
	Name string    `quad:"name"`
	Next *NodeLoop `quad:"next"`
}

type NestedNode struct {
	ID   quad.IRI  `quad:"@id"`
	Name string    `quad:"name"`
	Prev genObject `quad:"prev,opt"`
	Next genObject `quad:"next,opt"`
}

type Alts struct {
	Alt []OptFields `quad:"alt"`
}

type OptFields struct {
	One string `quad:"one,optional"`
	Two string `quad:"two,optional"`
}

func iri(s string) quad.IRI { return quad.IRI(s) }

const typeIRI = quad.IRI(rdf.Type)

var treeQuads = []quad.Quad{
	{iri("n1"), iri("name"), quad.String("Node 1"), nil},
	{iri("n2"), iri("name"), quad.String("Node 2"), nil},
	{iri("n3"), iri("name"), quad.String("Node 3"), nil},
	{iri("n4"), iri("name"), quad.String("Node 4"), nil},
	{iri("n5"), iri("name"), quad.String("Node 5"), nil},

	{iri("n1"), iri("child"), iri("n2"), nil},
	{iri("n1"), iri("child"), iri("n3"), nil},

	{iri("n3"), iri("child"), iri("n4"), nil},
}
