package jsonld

import (
	"bytes"
	"encoding/json"
	"github.com/cayleygraph/cayley/quad"
	"reflect"
	"sort"
	"strings"
	"testing"
)

var testReadCases = []struct {
	data   string
	expect []quad.Quad
}{
	{
		`{
  "@context": {
    "ex": "http://example.org/",
    "term1": {"@id": "ex:term1", "@type": "ex:datatype"},
    "term2": {"@id": "ex:term2", "@type": "@id"},
    "term3": {"@id": "ex:term3", "@language": "en"}
  },
  "@id": "ex:id1",
  "@type": ["ex:Type1", "ex:Type2"],
  "term1": "v1",
  "term2": "ex:id2",
  "term3": "v3"
}`,
		[]quad.Quad{
			{
				Subject:   quad.IRI(`http://example.org/id1`),
				Predicate: quad.IRI(`http://example.org/term1`),
				Object: quad.TypedString{
					Value: "v1", Type: "http://example.org/datatype",
				},
				Label: nil,
			},
			{
				Subject:   quad.IRI(`http://example.org/id1`),
				Predicate: quad.IRI(`http://example.org/term2`),
				Object:    quad.IRI(`http://example.org/id2`),
				Label:     nil,
			},
			{
				Subject:   quad.IRI(`http://example.org/id1`),
				Predicate: quad.IRI(`http://example.org/term3`),
				Object: quad.LangString{
					Value: "v3", Lang: "en",
				},
				Label: nil,
			},
			{
				Subject:   quad.IRI(`http://example.org/id1`),
				Predicate: quad.IRI(`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`),
				Object:    quad.IRI(`http://example.org/Type1`),
				Label:     nil,
			},
			{
				Subject:   quad.IRI(`http://example.org/id1`),
				Predicate: quad.IRI(`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`),
				Object:    quad.IRI(`http://example.org/Type2`),
				Label:     nil,
			},
		},
	},
}

type ByQuad []quad.Quad

func (a ByQuad) Len() int           { return len(a) }
func (a ByQuad) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByQuad) Less(i, j int) bool { return a[i].NQuad() < a[j].NQuad() }

func TestRead(t *testing.T) {
	for i, c := range testReadCases {
		r := NewReader(strings.NewReader(c.data))
		quads, err := quad.ReadAll(r)
		if err != nil {
			t.Errorf("case %d failed: %v", i, err)
		}
		sort.Sort(ByQuad(quads))
		sort.Sort(ByQuad(c.expect))
		if !reflect.DeepEqual(quads, c.expect) {
			t.Errorf("case %d failed: wrong quads returned:\n%v\n%v", i, quads, c.expect)
		}
		r.Close()
	}
}

var testWriteCases = []struct {
	data   []quad.Quad
	ctx    interface{}
	expect string
}{
	{
		[]quad.Quad{
			{
				Subject:   quad.IRI(`http://example.org/id1`),
				Predicate: quad.IRI(`http://example.org/term1`),
				Object: quad.TypedString{
					Value: "v1", Type: "http://example.org/datatype",
				},
				Label: nil,
			},
			{
				Subject:   quad.IRI(`http://example.org/id1`),
				Predicate: quad.IRI(`http://example.org/term2`),
				Object:    quad.IRI(`http://example.org/id2`),
				Label:     nil,
			},
			{
				Subject:   quad.IRI(`http://example.org/id1`),
				Predicate: quad.IRI(`http://example.org/term3`),
				Object: quad.LangString{
					Value: "v3", Lang: "en",
				},
				Label: nil,
			},
			{
				Subject:   quad.IRI(`http://example.org/id1`),
				Predicate: quad.IRI(`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`),
				Object:    quad.IRI(`http://example.org/Type1`),
				Label:     nil,
			},
			{
				Subject:   quad.IRI(`http://example.org/id1`),
				Predicate: quad.IRI(`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`),
				Object:    quad.IRI(`http://example.org/Type2`),
				Label:     nil,
			},
		},
		map[string]interface{}{
			"ex": "http://example.org/",
		},
		`{
  "@context": {
    "ex": "http://example.org/"
  },
  "@id": "ex:id1",
  "@type": [
    "ex:Type1",
    "ex:Type2"
  ],
  "ex:term1": {
    "@type": "ex:datatype",
    "@value": "v1"
  },
  "ex:term2": {
    "@id": "ex:id2"
  },
  "ex:term3": {
    "@language": "en",
    "@value": "v3"
  }
}
`,
	},
}

func TestWrite(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	for i, c := range testWriteCases {
		buf.Reset()
		w := NewWriter(buf)
		w.SetLdContext(c.ctx)
		_, err := quad.Copy(w, quad.NewReader(c.data))
		if err != nil {
			t.Errorf("case %d failed: %v", i, err)
		} else if err = w.Close(); err != nil {
			t.Errorf("case %d failed: %v", i, err)
		}
		data := make([]byte, buf.Len())
		copy(data, buf.Bytes())
		buf.Reset()
		json.Indent(buf, data, "", "  ")
		if buf.String() != c.expect {
			t.Errorf("case %d failed: wrong data returned:\n%v\n%v", i, buf.String(), c.expect)
		}
	}
}

var testRoundtripCases = []struct {
	data []quad.Quad
}{
	{
		[]quad.Quad{
			{
				Subject:   quad.IRI(`http://example.org/id1`),
				Predicate: quad.IRI(`http://example.org/term1`),
				Object: quad.TypedString{
					Value: "v1", Type: "http://example.org/datatype",
				},
				Label: nil,
			},
			{
				Subject:   quad.IRI(`http://example.org/id1`),
				Predicate: quad.IRI(`http://example.org/term2`),
				Object:    quad.IRI(`http://example.org/id2`),
				Label:     nil,
			},
			{
				Subject:   quad.IRI(`http://example.org/id1`),
				Predicate: quad.IRI(`http://example.org/term3`),
				Object: quad.LangString{
					Value: "v3", Lang: "en",
				},
				Label: nil,
			},
			{
				Subject:   quad.IRI(`http://example.org/id1`),
				Predicate: quad.IRI(`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`),
				Object:    quad.IRI(`http://example.org/Type1`),
				Label:     nil,
			},
			{
				Subject:   quad.IRI(`http://example.org/id1`),
				Predicate: quad.IRI(`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`),
				Object:    quad.IRI(`http://example.org/Type2`),
				Label:     nil,
			},
		},
	},
}

func TestRoundtrip(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	for i, c := range testRoundtripCases {
		buf.Reset()
		w := NewWriter(buf)
		_, err := quad.Copy(w, quad.NewReader(c.data))
		if err != nil {
			t.Errorf("case %d failed: %v", i, err)
		} else if err = w.Close(); err != nil {
			t.Errorf("case %d failed: %v", i, err)
		}
		arr, err := quad.ReadAll(NewReader(buf))
		sort.Sort(quad.ByQuadString(arr))
		sort.Sort(quad.ByQuadString(c.data))
		if err != nil {
			t.Errorf("case %d failed: %v", i, err)
		} else if !reflect.DeepEqual(arr, c.data) {
			t.Errorf("case %d failed: wrong data returned:\n%v\n%v", i, arr, c.data)
		}
	}
}
