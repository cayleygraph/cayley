package voc

import "testing"

var casesShortIRI = []struct {
	full  string
	short string
}{
	{full: "http://example.com/name", short: "ex:name"},
}

func TestShortIRI(t *testing.T) {
	RegisterPrefix("ex:", "http://example.com/")
	for _, c := range casesShortIRI {
		if f := FullIRI(c.full); f != c.full {
			t.Fatal("unexpected full iri:", f)
		}
		s := ShortIRI(c.full)
		if s != c.short {
			t.Fatal("unexpected short iri:", s)
		}
		if f := FullIRI(s); f != c.full {
			t.Fatal("unexpected full iri:", f)
		}
	}
}
