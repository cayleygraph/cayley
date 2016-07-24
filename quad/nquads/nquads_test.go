// Copyright 2014 The Cayley Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nquads

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/cayleygraph/cayley/quad"
)

var testNQuadsRaw = []struct {
	message string
	input   string
	expect  quad.Quad
	err     error
}{
	// Tests taken from http://www.w3.org/TR/n-quads/ and http://www.w3.org/TR/n-triples/.

	// _:100000 </film/performance/actor> </en/larry_fine_1902> . # example from 30movies
	{
		message: "parse triple with commment",
		input:   `_:100000 </film/performance/actor> </en/larry_fine_1902> . # example from 30movies`,
		expect: quad.Quad{
			Subject:   quad.Raw("_:100000"),
			Predicate: quad.Raw("</film/performance/actor>"),
			Object:    quad.Raw("</en/larry_fine_1902>"),
			Label:     nil,
		},
		err: nil,
	},
	// _:10011 </film/performance/character> "Tomás de Torquemada" . # example from 30movies with unicode
	{
		message: "parse triple with commment",
		input:   `_:10011 </film/performance/character> "Tomás de Torquemada" . # example from 30movies with unicode`,
		expect: quad.Quad{
			Subject:   quad.Raw("_:10011"),
			Predicate: quad.Raw("</film/performance/character>"),
			Object:    quad.Raw(`"Tomás de Torquemada"`),
			Label:     nil,
		},
		err: nil,
	},

	// N-Triples example 1.
	{
		message: "parse triple with commment",
		input:   `<http://one.example/subject1> <http://one.example/predicate1> <http://one.example/object1> . # comments here`,
		expect: quad.Quad{
			Subject:   quad.Raw("<http://one.example/subject1>"),
			Predicate: quad.Raw("<http://one.example/predicate1>"),
			Object:    quad.Raw("<http://one.example/object1>"),
			Label:     nil,
		},
		err: nil,
	},
	{
		message: "parse triple with blank subject node, literal object and no comment (1)",
		input:   `_:subject1 <http://an.example/predicate1> "object1" .`,
		expect: quad.Quad{
			Subject:   quad.Raw("_:subject1"),
			Predicate: quad.Raw("<http://an.example/predicate1>"),
			Object:    quad.Raw(`"object1"`),
			Label:     nil,
		},
		err: nil,
	},
	{
		message: "parse triple with blank subject node, literal object and no comment (2)",
		input:   `_:subject2 <http://an.example/predicate2> "object2" .`,
		expect: quad.Quad{
			Subject:   quad.Raw("_:subject2"),
			Predicate: quad.Raw("<http://an.example/predicate2>"),
			Object:    quad.Raw(`"object2"`),
			Label:     nil,
		},
		err: nil,
	},

	// N-Triples example 2.
	{
		message: "parse triple with three IRIREFs",
		input:   `<http://example.org/#spiderman> <http://www.perceive.net/schemas/relationship/enemyOf> <http://example.org/#green-goblin> .`,
		expect: quad.Quad{
			Subject:   quad.Raw("<http://example.org/#spiderman>"),
			Predicate: quad.Raw("<http://www.perceive.net/schemas/relationship/enemyOf>"),
			Object:    quad.Raw("<http://example.org/#green-goblin>"),
			Label:     nil,
		},
		err: nil,
	},

	// N-Triples example 3.
	{
		message: "parse triple with blank node labelled subject and object and IRIREF predicate (1)",
		input:   `_:alice <http://xmlns.com/foaf/0.1/knows> _:bob .`,
		expect: quad.Quad{
			Subject:   quad.Raw("_:alice"),
			Predicate: quad.Raw("<http://xmlns.com/foaf/0.1/knows>"),
			Object:    quad.Raw("_:bob"),
			Label:     nil,
		},
		err: nil,
	},
	{
		message: "parse triple with blank node labelled subject and object and IRIREF predicate (2)",
		input:   `_:bob <http://xmlns.com/foaf/0.1/knows> _:alice .`,
		expect: quad.Quad{
			Subject:   quad.Raw("_:bob"),
			Predicate: quad.Raw("<http://xmlns.com/foaf/0.1/knows>"),
			Object:    quad.Raw("_:alice"),
			Label:     nil,
		},
		err: nil,
	},

	// N-Quads example 1.
	{
		message: "parse quad with commment",
		input:   `<http://one.example/subject1> <http://one.example/predicate1> <http://one.example/object1> <http://example.org/graph3> . # comments here`,
		expect: quad.Quad{
			Subject:   quad.Raw("<http://one.example/subject1>"),
			Predicate: quad.Raw("<http://one.example/predicate1>"),
			Object:    quad.Raw("<http://one.example/object1>"),
			Label:     quad.Raw("<http://example.org/graph3>"),
		},
		err: nil,
	},
	{
		message: "parse quad with blank subject node, literal object, IRIREF predicate and label, and no comment (1)",
		input:   `_:subject1 <http://an.example/predicate1> "object1" <http://example.org/graph1> .`,
		expect: quad.Quad{
			Subject:   quad.Raw("_:subject1"),
			Predicate: quad.Raw("<http://an.example/predicate1>"),
			Object:    quad.Raw(`"object1"`),
			Label:     quad.Raw("<http://example.org/graph1>"),
		},
		err: nil,
	},
	{
		message: "parse quad with blank subject node, literal object, IRIREF predicate and label, and no comment (2)",
		input:   `_:subject2 <http://an.example/predicate2> "object2" <http://example.org/graph5> .`,
		expect: quad.Quad{
			Subject:   quad.Raw("_:subject2"),
			Predicate: quad.Raw("<http://an.example/predicate2>"),
			Object:    quad.Raw(`"object2"`),
			Label:     quad.Raw("<http://example.org/graph5>"),
		},
		err: nil,
	},

	// N-Quads example 2.
	{
		message: "parse quad with all IRIREF parts",
		input:   `<http://example.org/#spiderman> <http://www.perceive.net/schemas/relationship/enemyOf> <http://example.org/#green-goblin> <http://example.org/graphs/spiderman> .`,
		expect: quad.Quad{
			Subject:   quad.Raw("<http://example.org/#spiderman>"),
			Predicate: quad.Raw("<http://www.perceive.net/schemas/relationship/enemyOf>"),
			Object:    quad.Raw("<http://example.org/#green-goblin>"),
			Label:     quad.Raw("<http://example.org/graphs/spiderman>"),
		},
		err: nil,
	},

	// N-Quads example 3.
	{
		message: "parse quad with blank node labelled subject and object and IRIREF predicate and label (1)",
		input:   `_:alice <http://xmlns.com/foaf/0.1/knows> _:bob <http://example.org/graphs/john> .`,
		expect: quad.Quad{
			Subject:   quad.Raw("_:alice"),
			Predicate: quad.Raw("<http://xmlns.com/foaf/0.1/knows>"),
			Object:    quad.Raw("_:bob"),
			Label:     quad.Raw("<http://example.org/graphs/john>"),
		},
		err: nil,
	},
	{
		message: "parse quad with blank node labelled subject and object and IRIREF predicate and label (2)",
		input:   `_:bob <http://xmlns.com/foaf/0.1/knows> _:alice <http://example.org/graphs/james> .`,
		expect: quad.Quad{
			Subject:   quad.Raw("_:bob"),
			Predicate: quad.Raw("<http://xmlns.com/foaf/0.1/knows>"),
			Object:    quad.Raw("_:alice"),
			Label:     quad.Raw("<http://example.org/graphs/james>"),
		},
		err: nil,
	},

	// N-Triples tests.
	{
		message: "parse triple with all IRIREF parts",
		input:   `<http://example.org/bob#me> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> .`,
		expect: quad.Quad{
			Subject:   quad.Raw("<http://example.org/bob#me>"),
			Predicate: quad.Raw("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"),
			Object:    quad.Raw("<http://xmlns.com/foaf/0.1/Person>"),
			Label:     nil,
		},
		err: nil,
	},
	{
		message: "parse triple with all IRIREF parts",
		input:   `<http://example.org/bob#me> <http://xmlns.com/foaf/0.1/knows> <http://example.org/alice#me> .`,
		expect: quad.Quad{
			Subject:   quad.Raw("<http://example.org/bob#me>"),
			Predicate: quad.Raw("<http://xmlns.com/foaf/0.1/knows>"),
			Object:    quad.Raw("<http://example.org/alice#me>"),
			Label:     nil,
		},
		err: nil,
	},
	{
		message: "parse triple with IRIREF schema on literal object",
		input:   `<http://example.org/bob#me> <http://schema.org/birthDate> "1990-07-04"^^<http://www.w3.org/2001/XMLSchema#date> .`,
		expect: quad.Quad{
			Subject:   quad.Raw("<http://example.org/bob#me>"),
			Predicate: quad.Raw("<http://schema.org/birthDate>"),
			Object:    quad.Raw(`"1990-07-04"^^<http://www.w3.org/2001/XMLSchema#date>`),
			Label:     nil,
		},
		err: nil,
	},
	{
		message: "parse commented IRIREF in triple",
		input:   `<http://example.org/bob#me> <http://xmlns.com/foaf/0.1/topic_interest> <http://www.wikidata.org/entity/Q12418> .`,
		expect: quad.Quad{
			Subject:   quad.Raw("<http://example.org/bob#me>"),
			Predicate: quad.Raw("<http://xmlns.com/foaf/0.1/topic_interest>"),
			Object:    quad.Raw("<http://www.wikidata.org/entity/Q12418>"),
			Label:     nil,
		},
		err: nil,
	},
	{
		message: "parse triple with literal subject",
		input:   `<http://www.wikidata.org/entity/Q12418> <http://purl.org/dc/terms/title> "Mona Lisa" .`,
		expect: quad.Quad{
			Subject:   quad.Raw("<http://www.wikidata.org/entity/Q12418>"),
			Predicate: quad.Raw("<http://purl.org/dc/terms/title>"),
			Object:    quad.Raw(`"Mona Lisa"`),
			Label:     nil,
		},
		err: nil,
	},
	{
		message: "parse triple with all IRIREF parts (1)",
		input:   `<http://www.wikidata.org/entity/Q12418> <http://purl.org/dc/terms/creator> <http://dbpedia.org/resource/Leonardo_da_Vinci> .`,
		expect: quad.Quad{
			Subject:   quad.Raw("<http://www.wikidata.org/entity/Q12418>"),
			Predicate: quad.Raw("<http://purl.org/dc/terms/creator>"),
			Object:    quad.Raw("<http://dbpedia.org/resource/Leonardo_da_Vinci>"),
			Label:     nil,
		},
		err: nil,
	},
	{
		message: "parse triple with all IRIREF parts (2)",
		input:   `<http://data.europeana.eu/item/04802/243FA8618938F4117025F17A8B813C5F9AA4D619> <http://purl.org/dc/terms/subject> <http://www.wikidata.org/entity/Q12418> .`,
		expect: quad.Quad{
			Subject:   quad.Raw("<http://data.europeana.eu/item/04802/243FA8618938F4117025F17A8B813C5F9AA4D619>"),
			Predicate: quad.Raw("<http://purl.org/dc/terms/subject>"),
			Object:    quad.Raw("<http://www.wikidata.org/entity/Q12418>"),
			Label:     nil,
		},
		err: nil,
	},

	// N-Quads tests.
	{
		message: "parse commented IRIREF in quad (1)",
		input:   `<http://example.org/bob#me> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> <http://example.org/bob> .`,
		expect: quad.Quad{
			Subject:   quad.Raw("<http://example.org/bob#me>"),
			Predicate: quad.Raw("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"),
			Object:    quad.Raw("<http://xmlns.com/foaf/0.1/Person>"),
			Label:     quad.Raw("<http://example.org/bob>"),
		},
		err: nil,
	},
	{
		message: "parse quad with all IRIREF parts",
		input:   `<http://example.org/bob#me> <http://xmlns.com/foaf/0.1/knows> <http://example.org/alice#me> <http://example.org/bob> .`,
		expect: quad.Quad{
			Subject:   quad.Raw("<http://example.org/bob#me>"),
			Predicate: quad.Raw("<http://xmlns.com/foaf/0.1/knows>"),
			Object:    quad.Raw("<http://example.org/alice#me>"),
			Label:     quad.Raw("<http://example.org/bob>"),
		},
		err: nil,
	},
	{
		message: "parse quad with IRIREF schema on literal object",
		input:   `<http://example.org/bob#me> <http://schema.org/birthDate> "1990-07-04"^^<http://www.w3.org/2001/XMLSchema#date> <http://example.org/bob> .`,
		expect: quad.Quad{
			Subject:   quad.Raw("<http://example.org/bob#me>"),
			Predicate: quad.Raw("<http://schema.org/birthDate>"),
			Object:    quad.Raw(`"1990-07-04"^^<http://www.w3.org/2001/XMLSchema#date>`),
			Label:     quad.Raw("<http://example.org/bob>"),
		},
		err: nil,
	},
	{
		message: "parse commented IRIREF in quad (2)",
		input:   `<http://example.org/bob#me> <http://xmlns.com/foaf/0.1/topic_interest> <http://www.wikidata.org/entity/Q12418> <http://example.org/bob> .`,
		expect: quad.Quad{
			Subject:   quad.Raw("<http://example.org/bob#me>"),
			Predicate: quad.Raw("<http://xmlns.com/foaf/0.1/topic_interest>"),
			Object:    quad.Raw("<http://www.wikidata.org/entity/Q12418>"),
			Label:     quad.Raw("<http://example.org/bob>"),
		},
		err: nil,
	},
	{
		message: "parse literal object and colon qualified label in quad",
		input:   `<http://www.wikidata.org/entity/Q12418> <http://purl.org/dc/terms/title> "Mona Lisa" <https://www.wikidata.org/wiki/Special:EntityData/Q12418> .`,
		expect: quad.Quad{
			Subject:   quad.Raw("<http://www.wikidata.org/entity/Q12418>"),
			Predicate: quad.Raw("<http://purl.org/dc/terms/title>"),
			Object:    quad.Raw(`"Mona Lisa"`),
			Label:     quad.Raw("<https://www.wikidata.org/wiki/Special:EntityData/Q12418>"),
		},
		err: nil,
	},
	{
		message: "parse all IRIREF parts with colon qualified label in quad (1)",
		input:   `<http://www.wikidata.org/entity/Q12418> <http://purl.org/dc/terms/creator> <http://dbpedia.org/resource/Leonardo_da_Vinci> <https://www.wikidata.org/wiki/Special:EntityData/Q12418> .`,
		expect: quad.Quad{
			Subject:   quad.Raw("<http://www.wikidata.org/entity/Q12418>"),
			Predicate: quad.Raw("<http://purl.org/dc/terms/creator>"),
			Object:    quad.Raw("<http://dbpedia.org/resource/Leonardo_da_Vinci>"),
			Label:     quad.Raw("<https://www.wikidata.org/wiki/Special:EntityData/Q12418>"),
		},
		err: nil,
	},
	{
		message: "parse all IRIREF parts with colon qualified label in quad (2)",
		input:   `<http://data.europeana.eu/item/04802/243FA8618938F4117025F17A8B813C5F9AA4D619> <http://purl.org/dc/terms/subject> <http://www.wikidata.org/entity/Q12418> <https://www.wikidata.org/wiki/Special:EntityData/Q12418> .`,
		expect: quad.Quad{
			Subject:   quad.Raw("<http://data.europeana.eu/item/04802/243FA8618938F4117025F17A8B813C5F9AA4D619>"),
			Predicate: quad.Raw("<http://purl.org/dc/terms/subject>"),
			Object:    quad.Raw("<http://www.wikidata.org/entity/Q12418>"),
			Label:     quad.Raw("<https://www.wikidata.org/wiki/Special:EntityData/Q12418>"),
		},
		err: nil,
	},
	{
		message: "parse all IRIREF parts (quad section - 1)",
		input:   `<http://example.org/bob> <http://purl.org/dc/terms/publisher> <http://example.org> .`,
		expect: quad.Quad{
			Subject:   quad.Raw("<http://example.org/bob>"),
			Predicate: quad.Raw("<http://purl.org/dc/terms/publisher>"),
			Object:    quad.Raw("<http://example.org>"),
			Label:     nil,
		},
		err: nil,
	},
	{
		message: "parse all IRIREF parts (quad section - 2)",
		input:   `<http://example.org/bob> <http://purl.org/dc/terms/rights> <http://creativecommons.org/licenses/by/3.0/> .`,
		expect: quad.Quad{
			Subject:   quad.Raw("<http://example.org/bob>"),
			Predicate: quad.Raw("<http://purl.org/dc/terms/rights>"),
			Object:    quad.Raw("<http://creativecommons.org/licenses/by/3.0/>"),
			Label:     nil,
		},
		err: nil,
	},

	// Invalid input.
	{
		message: "parse empty",
		input:   ``,
		expect:  quad.Quad{},
		err:     quad.ErrIncomplete,
	},
	{
		message: "parse commented",
		input:   `# comment`,
		expect:  quad.Quad{},
		err:     fmt.Errorf("%v: unexpected rune '#' at 0", quad.ErrInvalid),
	},
	{
		message: "parse incomplete quad",
		input:   `<http://example.org/bob#me> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> .`,
		expect: quad.Quad{
			Subject:   quad.Raw("<http://example.org/bob#me>"),
			Predicate: quad.Raw("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"),
			Object:    nil,
			Label:     nil,
		},
		err: fmt.Errorf("%v: unexpected rune '.' at 78", quad.ErrInvalid),
	},
	{
		message: "parse incomplete quad",
		input:   `<http://example.org/bob#me> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> .`,
		expect: quad.Quad{
			Subject:   quad.Raw("<http://example.org/bob#me>"),
			Predicate: quad.Raw("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"),
			Object:    nil,
			Label:     nil,
		},
		err: fmt.Errorf("%v: unexpected rune '.' at 78", quad.ErrInvalid),
	},

	// Example quad from issue #140.
	{
		message: "parse incomplete quad",
		input:   "<ns:m.0y_chx>\t<ns:music.recording.lyrics_website..common.webpage.uri>\t<http://www.metrolyrics.com/?\"-lyrics-stephen-sondheim.html>.",
		expect: quad.Quad{
			Subject:   quad.Raw("<ns:m.0y_chx>"),
			Predicate: quad.Raw("<ns:music.recording.lyrics_website..common.webpage.uri>"),
			Object:    nil,
			Label:     nil,
		},
		err: fmt.Errorf("%v: unexpected rune '\"' at 99", quad.ErrInvalid),
	},
}

func TestParseRaw(t *testing.T) {
	for _, test := range testNQuadsRaw {
		got, err := ParseRaw(test.input)
		if err != test.err && (err != nil && err.Error() != test.err.Error()) {
			t.Errorf("Unexpected error when %s: got:%v expect:%v", test.message, err, test.err)
		}
		if !reflect.DeepEqual(got, test.expect) {
			t.Errorf("Failed to %s, %q, got:%#v expect:%#v", test.message, test.input, got, test.expect)
		}
	}
}

func TestRawDecoder(t *testing.T) {
	dec := NewRawReader(strings.NewReader(document))
	var n int
	for {
		q, err := dec.ReadQuad()
		if err != nil {
			if err != io.EOF {
				t.Fatalf("Failed to read documentRaw: %v", err)
			}
			break
		}
		if !q.IsValid() {
			t.Errorf("Unexpected quad, got:%v", q)
		}
		n++
	}
	if n != 20 {
		t.Errorf("Unexpected number of quads read, got:%d expect:20", n)
	}
}

func TestRDFWorkingGroupSuitRaw(t *testing.T) {
	// These tests erroneously pass because the parser does not
	// perform semantic testing on the URI in the IRIRef as required
	// by the specification. So, we skip them.
	skip := map[string]bool{
		// N-Triples.
		"nt-syntax-bad-uri-06.nt": true,
		"nt-syntax-bad-uri-07.nt": true,
		"nt-syntax-bad-uri-08.nt": true,
		"nt-syntax-bad-uri-09.nt": true,

		// N-Quads.
		"nq-syntax-bad-uri-01.nq": true,
		"nt-syntax-bad-uri-06.nq": true,
		"nt-syntax-bad-uri-07.nq": true,
		"nt-syntax-bad-uri-08.nq": true,
		"nt-syntax-bad-uri-09.nq": true,
	}

	for _, file := range []string{
		filepath.Join("..", "ntriple_tests.tar.gz"),
		filepath.Join("..", "nquad_tests.tar.gz"),
	} {
		suite, err := os.Open(file)
		if err != nil {
			t.Fatalf("Failed to open test suite in %q: %v", file, err)
		}
		defer suite.Close()

		r, err := gzip.NewReader(suite)
		if err != nil {
			t.Fatalf("Failed to uncompress test suite in %q: %v", file, err)
		}

		tr := tar.NewReader(r)
		for {
			h, err := tr.Next()
			if err != nil {
				if err == io.EOF {
					break
				}
				t.Fatalf("Unexpected error while reading suite archive: %v", err)
			}

			h.Name = filepath.Base(h.Name)
			if (filepath.Ext(h.Name) != ".nt" && filepath.Ext(h.Name) != ".nq") || skip[h.Name] {
				continue
			}

			isBad := strings.Contains(h.Name, "bad")

			dec := NewRawReader(tr)
			for {
				_, err := dec.ReadQuad()
				if err == io.EOF {
					break
				}
				got := err == nil
				if got == isBad {
					t.Errorf("Unexpected error return for test suite item %q, got: %v", h.Name, err)
				}
			}
		}
	}
}

func TestUnescapeRaw(t *testing.T) {
	for _, test := range escapeSequenceTests {
		got := unEscapeRaw([]rune(test.input), true)
		if got == nil || got.String() != test.expect {
			t.Errorf("Failed to properly unescape %q, got:%q expect:%q", test.input, got, test.expect)
		}
	}
}

func BenchmarkParserRaw(b *testing.B) {
	for n := 0; n < b.N; n++ {
		result, _ = ParseRaw("<http://example/s> <http://example/p> \"object of some real\\tlength\"@en . # comment")
	}
}
