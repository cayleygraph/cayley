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
	"time"

	"github.com/cayleygraph/cayley/quad"
)

var testNQuads = []struct {
	message string
	input   string
	expect  quad.Quad
	err     error
}{
	// Tests from original nquads.

	// NTriple tests.
	{
		message: "parse simple triples",
		input:   "this is valid .",
		expect: quad.Quad{
			Subject:   quad.Raw("this"),
			Predicate: quad.Raw("is"),
			Object:    quad.Raw("valid"),
			Label:     nil,
		},
	},
	{
		message: "parse quoted triples",
		input:   `this is "valid too" .`,
		expect: quad.Quad{
			Subject:   quad.Raw("this"),
			Predicate: quad.Raw("is"),
			Object:    quad.String("valid too"),
			Label:     nil,
		},
	},
	{
		message: "parse escaped quoted triples",
		input:   `he said "\"That's all folks\"" .`,
		expect: quad.Quad{
			Subject:   quad.Raw("he"),
			Predicate: quad.Raw("said"),
			Object:    quad.String(`"That's all folks"`),
			Label:     nil,
		},
	},
	{
		message: "parse an example real triple",
		input:   `":/guid/9202a8c04000641f80000000010c843c" "name" "George Morris" .`,
		expect: quad.Quad{
			Subject:   quad.String(":/guid/9202a8c04000641f80000000010c843c"),
			Predicate: quad.String("name"),
			Object:    quad.String("George Morris"),
			Label:     nil,
		},
	},
	{
		message: "parse a pathologically spaced triple",
		input:   "foo is \"\\tA big tough\\r\\nDeal\\\\\" .",
		expect: quad.Quad{
			Subject:   quad.Raw("foo"),
			Predicate: quad.Raw("is"),
			Object:    quad.String("\tA big tough\r\nDeal\\"),
			Label:     nil,
		},
	},

	// NQuad tests.
	{
		message: "parse a simple quad",
		input:   "this is valid quad .",
		expect: quad.Quad{
			Subject:   quad.Raw("this"),
			Predicate: quad.Raw("is"),
			Object:    quad.Raw("valid"),
			Label:     quad.Raw("quad"),
		},
	},
	{
		message: "parse a quoted quad",
		input:   `this is valid "quad thing" .`,
		expect: quad.Quad{
			Subject:   quad.Raw("this"),
			Predicate: quad.Raw("is"),
			Object:    quad.Raw("valid"),
			Label:     quad.String("quad thing"),
		},
	},
	{
		message: "parse crazy escaped quads",
		input:   `"\"this" "\"is" "\"valid" "\"quad thing".`,
		expect: quad.Quad{
			Subject:   quad.String(`"this`),
			Predicate: quad.String(`"is`),
			Object:    quad.String(`"valid`),
			Label:     quad.String(`"quad thing`),
		},
	},

	// NTriple official tests.
	{
		message: "handle simple case with comments",
		input:   "<http://example/s> <http://example/p> <http://example/o> . # comment",
		expect: quad.Quad{
			Subject:   quad.IRI("http://example/s"),
			Predicate: quad.IRI("http://example/p"),
			Object:    quad.IRI("http://example/o"),
			Label:     nil,
		},
	},
	{
		message: "handle simple case with comments",
		input:   "<http://example/s> <http://example/p> _:o . # comment",
		expect: quad.Quad{
			Subject:   quad.IRI("http://example/s"),
			Predicate: quad.IRI("http://example/p"),
			Object:    quad.BNode("o"),
			Label:     nil,
		},
	},
	{
		message: "handle simple case with comments",
		input:   "<http://example/s> <http://example/p> \"o\" . # comment",
		expect: quad.Quad{
			Subject:   quad.IRI("http://example/s"),
			Predicate: quad.IRI("http://example/p"),
			Object:    quad.String("o"),
			Label:     nil,
		},
	},
	{
		message: "handle simple case with comments",
		input:   "<http://example/s> <http://example/p> \"o\"^^<http://example/dt> . # comment",
		expect: quad.Quad{
			Subject:   quad.IRI("http://example/s"),
			Predicate: quad.IRI("http://example/p"),
			Object:    quad.TypedString{Value: "o", Type: "http://example/dt"},
			Label:     nil,
		},
	},
	{
		message: "handle simple case with typed string",
		input:   `<http://example/s> <http://example/p> "\U000000b7\n\\\u00b7"^^<http://example/dt> . # comment`,
		expect: quad.Quad{
			Subject:   quad.IRI("http://example/s"),
			Predicate: quad.IRI("http://example/p"),
			Object:    quad.TypedString{Value: "·\n\\·", Type: "http://example/dt"},
			Label:     nil,
		},
	},
	{
		message: "handle simple case with comments",
		input:   "<http://example/s> <http://example/p> \"o\"@en . # comment",
		expect: quad.Quad{
			Subject:   quad.IRI("http://example/s"),
			Predicate: quad.IRI("http://example/p"),
			Object:    quad.LangString{Value: "o", Lang: "en"},
			Label:     nil},
	},
	{
		message: "handle simple case with lang string",
		input:   "<http://example/s> <http://example/p> \"Tomás de Torquemada\"@es . # comment",
		expect: quad.Quad{
			Subject:   quad.IRI("http://example/s"),
			Predicate: quad.IRI("http://example/p"),
			Object:    quad.LangString{Value: "Tomás de Torquemada", Lang: "es"},
			Label:     nil},
	},

	// Tests taken from http://www.w3.org/TR/n-quads/ and http://www.w3.org/TR/n-triples/.

	// _:100000 </film/performance/actor> </en/larry_fine_1902> . # example from 30movies
	{
		message: "parse triple with commment",
		input:   `_:100000 </film/performance/actor> </en/larry_fine_1902> . # example from 30movies`,
		expect: quad.Quad{
			Subject:   quad.BNode("100000"),
			Predicate: quad.IRI("/film/performance/actor"),
			Object:    quad.IRI("/en/larry_fine_1902"),
			Label:     nil,
		},
		err: nil,
	},
	// _:10011 </film/performance/character> "Tomás de Torquemada" . # example from 30movies with unicode
	{
		message: "parse triple with commment",
		input:   `_:10011 </film/performance/character> "Tomás de Torquemada" . # example from 30movies with unicode`,
		expect: quad.Quad{
			Subject:   quad.BNode("10011"),
			Predicate: quad.IRI("/film/performance/character"),
			Object:    quad.String("Tomás de Torquemada"),
			Label:     nil,
		},
		err: nil,
	},

	// N-Triples example 1.
	{
		message: "parse triple with commment",
		input:   `<http://one.example/subject1> <http://one.example/predicate1> <http://one.example/object1> . # comments here`,
		expect: quad.Quad{
			Subject:   quad.IRI("http://one.example/subject1"),
			Predicate: quad.IRI("http://one.example/predicate1"),
			Object:    quad.IRI("http://one.example/object1"),
			Label:     nil,
		},
		err: nil,
	},
	{
		message: "parse triple with blank subject node, literal object and no comment (1)",
		input:   `_:subject1 <http://an.example/predicate1> "object1" .`,
		expect: quad.Quad{
			Subject:   quad.BNode("subject1"),
			Predicate: quad.IRI("http://an.example/predicate1"),
			Object:    quad.String("object1"),
			Label:     nil,
		},
		err: nil,
	},
	{
		message: "parse triple with blank subject node, literal object and no comment (2)",
		input:   `_:subject2 <http://an.example/predicate2> "object2" .`,
		expect: quad.Quad{
			Subject:   quad.BNode("subject2"),
			Predicate: quad.IRI("http://an.example/predicate2"),
			Object:    quad.String("object2"),
			Label:     nil,
		},
		err: nil,
	},

	// N-Triples example 2.
	{
		message: "parse triple with three IRIREFs",
		input:   `<http://example.org/#spiderman> <http://www.perceive.net/schemas/relationship/enemyOf> <http://example.org/#green-goblin> .`,
		expect: quad.Quad{
			Subject:   quad.IRI("http://example.org/#spiderman"),
			Predicate: quad.IRI("http://www.perceive.net/schemas/relationship/enemyOf"),
			Object:    quad.IRI("http://example.org/#green-goblin"),
			Label:     nil,
		},
		err: nil,
	},

	// N-Triples example 3.
	{
		message: "parse triple with blank node labelled subject and object and IRIREF predicate (1)",
		input:   `_:alice <http://xmlns.com/foaf/0.1/knows> _:bob .`,
		expect: quad.Quad{
			Subject:   quad.BNode("alice"),
			Predicate: quad.IRI("http://xmlns.com/foaf/0.1/knows"),
			Object:    quad.BNode("bob"),
			Label:     nil,
		},
		err: nil,
	},
	{
		message: "parse triple with blank node labelled subject and object and IRIREF predicate (2)",
		input:   `_:bob <http://xmlns.com/foaf/0.1/knows> _:alice .`,
		expect: quad.Quad{
			Subject:   quad.BNode("bob"),
			Predicate: quad.IRI("http://xmlns.com/foaf/0.1/knows"),
			Object:    quad.BNode("alice"),
			Label:     nil,
		},
		err: nil,
	},

	// N-Quads example 1.
	{
		message: "parse quad with commment",
		input:   `<http://one.example/subject1> <http://one.example/predicate1> <http://one.example/object1> <http://example.org/graph3> . # comments here`,
		expect: quad.Quad{
			Subject:   quad.IRI("http://one.example/subject1"),
			Predicate: quad.IRI("http://one.example/predicate1"),
			Object:    quad.IRI("http://one.example/object1"),
			Label:     quad.IRI("http://example.org/graph3"),
		},
		err: nil,
	},
	{
		message: "parse quad with blank subject node, literal object, IRIREF predicate and label, and no comment (1)",
		input:   `_:subject1 <http://an.example/predicate1> "object1" <http://example.org/graph1> .`,
		expect: quad.Quad{
			Subject:   quad.BNode("subject1"),
			Predicate: quad.IRI("http://an.example/predicate1"),
			Object:    quad.String("object1"),
			Label:     quad.IRI("http://example.org/graph1"),
		},
		err: nil,
	},
	{
		message: "parse quad with blank subject node, literal object, IRIREF predicate and label, and no comment (2)",
		input:   `_:subject2 <http://an.example/predicate2> "object2" <http://example.org/graph5> .`,
		expect: quad.Quad{
			Subject:   quad.BNode("subject2"),
			Predicate: quad.IRI("http://an.example/predicate2"),
			Object:    quad.String("object2"),
			Label:     quad.IRI("http://example.org/graph5"),
		},
		err: nil,
	},

	// N-Quads example 2.
	{
		message: "parse quad with all IRIREF parts",
		input:   `<http://example.org/#spiderman> <http://www.perceive.net/schemas/relationship/enemyOf> <http://example.org/#green-goblin> <http://example.org/graphs/spiderman> .`,
		expect: quad.Quad{
			Subject:   quad.IRI("http://example.org/#spiderman"),
			Predicate: quad.IRI("http://www.perceive.net/schemas/relationship/enemyOf"),
			Object:    quad.IRI("http://example.org/#green-goblin"),
			Label:     quad.IRI("http://example.org/graphs/spiderman"),
		},
		err: nil,
	},

	// N-Quads example 3.
	{
		message: "parse quad with blank node labelled subject and object and IRIREF predicate and label (1)",
		input:   `_:alice <http://xmlns.com/foaf/0.1/knows> _:bob <http://example.org/graphs/john> .`,
		expect: quad.Quad{
			Subject:   quad.BNode("alice"),
			Predicate: quad.IRI("http://xmlns.com/foaf/0.1/knows"),
			Object:    quad.BNode("bob"),
			Label:     quad.IRI("http://example.org/graphs/john"),
		},
		err: nil,
	},
	{
		message: "parse quad with blank node labelled subject and object and IRIREF predicate and label (2)",
		input:   `_:bob <http://xmlns.com/foaf/0.1/knows> _:alice <http://example.org/graphs/james> .`,
		expect: quad.Quad{
			Subject:   quad.BNode("bob"),
			Predicate: quad.IRI("http://xmlns.com/foaf/0.1/knows"),
			Object:    quad.BNode("alice"),
			Label:     quad.IRI("http://example.org/graphs/james"),
		},
		err: nil,
	},

	// N-Triples tests.
	{
		message: "parse triple with all IRIREF parts",
		input:   `<http://example.org/bob#me> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> .`,
		expect: quad.Quad{
			Subject:   quad.IRI("http://example.org/bob#me"),
			Predicate: quad.IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
			Object:    quad.IRI("http://xmlns.com/foaf/0.1/Person"),
			Label:     nil,
		},
		err: nil,
	},
	{
		message: "parse triple with all IRIREF parts",
		input:   `<http://example.org/bob#me> <http://xmlns.com/foaf/0.1/knows> <http://example.org/alice#me> .`,
		expect: quad.Quad{
			Subject:   quad.IRI("http://example.org/bob#me"),
			Predicate: quad.IRI("http://xmlns.com/foaf/0.1/knows"),
			Object:    quad.IRI("http://example.org/alice#me"),
			Label:     nil,
		},
		err: nil,
	},
	{
		message: "parse triple with IRIREF schema on literal object",
		input:   `<http://example.org/bob#me> <http://schema.org/birthDate> "1990-07-04"^^<http://www.w3.org/2001/XMLSchema#date> .`,
		expect: quad.Quad{
			Subject:   quad.IRI("http://example.org/bob#me"),
			Predicate: quad.IRI("http://schema.org/birthDate"),
			Object:    quad.TypedString{Value: "1990-07-04", Type: "http://www.w3.org/2001/XMLSchema#date"},
			Label:     nil,
		},
		err: nil,
	},
	{
		message: "parse triple with IRIREF schema on literal object and dateTime",
		input:   `<http://example.org/bob#me> <http://schema.org/birthDate> "1990-07-04T17:25:41Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> .`,
		expect: quad.Quad{
			Subject:   quad.IRI("http://example.org/bob#me"),
			Predicate: quad.IRI("http://schema.org/birthDate"),
			Object: func() quad.Time {
				t, err := time.Parse(time.RFC3339, "1990-07-04T17:25:41Z")
				if err != nil {
					panic(err)
				}
				return quad.Time(t)
			}(),
			Label: nil,
		},
		err: nil,
	},
	{
		message: "parse commented IRIREF in triple",
		input:   `<http://example.org/bob#me> <http://xmlns.com/foaf/0.1/topic_interest> <http://www.wikidata.org/entity/Q12418> .`,
		expect: quad.Quad{
			Subject:   quad.IRI("http://example.org/bob#me"),
			Predicate: quad.IRI("http://xmlns.com/foaf/0.1/topic_interest"),
			Object:    quad.IRI("http://www.wikidata.org/entity/Q12418"),
			Label:     nil,
		},
		err: nil,
	},
	{
		message: "parse triple with literal subject",
		input:   `<http://www.wikidata.org/entity/Q12418> <http://purl.org/dc/terms/title> "Mona Lisa" .`,
		expect: quad.Quad{
			Subject:   quad.IRI("http://www.wikidata.org/entity/Q12418"),
			Predicate: quad.IRI("http://purl.org/dc/terms/title"),
			Object:    quad.String("Mona Lisa"),
			Label:     nil,
		},
		err: nil,
	},
	{
		message: "parse triple with all IRIREF parts (1)",
		input:   `<http://www.wikidata.org/entity/Q12418> <http://purl.org/dc/terms/creator> <http://dbpedia.org/resource/Leonardo_da_Vinci> .`,
		expect: quad.Quad{
			Subject:   quad.IRI("http://www.wikidata.org/entity/Q12418"),
			Predicate: quad.IRI("http://purl.org/dc/terms/creator"),
			Object:    quad.IRI("http://dbpedia.org/resource/Leonardo_da_Vinci"),
			Label:     nil,
		},
		err: nil,
	},
	{
		message: "parse triple with all IRIREF parts (2)",
		input:   `<http://data.europeana.eu/item/04802/243FA8618938F4117025F17A8B813C5F9AA4D619> <http://purl.org/dc/terms/subject> <http://www.wikidata.org/entity/Q12418> .`,
		expect: quad.Quad{
			Subject:   quad.IRI("http://data.europeana.eu/item/04802/243FA8618938F4117025F17A8B813C5F9AA4D619"),
			Predicate: quad.IRI("http://purl.org/dc/terms/subject"),
			Object:    quad.IRI("http://www.wikidata.org/entity/Q12418"),
			Label:     nil,
		},
		err: nil,
	},

	// N-Quads tests.
	{
		message: "parse commented IRIREF in quad (1)",
		input:   `<http://example.org/bob#me> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> <http://example.org/bob> .`,
		expect: quad.Quad{
			Subject:   quad.IRI("http://example.org/bob#me"),
			Predicate: quad.IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
			Object:    quad.IRI("http://xmlns.com/foaf/0.1/Person"),
			Label:     quad.IRI("http://example.org/bob"),
		},
		err: nil,
	},
	{
		message: "parse quad with all IRIREF parts",
		input:   `<http://example.org/bob#me> <http://xmlns.com/foaf/0.1/knows> <http://example.org/alice#me> <http://example.org/bob> .`,
		expect: quad.Quad{
			Subject:   quad.IRI("http://example.org/bob#me"),
			Predicate: quad.IRI("http://xmlns.com/foaf/0.1/knows"),
			Object:    quad.IRI("http://example.org/alice#me"),
			Label:     quad.IRI("http://example.org/bob"),
		},
		err: nil,
	},
	{
		message: "parse quad with IRIREF schema on literal object",
		input:   `<http://example.org/bob#me> <http://schema.org/birthDate> "1990-07-04"^^<http://www.w3.org/2001/XMLSchema#date> <http://example.org/bob> .`,
		expect: quad.Quad{
			Subject:   quad.IRI("http://example.org/bob#me"),
			Predicate: quad.IRI("http://schema.org/birthDate"),
			Object:    quad.TypedString{Value: "1990-07-04", Type: "http://www.w3.org/2001/XMLSchema#date"},
			Label:     quad.IRI("http://example.org/bob"),
		},
		err: nil,
	},
	{
		message: "parse commented IRIREF in quad (2)",
		input:   `<http://example.org/bob#me> <http://xmlns.com/foaf/0.1/topic_interest> <http://www.wikidata.org/entity/Q12418> <http://example.org/bob> .`,
		expect: quad.Quad{
			Subject:   quad.IRI("http://example.org/bob#me"),
			Predicate: quad.IRI("http://xmlns.com/foaf/0.1/topic_interest"),
			Object:    quad.IRI("http://www.wikidata.org/entity/Q12418"),
			Label:     quad.IRI("http://example.org/bob"),
		},
		err: nil,
	},
	{
		message: "parse literal object and colon qualified label in quad",
		input:   `<http://www.wikidata.org/entity/Q12418> <http://purl.org/dc/terms/title> "Mona Lisa" <https://www.wikidata.org/wiki/Special:EntityData/Q12418> .`,
		expect: quad.Quad{
			Subject:   quad.IRI("http://www.wikidata.org/entity/Q12418"),
			Predicate: quad.IRI("http://purl.org/dc/terms/title"),
			Object:    quad.String("Mona Lisa"),
			Label:     quad.IRI("https://www.wikidata.org/wiki/Special:EntityData/Q12418"),
		},
		err: nil,
	},
	{
		message: "parse all IRIREF parts with colon qualified label in quad (1)",
		input:   `<http://www.wikidata.org/entity/Q12418> <http://purl.org/dc/terms/creator> <http://dbpedia.org/resource/Leonardo_da_Vinci> <https://www.wikidata.org/wiki/Special:EntityData/Q12418> .`,
		expect: quad.Quad{
			Subject:   quad.IRI("http://www.wikidata.org/entity/Q12418"),
			Predicate: quad.IRI("http://purl.org/dc/terms/creator"),
			Object:    quad.IRI("http://dbpedia.org/resource/Leonardo_da_Vinci"),
			Label:     quad.IRI("https://www.wikidata.org/wiki/Special:EntityData/Q12418"),
		},
		err: nil,
	},
	{
		message: "parse all IRIREF parts with colon qualified label in quad (2)",
		input:   `<http://data.europeana.eu/item/04802/243FA8618938F4117025F17A8B813C5F9AA4D619> <http://purl.org/dc/terms/subject> <http://www.wikidata.org/entity/Q12418> <https://www.wikidata.org/wiki/Special:EntityData/Q12418> .`,
		expect: quad.Quad{
			Subject:   quad.IRI("http://data.europeana.eu/item/04802/243FA8618938F4117025F17A8B813C5F9AA4D619"),
			Predicate: quad.IRI("http://purl.org/dc/terms/subject"),
			Object:    quad.IRI("http://www.wikidata.org/entity/Q12418"),
			Label:     quad.IRI("https://www.wikidata.org/wiki/Special:EntityData/Q12418"),
		},
		err: nil,
	},
	{
		message: "parse all IRIREF parts (quad section - 1)",
		input:   `<http://example.org/bob> <http://purl.org/dc/terms/publisher> <http://example.org> .`,
		expect: quad.Quad{
			Subject:   quad.IRI("http://example.org/bob"),
			Predicate: quad.IRI("http://purl.org/dc/terms/publisher"),
			Object:    quad.IRI("http://example.org"),
			Label:     nil,
		},
		err: nil,
	},
	{
		message: "parse all IRIREF parts (quad section - 2)",
		input:   `<http://example.org/bob> <http://purl.org/dc/terms/rights> <http://creativecommons.org/licenses/by/3.0/> .`,
		expect: quad.Quad{
			Subject:   quad.IRI("http://example.org/bob"),
			Predicate: quad.IRI("http://purl.org/dc/terms/rights"),
			Object:    quad.IRI("http://creativecommons.org/licenses/by/3.0/"),
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
		input:   `# is a comment`,
		expect:  quad.Quad{},
		err:     fmt.Errorf("%v: unexpected rune '#' at 0", quad.ErrInvalid),
	},
	{
		message: "parse commented internal (1)",
		input:   `is # a comment`,
		expect:  quad.Quad{Subject: quad.Raw("is")},
		err:     fmt.Errorf("%v: unexpected rune '#' at 3", quad.ErrInvalid),
	},
	{
		message: "parse commented internal (2)",
		input:   `is a # comment`,
		expect:  quad.Quad{Subject: quad.Raw("is"), Predicate: quad.Raw("a")},
		err:     fmt.Errorf("%v: unexpected rune '#' at 5", quad.ErrInvalid),
	},
	{
		message: "parse incomplete quad (1)",
		input:   `<http://example.org/bob#me> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> .`,
		expect: quad.Quad{
			Subject:   quad.IRI("http://example.org/bob#me"),
			Predicate: quad.IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
			Object:    nil,
			Label:     nil,
		},
		err: quad.ErrIncomplete,
	},
	{
		message: "parse incomplete quad (2)",
		input:   `<http://example.org/bob#me> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> .`,
		expect: quad.Quad{
			Subject:   quad.IRI("http://example.org/bob#me"),
			Predicate: quad.IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
			Object:    nil,
			Label:     nil,
		},
		err: quad.ErrIncomplete,
	},

	// Example quad from issue #140 in two forms: strict N-Quads and as quoted in issue.
	{
		message: "parse incomplete quad",
		input:   "<ns:m.0y_chx>\t<ns:music.recording.lyrics_website..common.webpage.uri>\t<http://www.metrolyrics.com/?\"-lyrics-stephen-sondheim.html>.",
		expect: quad.Quad{
			Subject:   quad.IRI("ns:m.0y_chx"),
			Predicate: quad.IRI("ns:music.recording.lyrics_website..common.webpage.uri"),
			Object:    quad.Raw("<http://www.metrolyrics"),
			Label:     nil,
		},
		err: fmt.Errorf("%v: unexpected rune '\"' at 99", quad.ErrInvalid),
	},
	{
		message: "parse incomplete quad",
		input:   "ns:m.0y_chx\tns:music.recording.lyrics_website..common.webpage.uri\t<http://www.metrolyrics.com/?\"-lyrics-stephen-sondheim.html>.",
		expect: quad.Quad{
			Subject:   quad.Raw("ns:m.0y_chx"),
			Predicate: quad.Raw("ns:music.recording.lyrics_website..common.webpage.uri"),
			Object:    quad.Raw("<http://www.metrolyrics"),
			Label:     nil,
		},
		err: fmt.Errorf("%v: unexpected rune '\"' at 95", quad.ErrInvalid),
	},
}

func TestParse(t *testing.T) {
	for _, test := range testNQuads {
		got, err := Parse(test.input)
		_ = err
		if err != test.err && (err != nil && test.err == nil || err.Error() != test.err.Error()) {
			t.Errorf("Unexpected error when %s: got:%v expect:%v", test.message, err, test.err)
		}
		if !reflect.DeepEqual(got, test.expect) {
			t.Errorf("Failed to %s, %q,\ngot:%#v(%s)\nexpect:%#v(%s)", test.message, test.input, got, got, test.expect, test.expect)
		}
	}
}

// This is a sample taken from 30kmoviedata.nq.
// It has intentional defects:
// The second comment is inset one space and
// the second line after that comment is blank.
var document = `# first 10 lines of 30kmoviedata.nq
_:100000 </film/performance/actor> </en/larry_fine_1902> .
_:100001 </film/performance/actor> </en/samuel_howard> .
_:100002 </film/performance/actor> </en/joe_palma> .
_:100003 </film/performance/actor> </en/symona_boniface> .
_:100004 </film/performance/actor> </en/dudley_dickerson> .
_:100005 </film/performance/actor> </guid/9202a8c04000641f8000000006ec181a> .
_:100006 </film/performance/actor> </en/emil_sitka> .
_:100007 </film/performance/actor> </en/christine_mcintyre> .
_:100008 </film/performance/actor> </en/moe_howard> .
_:100009 </film/performance/actor> </en/larry_fine_1902> .
 #last ten lines of 30kmoviedata.nq
</guid/9202a8c04000641f800000001473e673> <name> "Bill Fishman" .

</guid/9202a8c04000641f800000001473e673> <type> </people/person> .
</guid/9202a8c04000641f800000001474a221> <name> "Matthew J. Evans" .
</guid/9202a8c04000641f800000001474a221> <type> </people/person> .
</guid/9202a8c04000641f800000001474f486> <name> "Nina Bonherry" .
</guid/9202a8c04000641f800000001474f486> <type> </people/person> .
</user/basketball_loader/basketballdatabase_namespace/ROBERBI01> <name> "Bill Roberts" .
</user/basketball_loader/basketballdatabase_namespace/ROBERBI01> <type> </people/person> .
</user/jamie/nytdataid/N17971793050606542713> <name> "Christopher Ashley" .
</user/jamie/nytdataid/N17971793050606542713> <type> </people/person> .
`

func TestDecoder(t *testing.T) {
	dec := NewReader(strings.NewReader(document), false)
	var n int
	for {
		q, err := dec.ReadQuad()
		if err != nil {
			if err != io.EOF {
				t.Fatalf("Failed to read document: %v", err)
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

func TestRDFWorkingGroupSuit(t *testing.T) {
	// Tests that are not passable by cquads parsing from the RDF
	// Working Group Suite:
	//
	// [1] Because we don't require literal quoting, we cannot
	// distinguish quad terms without separating whitespace.
	//
	// [2] The cquads grammar accepts these because of its relaxation.
	//
	// [3] These tests pass because the parser does not perform
	// semantic testing on the URI in the IRIRef as required by
	// the specification.
	skip := map[string]bool{
		// N-Triples.
		// [1]
		"minimal_whitespace.nt": true,

		// [2]
		"nt-syntax-bad-num-01.nt":    true,
		"nt-syntax-bad-num-02.nt":    true,
		"nt-syntax-bad-num-03.nt":    true,
		"nt-syntax-bad-prefix-01.nt": true,
		"nt-syntax-bad-string-02.nt": true,
		"nt-syntax-bad-string-03.nt": true,
		"nt-syntax-bad-string-04.nt": true,
		"nt-syntax-bad-struct-01.nt": true,
		"nt-syntax-bad-uri-01.nt":    true,
		"nt-syntax-bad-uri-04.nt":    true,

		// [3]
		"nt-syntax-bad-uri-06.nt": true,
		"nt-syntax-bad-uri-07.nt": true,
		"nt-syntax-bad-uri-08.nt": true,
		"nt-syntax-bad-uri-09.nt": true,

		// N-Quads.
		// [1]
		"minimal_whitespace.nq": true,

		// [2]
		"nq-syntax-bad-literal-01.nq": true,
		"nq-syntax-bad-literal-02.nq": true,
		"nq-syntax-bad-literal-03.nq": true,
		"nt-syntax-bad-num-01.nq":     true,
		"nt-syntax-bad-num-02.nq":     true,
		"nt-syntax-bad-num-03.nq":     true,
		"nt-syntax-bad-prefix-01.nq":  true,
		"nt-syntax-bad-string-02.nq":  true,
		"nt-syntax-bad-string-03.nq":  true,
		"nt-syntax-bad-string-04.nq":  true,
		"nt-syntax-bad-struct-01.nq":  true,
		"nt-syntax-bad-uri-01.nq":     true,
		"nt-syntax-bad-uri-04.nq":     true,

		// [3]
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

			dec := NewReader(tr, false)
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

var escapeSequenceTests = []struct {
	input  string
	expect string
}{
	{input: `\t`, expect: "\t"},
	{input: `\b`, expect: "\b"},
	{input: `\n`, expect: "\n"},
	{input: `\r`, expect: "\r"},
	{input: `\f`, expect: "\f"},
	{input: `\\`, expect: "\\"},
	{input: `\u00b7`, expect: "·"},
	{input: `\U000000b7`, expect: "·"},

	{input: `\t\u00b7`, expect: "\t·"},
	{input: `\b\U000000b7`, expect: "\b·"},
	{input: `\u00b7\n`, expect: "·\n"},
	{input: `\U000000b7\r`, expect: "·\r"},
	{input: `\u00b7\f\U000000b7`, expect: "·\f·"},
	{input: `\U000000b7\\\u00b7`, expect: "·\\·"},
}

func TestUnescape(t *testing.T) {
	for _, test := range escapeSequenceTests {
		got := unEscape([]rune(test.input), -1, false, true)
		if got == nil || got.String() != test.expect {
			t.Errorf("Failed to properly unescape %q, got:%q expect:%q", test.input, got, test.expect)
		}
	}
}

var result quad.Quad

func BenchmarkParser(b *testing.B) {
	for n := 0; n < b.N; n++ {
		result, _ = Parse("<http://example/s> <http://example/p> \"object of some real\\tlength\"@en . # comment")
	}
}
