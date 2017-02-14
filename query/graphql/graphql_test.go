package graphql

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"

	"github.com/cayleygraph/cayley/graph/graphtest"
	"github.com/cayleygraph/cayley/graph/memstore"
	"github.com/cayleygraph/cayley/quad"
)

var casesParse = []struct {
	query  string
	expect []field
}{
	{
		`{
	user(id: 3500401, http://iri: http://some_iri, follow: <bob>, n: _:bob) @rev(follow: "123"){
	id: ` + ValueKey + `,
	name,
	followed: follow @reverse {
		name: <name>
		followed: ~follow
	}
	isViewerFriend,
		profilePicture(size: 50)  {
			 uri,
			 width @opt,
			 height @rev
		}
	}
}`,
		[]field{{
			Via: "user", Alias: "user",
			Has: []has{
				{"id", false, []quad.Value{quad.Int(3500401)}},
				{"http://iri", false, []quad.Value{quad.IRI("http://some_iri")}},
				{"follow", false, []quad.Value{quad.IRI("bob")}},
				{"n", false, []quad.Value{quad.BNode("bob")}},
				{"follow", true, []quad.Value{quad.String("123")}},
			},
			Fields: []field{
				{Via: quad.IRI(ValueKey), Alias: "id"},
				{Via: "name", Alias: "name"},
				{
					Via: "follow", Alias: "followed", Rev: true,
					Fields: []field{
						{Via: "name", Alias: "name"},
						{Via: "follow", Alias: "followed", Rev: true},
					},
				},
				{Via: "isViewerFriend", Alias: "isViewerFriend"},
				{
					Via: "profilePicture", Alias: "profilePicture",
					Has: []has{{"size", false, []quad.Value{quad.Int(50)}}},
					Fields: []field{
						{Via: "uri", Alias: "uri"},
						{Via: "width", Alias: "width", Opt: true},
						{Via: "height", Alias: "height", Rev: true},
					},
				},
			},
		}},
	},
}

func TestParse(t *testing.T) {
	for _, c := range casesParse {
		q, err := Parse(strings.NewReader(c.query))
		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(q.fields, c.expect) {
			t.Fatalf("\n%v\nvs\n%v", q.fields, c.expect)
		}
	}
}

var casesExecute = []struct {
	query  string
	result map[string]interface{}
}{
	{
		`{
  me(status: "cool_person") {
    id: ` + ValueKey + `
    follows {
      ` + ValueKey + `
      status
    }
    followed: follows @rev {
      ` + ValueKey + `
    }
  }
}`,
		map[string]interface{}{
			"me": []map[string]interface{}{
				{
					"id":      quad.IRI("bob"),
					"follows": nil,
					"followed": []map[string]interface{}{
						{ValueKey: quad.IRI("alice")},
						{ValueKey: quad.IRI("charlie")},
						{ValueKey: quad.IRI("dani")},
					},
				},
				{
					"id": quad.IRI("dani"),
					"follows": []map[string]interface{}{
						{
							ValueKey: quad.IRI("bob"),
							"status": quad.String("cool_person"),
						},
						{
							ValueKey: quad.IRI("greg"),
							"status": []quad.Value{
								quad.String("cool_person"),
								quad.String("smart_person"),
							},
						},
					},
					"followed": map[string]interface{}{
						ValueKey: quad.IRI("charlie"),
					},
				},
				{
					"id":      quad.IRI("greg"),
					"follows": nil,
					"followed": []map[string]interface{}{
						{ValueKey: quad.IRI("dani")},
						{ValueKey: quad.IRI("fred")},
					},
				},
			},
		},
	},
	{
		`{
  me(status: "cool_person", ` + LimitKey + `: 1, ` + SkipKey + `: 1) {
    id: ` + ValueKey + `
    follows(` + LimitKey + `: 1) @opt {
      ` + ValueKey + `
    }
  }
}`,
		map[string]interface{}{
			"me": map[string]interface{}{
				"id": quad.IRI("dani"),
				"follows": map[string]interface{}{
					ValueKey: quad.IRI("bob"),
				},
			},
		},
	},
}

func toJson(o interface{}) string {
	buf := bytes.NewBuffer(nil)
	json.NewEncoder(buf).Encode(o)
	buf2 := bytes.NewBuffer(nil)
	json.Indent(buf2, buf.Bytes(), "", "   ")
	return buf2.String()
}

func TestExecute(t *testing.T) {
	qs := memstore.New()
	qw := graphtest.MakeWriter(t, qs, nil)
	quads := graphtest.LoadGraph(t, "../../data/testdata.nq")
	err := qw.AddQuadSet(quads)
	require.Nil(t, err)

	for i, c := range casesExecute {
		q, err := Parse(strings.NewReader(c.query))
		if err != nil {
			t.Errorf("case %d failed: %v", i+1, err)
			continue
		}
		out, err := q.Execute(context.Background(), qs)
		if err != nil {
			t.Errorf("case %d failed: %v", i+1, err)
			continue
		}
		assert.Equal(t, c.result, out, "results:\n%v\n\nvs\n\n%v", toJson(c.result), toJson(out))
	}
}
