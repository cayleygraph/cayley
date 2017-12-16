package graphql

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cayleygraph/cayley/graph/graphtest"
	"github.com/cayleygraph/cayley/graph/memstore"
	"github.com/cayleygraph/cayley/quad"
	"github.com/cayleygraph/cayley/voc/rdf"
)

func iris(arr ...string) (out []quad.Value) {
	for _, s := range arr {
		out = append(out, quad.IRI(s))
	}
	return
}

var casesParse = []struct {
	query  string
	expect []field
}{
	{
		`{
	user(id: 3500401, http://iri: http://some_iri, follow: <bob>, n: _:bob) @rev(follow: "123"){
	id: ` + ValueKey + `,
	type: ` + rdf.NS + "type" + `,
	followed: follow @reverse @label(v: <fb>) {
		name: <name> @label(v: <google>)
		followed: ~follow
		sname @label
	}
	isViewerFriend,
		profilePicture(size: 50) {
			 uri,
			 width @opt,
			 height @rev
		}
	}
}`,
		[]field{{
			Via: "user", Alias: "user",
			Has: []has{
				{"follow", true, []quad.Value{quad.String("123")}, nil},
				{"id", false, []quad.Value{quad.Int(3500401)}, nil},
				{"http://iri", false, iris("http://some_iri"), nil},
				{"follow", false, iris("bob"), nil},
				{"n", false, []quad.Value{quad.BNode("bob")}, nil},
			},
			Fields: []field{
				{Via: quad.IRI(ValueKey), Alias: "id"},
				{Via: quad.IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), Alias: "type"},
				{
					Via: "follow", Alias: "followed", Rev: true, Labels: iris("fb"),
					Fields: []field{
						{Via: "name", Alias: "name", Labels: iris("google")},
						{Via: "follow", Alias: "followed", Rev: true, Labels: iris("fb")},
						{Via: "sname", Alias: "sname"},
					},
				},
				{Via: "isViewerFriend", Alias: "isViewerFriend"},
				{
					Via: "profilePicture", Alias: "profilePicture",
					Has: []has{{"size", false, []quad.Value{quad.Int(50)}, nil}},
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
			t.Fatalf("\n%#v\nvs\n%#v", q.fields, c.expect)
		}
	}
}

var casesExecute = []struct {
	name   string
	query  string
	result map[string]interface{}
}{
	{
		"cool people and friends",
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
		"skip and limit",
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
	{
		"labels",
		`{
  me {
    id: ` + ValueKey + `
    status @label(v: <smart_graph>)
  }
}`,
		map[string]interface{}{
			"me": []map[string]interface{}{
				{
					"id":     quad.IRI("emily"),
					"status": quad.String("smart_person"),
				},
				{
					"id":     quad.IRI("greg"),
					"status": quad.String("smart_person"),
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
	require.NoError(t, err)

	for _, c := range casesExecute {
		t.Run(c.name, func(t *testing.T) {
			q, err := Parse(strings.NewReader(c.query))
			require.NoError(t, err)
			out, err := q.Execute(context.Background(), qs)
			require.NoError(t, err)
			require.Equal(t, c.result, out, "results:\n%v\n\nvs\n\n%v", toJson(c.result), toJson(out))
		})
	}
}
