package graphql

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cayleygraph/cayley/graph/graphtest/testutil"
	"github.com/cayleygraph/cayley/graph/memstore"
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/voc/rdf"
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
	user(id: 3500401, http://iri: http://some_iri, follow: <bob>, n: _:bob, v: ["<bob>", "name", 3]) @rev(follow: "123"){
	id: ` + ValueKey + `,
	type: ` + rdf.NS + "type" + `,
	followed: follow @reverse @label(v: <fb>) {
		name: <name> @label(v: <google>)
		followed: ~follow
		sname @label
	}
	isViewerFriend,
		profilePicture(size: 50) @unnest {
			 uri,
			 width @opt,
			 height @rev
		}
	sub {*}
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
				{"v", false, []quad.Value{quad.IRI("bob"), quad.String("name"), quad.Int(3)}, nil},
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
					UnNest: true,
				},
				{Via: "sub", Alias: "sub", AllFields: true},
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

type M = map[string]interface{}

var casesExecute = []struct {
	name   string
	query  string
	result M
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
		M{
			"me": []M{
				{
					"id":      quad.IRI("bob"),
					"follows": nil,
					"followed": []M{
						{ValueKey: quad.IRI("alice")},
						{ValueKey: quad.IRI("charlie")},
						{ValueKey: quad.IRI("dani")},
					},
				},
				{
					"id": quad.IRI("dani"),
					"follows": []M{
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
					"followed": M{
						ValueKey: quad.IRI("charlie"),
					},
				},
				{
					"id":      quad.IRI("greg"),
					"follows": nil,
					"followed": []M{
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
		M{
			"me": M{
				"id": quad.IRI("dani"),
				"follows": M{
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
		M{
			"me": []M{
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
	{
		"expand all",
		`{
  me {
    id: ` + ValueKey + `
    status @label(v: <smart_graph>)
    follows {*}
  }
}`,
		M{
			"me": []M{
				{
					"id":     quad.IRI("emily"),
					"status": quad.String("smart_person"),
					"follows": M{
						"id":      quad.IRI("fred"),
						"follows": quad.IRI("greg"),
					},
				},
				{
					"id":      quad.IRI("greg"),
					"status":  quad.String("smart_person"),
					"follows": nil,
				},
			},
		},
	},
	{
		"unnest object",
		`{
  me(id: fred) {
    id: ` + ValueKey + `
    follows @unnest {
      friend: ` + ValueKey + `
      friend_status: status
      followed: follows(` + LimitKey + `: 1) @rev @unnest  {
        fof: ` + ValueKey + `
      }
    }
  }
}`,
		M{
			"me": M{
				"id":     quad.IRI("fred"),
				"fof":    quad.IRI("dani"),
				"friend": quad.IRI("greg"),
				"friend_status": []quad.Value{
					quad.String("cool_person"),
					quad.String("smart_person"),
				},
			},
		},
	},
	{
		"unnest object (non existent)",
		`{
  me(id: fred) {
    id: ` + ValueKey + `
    follows_missing @unnest {
      friend: ` + ValueKey + `
      friend_status: status
    }
  }
}`,
		M{
			"me": M{
				"id": quad.IRI("fred"),
			},
		},
	},
	{
		"all optional",
		`{
  nodes {
    id,
    status @opt
  }
}`,
		M{
			"nodes": []M{
				{"id": quad.IRI("alice")},
				{"id": quad.IRI("follows")},
				{"id": quad.IRI("bob"), "status": quad.String("cool_person")},
				{"id": quad.IRI("fred")},
				{"id": quad.IRI("status")},
				{"id": quad.String("cool_person")},
				{"id": quad.IRI("charlie")},
				{"id": quad.IRI("dani"), "status": quad.String("cool_person")},
				{"id": quad.IRI("greg"), "status": []quad.Value{
					quad.String("cool_person"),
					quad.String("smart_person"),
				}},
				{"id": quad.IRI("emily"), "status": quad.String("smart_person")},
				{"id": quad.IRI("predicates")},
				{"id": quad.IRI("are")},
				{"id": quad.String("smart_person")},
				{"id": quad.IRI("smart_graph")},
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
	qw := testutil.MakeWriter(t, qs, nil)
	quads := testutil.LoadGraph(t, "../../data/testdata.nq")
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
