package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"go/token"
	"io"
	"os"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/memstore"
	"github.com/cayleygraph/cayley/owl"
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/jsonld"
)

const schemaFile = "linkedql.json"

func loadSchema() (graph.QuadStore, error) {
	jsonFile, err := os.Open(schemaFile)
	if err != nil {
		return nil, err
	}
	var o interface{}
	qs := memstore.New()
	json.NewDecoder(jsonFile).Decode(&o)
	reader := jsonld.NewReaderFromMap(o)
	for true {
		quad, err := reader.ReadQuad()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		qs.AddQuad(quad)
	}
	return qs, nil
}

func main() {
	qs, err := loadSchema()

	ctx := context.TODO()
	stepClass, err := owl.GetClass(ctx, qs, quad.IRI("http://cayley.io/linkedql#PathStep"))

	if err != nil {
		panic(err)
	}

	stepSubClasses := stepClass.SubClasses()

	for _, stepSubClass := range stepSubClasses {
		properties := stepSubClass.Properties()
		if properties == nil {
			fmt.Printf("%v has no properties\n", stepSubClass)
		}
	}

	if err != nil {
		panic(err)
	}

	file := &ast.File{
		Name: ast.NewIdent("example"),
		Decls: []ast.Decl{
			&ast.FuncDecl{
				Name: ast.NewIdent("foo"),
				Type: &ast.FuncType{
					Params:  &ast.FieldList{},
					Results: &ast.FieldList{},
				},
			},
		},
	}
	// Create a FileSet for node. Since the node does not come
	// from a real source file, fset will be empty.
	fset := token.NewFileSet()

	var buf bytes.Buffer
	err = format.Node(&buf, fset, file)
	if err != nil {
		panic(err)
	}

	fmt.Println(buf.String())
}
