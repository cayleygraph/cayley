package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
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

func iriToIdent(iri quad.IRI) *ast.Ident {
	return ast.NewIdent(string(iri)[26:])
}

var quadValueType = &ast.SelectorExpr{
	Sel: ast.NewIdent("Value"),
	X:   ast.NewIdent("quad"),
}

func propertyToValueType(class *owl.Class, property *owl.Property) (ast.Expr, error) {
	_range, err := property.Range()
	if err != nil {
		return nil, err
	}
	isSlice := true
	isPTR := false
	cardinality, err := class.CardinalityOf(property)
	if cardinality == int64(1) {
		isSlice = false
		isPTR = false
	}
	maxCardinality, err := class.MaxCardinalityOf(property)
	if maxCardinality == int64(1) {
		isSlice = false
		isPTR = true
	}
	var t ast.Expr
	if _range == quad.IRI("http://www.w3.org/2001/XMLSchema#string") {
		t = ast.NewIdent("string")
	} else if _range == quad.IRI("http://cayley.io/linkedql#PathStep") {
		t = pathTypeIdent
	} else if _range == quad.IRI("http://www.w3.org/2000/01/rdf-schema#Resource") {
		t = quadValueType
	} else {
		return nil, fmt.Errorf("Unexpected range %v", _range)
	}
	if isPTR {
		t = &ast.StarExpr{
			X: t,
		}
	}
	if isSlice {
		t = &ast.ArrayType{
			Elt: t,
		}
	}
	return t, nil
}

var pathTypeIdent = ast.NewIdent("Path")
var pathIdent = ast.NewIdent("p")

func main() {
	qs, err := loadSchema()

	ctx := context.TODO()
	stepClass, err := owl.GetClass(ctx, qs, quad.IRI("http://cayley.io/linkedql#PathStep"))

	if err != nil {
		panic(err)
	}

	stepSubClasses := stepClass.SubClasses()
	var decls []ast.Decl

	for _, stepSubClass := range stepSubClasses {
		iri, ok := stepSubClass.Identifier.(quad.IRI)
		if !ok {
			panic(fmt.Errorf("Unexpected class identifier %v of type %T", stepSubClass.Identifier, stepSubClass.Identifier))
		}
		properties := stepSubClass.Properties()
		propertyToType := make(map[quad.IRI]ast.Expr)

		var paramsList []*ast.Field
		for _, property := range properties {
			_type, err := propertyToValueType(stepSubClass, property)
			if err != nil {
				panic(err)
			}
			propertyToType[property.Identifier] = _type
			ident := iriToIdent(property.Identifier)
			if ident.Name == "from" {
				continue
			}
			paramsList = append(paramsList, &ast.Field{
				Names: []*ast.Ident{ident},
				Type:  _type,
			})
		}
		elts := []ast.Expr{
			&ast.KeyValueExpr{
				Key: &ast.BasicLit{
					Kind:  token.STRING,
					Value: "\"@type\"",
				},
				Value: &ast.BasicLit{
					Kind:  token.STRING,
					Value: "\"" + string(iri) + "\"",
				},
			},
			&ast.KeyValueExpr{
				Key: &ast.BasicLit{
					Kind:  token.STRING,
					Value: "\"from\"",
				},
				Value: pathIdent,
			},
		}

		for _, property := range properties {
			ident := iriToIdent(property.Identifier)
			if ident.Name == "from" {
				continue
			}
			var value ast.Expr
			value = iriToIdent(property.Identifier)
			t := propertyToType[property.Identifier]
			if t == pathTypeIdent {
				value = &ast.SelectorExpr{
					Sel: ast.NewIdent("steps"),
					X:   value,
				}
			}
			elts = append(elts, &ast.KeyValueExpr{
				Key: &ast.BasicLit{
					Kind:  token.STRING,
					Value: "\"" + string(property.Identifier) + "\"",
				},
				Value: value,
			})
		}

		decls = append(decls, &ast.FuncDecl{
			Name: iriToIdent(iri),
			Type: &ast.FuncType{
				Params: &ast.FieldList{List: paramsList},
				Results: &ast.FieldList{
					List: []*ast.Field{
						&ast.Field{
							Names: nil,
							Type:  pathTypeIdent,
						},
					},
				},
			},
			Recv: &ast.FieldList{
				List: []*ast.Field{
					&ast.Field{
						Names: []*ast.Ident{pathIdent},
						Type:  pathTypeIdent,
					},
				},
			},
			Body: &ast.BlockStmt{
				List: []ast.Stmt{
					&ast.ReturnStmt{
						Results: []ast.Expr{
							&ast.CompositeLit{
								Type: pathTypeIdent,
								Elts: elts,
							},
						},
					},
				},
			},
		})
	}

	if err != nil {
		panic(err)
	}

	// Create a FileSet for node. Since the node does not come
	// from a real source file, fset will be empty.
	fset := token.NewFileSet()
	src := `
package client

import (
	"github.com/cayleygraph/quad"
)

type Path map[string]interface{}
	`
	file, err := parser.ParseFile(fset, "", src, 0)

	if err != nil {
		panic(err)
	}

	file.Decls = append(file.Decls, decls...)

	var buf bytes.Buffer
	err = format.Node(&buf, fset, file)
	if err != nil {
		panic(err)
	}

	fmt.Println(buf.String())
}
