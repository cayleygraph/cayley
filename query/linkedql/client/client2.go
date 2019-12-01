package main

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/token"
)

func main() {
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
	err := format.Node(&buf, fset, file)
	if err != nil {
		panic(err)
	}

	fmt.Println(buf.String())
}
