package main

import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"testing"

	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/jsonld"
	"github.com/stretchr/testify/require"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/memstore"
	chttp "github.com/cayleygraph/cayley/internal/http"
)

var testData = []quad.Quad{
	{
		Subject:   quad.IRI("http://example.com/alice"),
		Predicate: quad.IRI("http://example.com/likes"),
		Object:    quad.IRI("http://example.com/bob"),
		Label:     nil,
	},
}

func serializeTestData() string {
	buf := bytes.NewBuffer(nil)
	w := jsonld.NewWriter(buf)
	w.WriteQuads(testData)
	w.Close()
	return buf.String()
}

func TestCayleyExport(t *testing.T) {
	qs := memstore.New(testData...)
	qw, err := graph.NewQuadWriter("single", qs, graph.Options{})
	require.NoError(t, err)
	h := &graph.Handle{QuadStore: qs, QuadWriter: qw}
	chttp.SetupRoutes(h, &chttp.Config{})

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() {
		lis.Close()
	})

	srv := &http.Server{
		Addr: lis.Addr().String(),
	}
	go srv.Serve(lis)

	cmd := NewCmd()
	b := bytes.NewBufferString("")
	cmd.SetOut(b)
	cmd.SetArgs([]string{
		"--uri",
		fmt.Sprintf("http://%s", lis.Addr().String()),
	})
	err = cmd.Execute()
	require.NoError(t, err)
	data := serializeTestData()
	require.NotEmpty(t, data)
	require.Equal(t, data, b.String())
}
