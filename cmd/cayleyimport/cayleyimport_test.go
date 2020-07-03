package main

import (
	"bytes"
	"fmt"
	"net/http"
	"path"
	"testing"
	"time"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/memstore"
	chttp "github.com/cayleygraph/cayley/internal/http"
	"github.com/phayes/freeport"
	"github.com/stretchr/testify/require"
)

func serve(addr string) {
	qs := memstore.New()
	qw, err := graph.NewQuadWriter("single", qs, graph.Options{})
	if err != nil {
		panic(err)
	}
	h := &graph.Handle{QuadStore: qs, QuadWriter: qw}
	chttp.SetupRoutes(h, &chttp.Config{})
	err = http.ListenAndServe(addr, nil)
	if err != nil {
		panic(err)
	}
}

func TestCayleyImport(t *testing.T) {
	port, err := freeport.GetFreePort()
	require.NoError(t, err)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	uri := fmt.Sprintf("http://%s", addr)
	go serve(addr)
	time.Sleep(3)
	cmd := NewCmd()
	b := bytes.NewBufferString("")
	cmd.SetOut(b)
	fileName := path.Join("..", "..", "data", "people.jsonld")
	cmd.SetArgs([]string{
		fileName,
		"--uri",
		uri,
	})
	err = cmd.Execute()
	require.NoError(t, err)
	require.Empty(t, b.String())
}
