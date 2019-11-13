package cayleyhttp

import (
	"net/http/httptest"
	"sort"
	"testing"

	"github.com/cayleygraph/cayley/client"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/graphtest"
	"github.com/cayleygraph/cayley/graph/memstore"
	"github.com/cayleygraph/cayley/writer"
	"github.com/cayleygraph/quad"
	"github.com/stretchr/testify/require"
)

func makeHandle(t testing.TB, quads ...quad.Quad) *graph.Handle {
	qs := memstore.New(quads...)
	wr, err := writer.NewSingleReplication(qs, nil)
	require.NoError(t, err)
	return &graph.Handle{qs, wr}
}

func makeServerV2(t testing.TB, quads ...quad.Quad) (string, func()) {
	h := makeHandle(t, quads...)

	api2 := NewAPIv2(h)
	srv := httptest.NewServer(api2)
	addr := srv.Listener.Addr()
	return "http://" + addr.String(), func() {
		srv.Close()
		h.Close()
	}
}

func TestV2Write(t *testing.T) {
	addr, closer := makeServerV2(t)
	defer closer()

	quads := graphtest.MakeQuadSet()
	cli := client.New(addr)
	qw, err := cli.QuadWriter()
	require.NoError(t, err)
	defer qw.Close()
	n, err := quad.Copy(qw, quad.NewReader(quads))
	require.NoError(t, err)
	require.Equal(t, int(len(quads)), n)
	err = qw.Close()
	require.NoError(t, err)
}

func TestV2Read(t *testing.T) {
	expect := graphtest.MakeQuadSet()
	addr, closer := makeServerV2(t, expect...)
	defer closer()

	cli := client.New(addr)
	qr, err := cli.QuadReader()
	require.NoError(t, err)
	defer qr.Close()
	quads, err := quad.ReadAll(qr)
	require.NoError(t, err)
	sort.Sort(quad.ByQuadString(quads))
	sort.Sort(quad.ByQuadString(expect))
	require.Equal(t, expect, quads)
}
