package cayleyhttp

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/memstore"
	"github.com/cayleygraph/cayley/writer"
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/jsonld"
	"github.com/stretchr/testify/require"
)

func makeHandle(t testing.TB, quads ...quad.Quad) *graph.Handle {
	qs := memstore.New(quads...)
	wr, err := writer.NewSingleReplication(qs, nil)
	require.NoError(t, err)
	return &graph.Handle{qs, wr}
}

func makeServerV2(t testing.TB, quads ...quad.Quad) *APIv2 {
	h := makeHandle(t, quads...)
	return NewAPIv2(h)
}

func writeQuads(q []quad.Quad, w io.Writer) error {
	writer := jsonld.NewWriter(w)
	reader := quad.NewReader(quads)
	_, err := quad.Copy(writer, reader)
	writer.Close()
	return err
}

var mime = quad.FormatByName("jsonld").Mime[0]

var quads = []quad.Quad{
	quad.MakeIRI("http://example.com/bob", "http://example.com/likes", "http://example.com/alice", ""),
	quad.MakeIRI("http://example.com/alice", "http://example.com/likes", "http://example.com/bob", ""),
}

func TestV2Write(t *testing.T) {
	api := makeServerV2(t)
	buf := bytes.NewBuffer(nil)

	err := writeQuads(quads, buf)
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodGet, prefix+"/write", buf)
	require.NoError(t, err)
	req.Header.Set(hdrContentType, mime)

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(api.ServeWrite)
	handler.ServeHTTP(rr, req)

	require.Equal(t, rr.Code, http.StatusOK, rr.Body.String())

	expectedResponse := newWriteResponse(len(quads))

	var response writeResponse
	json.Unmarshal(rr.Body.Bytes(), &response)

	require.Equal(t, expectedResponse, response)
}

func TestV2Read(t *testing.T) {
	api := makeServerV2(t, quads...)
	buf := bytes.NewBuffer(nil)

	req, err := http.NewRequest(http.MethodGet, prefix+"/read", buf)
	require.NoError(t, err)
	req.Header.Set(hdrAccept, mime)

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(api.ServeRead)
	handler.ServeHTTP(rr, req)

	require.Equal(t, rr.Code, http.StatusOK, rr.Body.String())

	reader := jsonld.NewReader(rr.Body)
	receivedQuads, err := quad.ReadAll(reader)
	require.NoError(t, err)
	sort.Sort(quad.ByQuadString(receivedQuads))
	sort.Sort(quad.ByQuadString(quads))
	require.Equal(t, quads, receivedQuads)

}
