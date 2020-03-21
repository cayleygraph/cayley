package steps

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cayleygraph/cayley/graph/memstore"
	"github.com/cayleygraph/cayley/query/linkedql"
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/jsonld"
	"github.com/cayleygraph/quad/voc"
	"github.com/piprate/json-gold/ld"
	"github.com/stretchr/testify/require"
)

type TestCase struct {
	Data    interface{} `json:"data"`
	Query   interface{} `json:"query"`
	Results interface{} `json:"results"`
}

func readData(data interface{}) ([]quad.Quad, error) {
	d, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	b := bytes.NewBuffer(d)
	reader := jsonld.NewReader(b)
	quads, err := quad.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	return quads, nil
}

func normalizeResults(t *testing.T, results interface{}) interface{} {
	compact, err := ld.NewJsonLdProcessor().Compact(results, nil, ld.NewJsonLdOptions(""))
	require.NoError(t, err)
	serialized, err := json.Marshal(compact)
	require.NoError(t, err)
	var deserialized interface{}
	err = json.Unmarshal(serialized, &deserialized)
	require.NoError(t, err)
	return deserialized
}

func TestLinkedQL(t *testing.T) {
	// Using files
	direcotry := "test-cases"
	files, err := ioutil.ReadDir(direcotry)
	if err != nil {
		require.NoError(t, err)
	}
	for _, info := range files {
		fileName := info.Name()
		filePath := filepath.Join(direcotry, fileName)
		file, err := ioutil.ReadFile(filePath)
		require.NoError(t, err)

		var c TestCase
		err = json.Unmarshal(file, &c)
		require.NoError(t, err)

		data, err := readData(c.Data)
		require.NoError(t, err)
		require.NotEmpty(t, data)

		d, err := json.Marshal(c.Query)
		require.NoError(t, err)
		require.NotEmpty(t, d)
		q, err := linkedql.Unmarshal(d)
		require.NoError(t, err)
		query, ok := q.(linkedql.IteratorStep)
		require.True(t, ok)

		testName := strings.TrimSuffix(fileName, filepath.Ext(fileName))
		t.Run(testName, func(t *testing.T) {
			store := memstore.New(data...)
			voc := voc.Namespaces{}
			ctx := context.TODO()
			iterator, err := query.BuildIterator(store, &voc)
			require.NoError(t, err)
			var results []interface{}
			for iterator.Next(ctx) {
				results = append(results, iterator.Result())
			}
			require.NoError(t, iterator.Err())
			require.Equal(t, normalizeResults(t, c.Results), normalizeResults(t, results))
		})
	}
}
