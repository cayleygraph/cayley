package steps

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cayleygraph/cayley/graph/memstore"
	"github.com/cayleygraph/cayley/query/linkedql"
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/jsonld"
	"github.com/cayleygraph/quad/voc"
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

func readQuery(raw interface{}) (linkedql.Step, error) {
	d, err := json.Marshal(raw)
	if err != nil {
		return nil, err
	}
	q, err := linkedql.Unmarshal(d)
	if err != nil {
		return nil, err
	}
	query, ok := q.(linkedql.Step)
	if !ok {
		return nil, fmt.Errorf("Expected linkedql.Step")
	}
	return query, nil
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
		require.NoError(t, err, fileName)
		require.NotEmpty(t, data, fileName)

		query, err := readQuery(c.Query)
		require.NoError(t, err, fileName)
		require.NotNil(t, query, fileName)

		testName := strings.TrimSuffix(fileName, filepath.Ext(fileName))
		t.Run(testName, func(t *testing.T) {
			store := memstore.New(data...)
			voc := voc.Namespaces{}
			ctx := context.TODO()
			iterator, err := linkedql.BuildIterator(query, store, &voc)
			require.NoError(t, err)
			var results []interface{}
			for iterator.Next(ctx) {
				results = append(results, iterator.Result())
			}
			require.NoError(t, iterator.Err())
			require.Equal(t, nil, isomorphic(c.Results, results))
		})
	}
}
