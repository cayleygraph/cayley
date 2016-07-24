package internal

import (
	"fmt"
	"io"
	client "net/http"
	"net/url"
	"os"
	"path/filepath"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/internal/config"
	"github.com/cayleygraph/cayley/internal/db"
	"github.com/cayleygraph/cayley/quad"
	"github.com/cayleygraph/cayley/quad/nquads"
)

// Load loads a graph from the given path and write it to qw.  See
// DecompressAndLoad for more information.
func Load(qw graph.QuadWriter, cfg *config.Config, path, typ string) error {
	return DecompressAndLoad(qw, cfg, path, typ, db.Load)
}

// DecompressAndLoad will load or fetch a graph from the given path, decompress
// it, and then call the given load function to process the decompressed graph.
// If no loadFn is provided, db.Load is called.
func DecompressAndLoad(qw graph.QuadWriter, cfg *config.Config, path, typ string, loadFn func(graph.QuadWriter, *config.Config, quad.Reader) error) error {
	var r io.Reader

	if path == "" {
		path = cfg.DatabasePath
	}
	if path == "" {
		return nil
	}
	u, err := url.Parse(path)
	if err != nil || u.Scheme == "file" || u.Scheme == "" {
		// Don't alter relative URL path or non-URL path parameter.
		if u.Scheme != "" && err == nil {
			// Recovery heuristic for mistyping "file://path/to/file".
			path = filepath.Join(u.Host, u.Path)
		}
		f, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("could not open file %q: %v", path, err)
		}
		defer f.Close()
		r = f
	} else {
		res, err := client.Get(path)
		if err != nil {
			return fmt.Errorf("could not get resource <%s>: %v", u, err)
		}
		defer res.Body.Close()
		r = res.Body
	}

	r, err = Decompressor(r)
	if err != nil {
		if err == io.EOF {
			return nil
		}
		return err
	}

	var qr quad.Reader
	switch typ {
	case "cquad":
		qr = nquads.NewReader(r)
	case "nquad":
		qr = nquads.NewRawReader(r)
	default:
		return fmt.Errorf("unknown quad format %q", typ)
	}

	if loadFn != nil {
		return loadFn(qw, cfg, qr)
	}

	return db.Load(qw, cfg, qr)
}
